use crate::error::Error;
use crate::repo::paths::{block_path, filestem_to_block_cid};
use crate::repo::{BlockPut, BlockStore};
use crate::Block;
use futures::stream::{self, BoxStream};
use futures::{StreamExt, TryFutureExt, TryStreamExt};
use ipld_core::cid::Cid;
use std::io::{self, ErrorKind, Read};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs;
use tokio::sync::RwLock;
use tokio_stream::wrappers::ReadDirStream;

/// File system backed block store.
///
/// For information on path mangling, please see `block_path` and `filestem_to_block_cid`.
#[derive(Debug, Clone)]
pub struct FsBlockStore {
    inner: Arc<RwLock<FsBlockStoreInner>>,
}

#[derive(Debug)]
struct FsBlockStoreInner {
    path: PathBuf,
}

impl FsBlockStore {
    pub fn new(path: PathBuf) -> Self {
        let inner = Arc::new(RwLock::new(FsBlockStoreInner { path }));

        FsBlockStore { inner }
    }
}

impl BlockStore for FsBlockStore {
    async fn init(&self) -> Result<(), Error> {
        let inner = &*self.inner.read().await;
        fs::create_dir_all(inner.path.clone()).await?;
        Ok(())
    }

    async fn contains(&self, cid: &Cid) -> Result<bool, Error> {
        let inner = &*self.inner.read().await;
        inner.contains(cid).await
    }

    async fn get(&self, cid: &Cid) -> Result<Option<Block>, Error> {
        let inner = &*self.inner.read().await;
        inner.get(cid).await
    }

    async fn size(&self, cid: &[Cid]) -> Result<Option<usize>, Error> {
        let inner = &*self.inner.read().await;
        Ok(inner.size(cid).await)
    }

    async fn total_size(&self) -> Result<usize, Error> {
        let inner = &*self.inner.read().await;
        Ok(inner.total_size().await)
    }

    //TODO: Allow multiple puts without holding a lock. We could probably hold a read lock instead
    //      and revert back to using a broadcast
    async fn put(&self, block: &Block) -> Result<(Cid, BlockPut), Error> {
        let inner = &mut *self.inner.write().await;
        inner.put(block).await
    }

    async fn remove(&self, cid: &Cid) -> Result<(), Error> {
        let inner = &mut *self.inner.write().await;
        inner.remove(cid).await
    }

    async fn remove_many(&self, blocks: BoxStream<'static, Cid>) -> BoxStream<'static, Cid> {
        let inner = self.inner.clone();
        let stream = async_stream::stream! {
            let inner = &mut *inner.write().await;
            let path = inner.path.clone();
            for await cid in blocks
                .map(move |cid| (cid, block_path(path.clone(), &cid)))
                .filter_map(|(cid, path)| async move { fs::remove_file(path).await.ok().map(|_| cid) }) {
                    yield cid;
                }
        };

        stream.boxed()
    }

    async fn list(&self) -> BoxStream<'static, Cid> {
        let inner = &*self.inner.read().await;
        inner.list().await
    }
}

impl FsBlockStoreInner {
    async fn contains(&self, cid: &Cid) -> Result<bool, Error> {
        let path = block_path(self.path.clone(), cid);

        let metadata = match fs::metadata(path).await {
            Ok(m) => m,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(false),
            Err(e) => return Err(e.into()),
        };

        Ok(metadata.is_file())
    }

    async fn get(&self, cid: &Cid) -> Result<Option<Block>, Error> {
        let path = block_path(self.path.clone(), cid);

        let cid = *cid;

        // probably best to do everything in the blocking thread if we are to issue multiple
        // syscalls
        tokio::task::spawn_blocking(move || {
            let mut file = match std::fs::File::open(path) {
                Ok(file) => file,
                Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
                Err(e) => {
                    return Err(e.into());
                }
            };

            let len = file.metadata()?.len();

            let mut data = Vec::with_capacity(len as usize);
            file.read_to_end(&mut data)?;
            let block = Block::new(cid, data)?;
            Ok(Some(block))
        })
        .await?
    }

    async fn put(&mut self, block: &Block) -> Result<(Cid, BlockPut), Error> {
        let block = block.clone();
        let target_path = block_path(self.path.clone(), block.cid());
        let cid = *block.cid();

        let put = tokio::task::spawn_blocking(move || {
            let sharded = target_path
                .parent()
                .expect("we already have at least the shard parent");

            std::fs::create_dir_all(sharded)?;

            write_block(&target_path, block.data())
        })
        .await
        .map_err(|e| {
            error!("blocking put task error: {}", e);
            e
        })??;

        trace!(?put, %cid, "block writing finished");
        Ok((cid, put))
    }

    async fn size(&self, cids: &[Cid]) -> Option<usize> {
        let mut block_sizes = 0;

        for cid in cids {
            let path = block_path(self.path.clone(), cid);
            if let Ok(size) = fs::metadata(path).await.map(|m| m.len() as usize) {
                block_sizes += size;
            }
        }

        Some(block_sizes)
    }

    async fn total_size(&self) -> usize {
        self.list_stream()
            .and_then(|blocks| async move {
                let list = blocks
                    .try_filter_map(|(_, path)| async move {
                        let meta = fs::metadata(path).await?;
                        Ok(Some(meta.len()))
                    })
                    .try_collect::<Vec<_>>()
                    .await
                    .unwrap_or_default();
                Ok(list.into_iter().sum::<u64>() as usize)
            })
            .await
            .unwrap_or_default()
    }

    async fn remove(&mut self, cid: &Cid) -> Result<(), Error> {
        let path = block_path(self.path.clone(), cid);
        trace!(cid = %cid, "removing block after synchronizing");
        fs::remove_file(path).await.map_err(anyhow::Error::from)
    }

    async fn list_stream(
        &self,
    ) -> Result<BoxStream<'static, Result<(Cid, PathBuf), io::Error>>, Error> {
        let stream = ReadDirStream::new(fs::read_dir(&self.path).await?);
        let path = self.path.clone();
        Ok(stream
            .try_filter_map(|d| async move {
                // map over the shard directories
                Ok(if d.file_type().await?.is_dir() {
                    Some(ReadDirStream::new(fs::read_dir(d.path()).await?))
                } else {
                    None
                })
            })
            // flatten each; there could be unordered execution pre-flattening
            .try_flatten()
            // convert the paths ending in ".data" into cid
            .try_filter_map(|d| {
                let name = d.file_name();
                let path: &std::path::Path = name.as_ref();

                futures::future::ready(if path.extension() != Some("data".as_ref()) {
                    Ok(None)
                } else {
                    let maybe_cid = filestem_to_block_cid(path.file_stem());
                    Ok(maybe_cid)
                })
            })
            .try_filter_map(move |cid| {
                let path = path.clone();
                async move {
                    let path = block_path(path, &cid);
                    Ok(Some((cid, path)))
                }
            })
            .boxed())
    }

    async fn list(&self) -> BoxStream<'static, Cid> {
        let stream = self.list_stream().await.unwrap_or(stream::empty().boxed());
        stream
            .try_filter_map(|(cid, _)| futures::future::ready(Ok(Some(cid))))
            .filter_map(|cid| futures::future::ready(cid.ok()))
            .boxed()
    }
}

/// Atomically publishes a block at `target_path`.
///
/// Note that the full block is written to a uniquely named sibling temp file and fsynced,
/// then hard-linked into place. `hard_link` fails with `AlreadyExists` if the block is already
/// present (so we can report, `Existed` and keep the put-once guarantee), and because the link only
/// ever points at a fully written file, a crash can never leave a present-but-empty `.data` behind.
/// The existence latch is the link itself, never a pre-created final file.
fn write_block(target_path: &std::path::Path, data: &[u8]) -> Result<BlockPut, std::io::Error> {
    use std::io::Write;
    use std::sync::atomic::{AtomicU64, Ordering};

    // A per-process unique suffix so concurrent puts of the same cid never share a temp file.
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let mut temp_name = target_path.as_os_str().to_owned();
    temp_name.push(format!(
        ".{}.{}.tmp",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    let temp_path = PathBuf::from(temp_name);

    let write_temp = || -> Result<(), std::io::Error> {
        let mut temp = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&temp_path)?;
        temp.write_all(data)?;
        temp.flush()?;
        // safe default
        temp.sync_all()
    };

    if let Err(e) = write_temp() {
        let _ = std::fs::remove_file(&temp_path);
        return Err(e);
    }

    let put = match std::fs::hard_link(&temp_path, target_path) {
        Ok(()) => BlockPut::NewBlock,
        Err(e) if e.kind() == ErrorKind::AlreadyExists => BlockPut::Existed,
        Err(e) => {
            let _ = std::fs::remove_file(&temp_path);
            return Err(e);
        }
    };

    // The data now lives at target_path (or already did); drop the temp link either way.
    let _ = std::fs::remove_file(&temp_path);

    // FIXME: a directory fsync here would make a freshly linked block durable across power loss
    //        (currently a crash can lose the dir entry, leaving the block absent but re-fetchable).
    Ok(put)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::BlockCodec;
    use crate::Block;
    use hex_literal::hex;
    use ipld_core::cid::Cid;
    use multihash_codetable::{Code, MultihashDigest};
    use std::convert::TryFrom;
    use std::env::temp_dir;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_fs_blockstore() {
        let mut tmp = temp_dir();
        tmp.push("blockstore1");
        std::fs::remove_dir_all(tmp.clone()).ok();
        let store = FsBlockStore::new(tmp.clone());

        let data = b"1".to_vec();
        let cid = Cid::new_v1(BlockCodec::Raw.into(), Code::Sha2_256.digest(&data));
        let block = Block::new(cid, data).unwrap();

        store.init().await.unwrap();

        let contains = store.contains(&cid).await.unwrap();
        assert!(!contains);
        let get = store.get(&cid).await.unwrap();
        assert_eq!(get, None);
        if store.remove(&cid).await.is_ok() {
            panic!("block should not be found")
        }

        let put = store.put(&block).await.unwrap();
        assert_eq!(put.0, cid.to_owned());
        let contains = store.contains(&cid);
        assert!(contains.await.unwrap());
        let get = store.get(&cid);
        assert_eq!(get.await.unwrap(), Some(block.clone()));

        store.remove(&cid).await.unwrap();
        let contains = store.contains(&cid);
        assert!(!contains.await.unwrap());
        let get = store.get(&cid);
        assert_eq!(get.await.unwrap(), None);

        std::fs::remove_dir_all(tmp).ok();
    }

    #[tokio::test]
    async fn test_fs_blockstore_open() {
        let mut tmp = temp_dir();
        tmp.push("blockstore2");
        std::fs::remove_dir_all(&tmp).ok();

        let data = b"1".to_vec();
        let cid = Cid::new_v1(BlockCodec::Raw.into(), Code::Sha2_256.digest(&data));
        let block = Block::new(cid, data).unwrap();

        let block_store = FsBlockStore::new(tmp.clone());
        block_store.init().await.unwrap();

        assert!(!block_store.contains(block.cid()).await.unwrap());
        block_store.put(&block).await.unwrap();

        let block_store = FsBlockStore::new(tmp.clone());
        assert!(block_store.contains(block.cid()).await.unwrap());
        assert_eq!(block_store.get(block.cid()).await.unwrap().unwrap(), block);

        std::fs::remove_dir_all(&tmp).ok();
    }

    #[tokio::test]
    async fn test_fs_blockstore_list() {
        let mut tmp = temp_dir();
        tmp.push("blockstore_list");
        std::fs::remove_dir_all(&tmp).ok();

        let block_store = FsBlockStore::new(tmp.clone());
        block_store.init().await.unwrap();

        for data in &[b"1", b"2", b"3"] {
            let data_slice = data.to_vec();
            let cid = Cid::new_v1(BlockCodec::Raw.into(), Code::Sha2_256.digest(&data_slice));
            let block = Block::new(cid, data_slice).unwrap();
            block_store.put(&block).await.unwrap();
        }

        let cids = block_store.list().await.collect::<Vec<_>>().await;
        assert_eq!(cids.len(), 3);
        for cid in cids.iter() {
            assert!(block_store.contains(cid).await.unwrap());
        }
    }

    #[tokio::test]
    async fn race_to_insert_new() {
        // FIXME: why not tempdir?
        let mut tmp = temp_dir();
        tmp.push("race_to_insert_new");
        std::fs::remove_dir_all(&tmp).ok();

        let single = FsBlockStore::new(tmp.clone());
        single.init().await.unwrap();

        let single = Arc::new(single);

        let cid = Cid::try_from("QmRgutAxd8t7oGkSm4wmeuByG6M51wcTso6cubDdQtuEfL").unwrap();
        let data = hex!("0a0d08021207666f6f6261720a1807");

        let block = Block::new(cid, data.to_vec()).unwrap();

        let count = 10;

        let (writes, existing) = race_to_insert_scenario(count, block, &single).await;

        assert_eq!(writes, 1);
        assert_eq!(existing, count - 1);
    }

    async fn race_to_insert_scenario(
        count: usize,
        block: Block,
        blockstore: &Arc<FsBlockStore>,
    ) -> (usize, usize) {
        let barrier = Arc::new(tokio::sync::Barrier::new(count));

        let join_handles = (0..count)
            .map(|_| {
                tokio::spawn({
                    let bs = Arc::clone(blockstore);
                    let barrier = Arc::clone(&barrier);
                    let block = block.clone();
                    async move {
                        barrier.wait().await;
                        bs.put(&block).await
                    }
                })
            })
            .collect::<Vec<_>>();

        let mut writes = 0usize;
        let mut existing = 0usize;

        for jh in join_handles {
            let res = jh.await;

            match res {
                Ok(Ok((_, BlockPut::NewBlock))) => writes += 1,
                Ok(Ok((_, BlockPut::Existed))) => existing += 1,
                Ok(Err(e)) => tracing::error!("joinhandle err: {e}"),
                _ => unreachable!("join error"),
            }
        }

        (writes, existing)
    }

    #[tokio::test]
    async fn put_leaves_no_temp_and_full_block() {
        let mut tmp = temp_dir();
        tmp.push("put_no_temp");
        std::fs::remove_dir_all(&tmp).ok();

        let store = FsBlockStore::new(tmp.clone());
        store.init().await.unwrap();

        let data = b"hello durable block".to_vec();
        let cid = Cid::new_v1(BlockCodec::Raw.into(), Code::Sha2_256.digest(&data));
        let block = Block::new(cid, data.clone()).unwrap();

        assert_eq!(store.put(&block).await.unwrap().1, BlockPut::NewBlock);
        // re-putting the same block must not clobber it and must report Existed.
        assert_eq!(store.put(&block).await.unwrap().1, BlockPut::Existed);

        // the block reads back intact, and only the canonical .data file remains (no .tmp litter).
        assert_eq!(store.get(&cid).await.unwrap().unwrap(), block);
        let mut files = Vec::new();
        for shard in std::fs::read_dir(&tmp).unwrap() {
            let shard = shard.unwrap().path();
            if shard.is_dir() {
                for f in std::fs::read_dir(&shard).unwrap() {
                    files.push(f.unwrap().path());
                }
            }
        }
        assert_eq!(
            files.len(),
            1,
            "expected only the .data block, got {files:?}"
        );
        assert_eq!(files[0].extension().and_then(|e| e.to_str()), Some("data"));

        std::fs::remove_dir_all(&tmp).ok();
    }

    #[tokio::test]
    async fn remove() {
        // FIXME: why not tempdir?
        let mut tmp = temp_dir();
        tmp.push("remove");
        std::fs::remove_dir_all(&tmp).ok();

        let single = FsBlockStore::new(tmp.clone());

        single.init().await.unwrap();

        let cid = Cid::try_from("QmRgutAxd8t7oGkSm4wmeuByG6M51wcTso6cubDdQtuEfL").unwrap();
        let data = hex!("0a0d08021207666f6f6261720a1807");

        let block = Block::new(cid, data.to_vec()).unwrap();

        assert_eq!(single.list().await.collect::<Vec<_>>().await.len(), 0);

        single.put(&block).await.unwrap();

        // compare the multihash since we store the block named as cidv1
        assert_eq!(
            single.list().await.collect::<Vec<_>>().await[0].hash(),
            cid.hash()
        );

        single.remove(&cid).await.unwrap();
        assert_eq!(single.list().await.collect::<Vec<_>>().await.len(), 0);
    }
}
