use crate::error::Error;
use crate::repo::paths::{block_path, filestem_to_block_cid};
use crate::repo::{BlockPut, BlockStore};
use crate::repo::{BlockRm, BlockRmError};
use crate::Block;
use async_trait::async_trait;
use futures::{SinkExt, StreamExt, TryStreamExt};
use libipld::Cid;
use std::io::{ErrorKind, Read};
use std::path::PathBuf;
use tokio::fs;
use tokio_stream::wrappers::ReadDirStream;

use super::RepoBlockCommand;

/// File system backed block store.
///
/// For information on path mangling, please see `block_path` and `filestem_to_block_cid`.
#[derive(Debug)]
pub struct FsBlockStore {
    path: PathBuf,
    tx: futures::channel::mpsc::Sender<RepoBlockCommand>,
}

pub struct FsBlockStoreTask {
    path: PathBuf,

    rx: futures::channel::mpsc::Receiver<RepoBlockCommand>,
}

impl FsBlockStore {
    pub fn new(path: PathBuf) -> Self {
        let (tx, rx) = futures::channel::mpsc::channel(1);
        let mut task = FsBlockStoreTask {
            path: path.clone(),
            rx,
        };

        tokio::spawn(async move {
            task.start().await;
        });

        Self { path, tx }
    }
}

#[async_trait]
impl BlockStore for FsBlockStore {
    async fn init(&self) -> Result<(), Error> {
        fs::create_dir_all(self.path.clone()).await?;
        Ok(())
    }

    async fn open(&self) -> Result<(), Error> {
        // TODO: we probably want to cache the space usage?
        Ok(())
    }

    async fn contains(&self, cid: &Cid) -> Result<bool, Error> {
        let (tx, rx) = futures::channel::oneshot::channel();
        let _ = self
            .tx
            .clone()
            .send(RepoBlockCommand::Contains {
                cid: *cid,
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }

    async fn get(&self, cid: &Cid) -> Result<Option<Block>, Error> {
        let (tx, rx) = futures::channel::oneshot::channel();
        let _ = self
            .tx
            .clone()
            .send(RepoBlockCommand::Get {
                cid: *cid,
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }

    async fn put(&self, block: Block) -> Result<(Cid, BlockPut), Error> {
        let (tx, rx) = futures::channel::oneshot::channel();
        let _ = self
            .tx
            .clone()
            .send(RepoBlockCommand::PutBlock {
                block,
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }

    async fn remove(&self, cid: &Cid) -> Result<Result<BlockRm, BlockRmError>, Error> {
        let (tx, rx) = futures::channel::oneshot::channel();
        let _ = self
            .tx
            .clone()
            .send(RepoBlockCommand::Remove {
                cid: *cid,
                response: tx,
            })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }

    async fn list(&self) -> Result<Vec<Cid>, Error> {
        let (tx, rx) = futures::channel::oneshot::channel();
        let _ = self
            .tx
            .clone()
            .send(RepoBlockCommand::List { response: tx })
            .await;
        rx.await.map_err(anyhow::Error::from)?
    }

    async fn wipe(&self) {
        let (tx, rx) = futures::channel::oneshot::channel();
        let _ = self
            .tx
            .clone()
            .send(RepoBlockCommand::Wipe { response: tx })
            .await;
        let _ = rx.await.map_err(anyhow::Error::from);
    }
}

impl FsBlockStoreTask {
    async fn start(&mut self) {
        while let Some(command) = self.rx.next().await {
            match command {
                RepoBlockCommand::Contains { cid, response } => {
                    let _ = response.send(self.contains(&cid).await);
                }
                RepoBlockCommand::Get { cid, response } => {
                    let _ = response.send(self.get(&cid).await);
                }
                RepoBlockCommand::PutBlock { block, response } => {
                    let _ = response.send(self.put(block).await);
                }
                RepoBlockCommand::Remove { cid, response } => {
                    let _ = response.send(self.remove(&cid).await);
                }
                RepoBlockCommand::List { response } => {
                    let _ = response.send(self.list().await);
                }
                RepoBlockCommand::Wipe { response } => {
                    let _ = response.send({
                        self.wipe().await;
                        Ok(())
                    });
                }
            }
        }
    }
}

impl FsBlockStoreTask {
    async fn contains(&self, cid: &Cid) -> Result<bool, Error> {
        let path = block_path(self.path.clone(), cid);

        let metadata = match fs::metadata(path).await {
            Ok(m) => m,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
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
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
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

    async fn put(&mut self, block: Block) -> Result<(Cid, BlockPut), Error> {
        let target_path = block_path(self.path.clone(), block.cid());
        let cid = *block.cid();

        let je = tokio::task::spawn_blocking(move || {
            let sharded = target_path
                .parent()
                .expect("we already have at least the shard parent");

            std::fs::create_dir_all(sharded)?;

            let target = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&target_path)?;

            let temp_path = target_path.with_extension("tmp");

            match write_through_tempfile(target, &target_path, temp_path, block.data()) {
                Ok(()) => {
                    trace!("successfully wrote the block");
                    Ok::<_, std::io::Error>(Ok(block.data().len()))
                }
                Err(e) => {
                    match std::fs::remove_file(&target_path) {
                        Ok(_) => debug!("removed partially written {:?}", target_path),
                        Err(removal) => warn!(
                            "failed to remove partially written {:?}: {}",
                            target_path, removal
                        ),
                    }
                    Ok(Err(e))
                }
            }
        })
        .await
        .map_err(|e| {
            error!("blocking put task error: {}", e);
            e
        })?;

        match je {
            Ok(Ok(written)) => {
                trace!(bytes = written, "block writing succeeded");

                Ok((cid, BlockPut::NewBlock))
            }
            Ok(Err(e)) => {
                trace!("write failed but hopefully the target was removed");

                Err(Error::new(e))
            }
            Err(e) if e.kind() == ErrorKind::AlreadyExists => {
                trace!("block exist: {}", e);
                Ok((cid, BlockPut::Existed))
            }
            Err(e) => Err(Error::new(e)),
        }
    }

    async fn remove(&mut self, cid: &Cid) -> Result<Result<BlockRm, BlockRmError>, Error> {
        let path = block_path(self.path.clone(), cid);

        trace!(cid = %cid, "removing block after synchronizing");
        match fs::remove_file(path).await {
            // FIXME: not sure if theres any point in taking cid ownership here?
            Ok(()) => Ok(Ok(BlockRm::Removed(*cid))),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                Ok(Err(BlockRmError::NotFound(*cid)))
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn list(&self) -> Result<Vec<Cid>, Error> {
        let stream = ReadDirStream::new(fs::read_dir(&self.path).await?);

        let vec = stream
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
            .try_collect::<Vec<_>>()
            .await?;

        Ok(vec)
    }

    async fn wipe(&mut self) {}
}

fn write_through_tempfile(
    target: std::fs::File,
    target_path: impl AsRef<std::path::Path>,
    temp_path: impl AsRef<std::path::Path>,
    data: &[u8],
) -> Result<(), std::io::Error> {
    use std::io::Write;

    let mut temp = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&temp_path)?;

    temp.write_all(data)?;
    temp.flush()?;

    // safe default
    temp.sync_all()?;

    drop(temp);
    drop(target);

    std::fs::rename(temp_path, target_path)?;

    // FIXME: there should be a directory fsync here as well

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Block;
    use hex_literal::hex;
    use libipld::{
        multihash::{Code, MultihashDigest},
        Cid, IpldCodec,
    };
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
        let cid = Cid::new_v1(IpldCodec::Raw.into(), Code::Sha2_256.digest(&data));
        let block = Block::new(cid, data).unwrap();

        store.init().await.unwrap();
        store.open().await.unwrap();

        let contains = store.contains(&cid).await.unwrap();
        assert!(!contains);
        let get = store.get(&cid).await.unwrap();
        assert_eq!(get, None);
        if store.remove(&cid).await.unwrap().is_ok() {
            panic!("block should not be found")
        }

        let put = store.put(block.clone()).await.unwrap();
        assert_eq!(put.0, cid.to_owned());
        let contains = store.contains(&cid);
        assert!(contains.await.unwrap());
        let get = store.get(&cid);
        assert_eq!(get.await.unwrap(), Some(block.clone()));

        store.remove(&cid).await.unwrap().unwrap();
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
        let cid = Cid::new_v1(IpldCodec::Raw.into(), Code::Sha2_256.digest(&data));
        let block = Block::new(cid, data).unwrap();

        let block_store = FsBlockStore::new(tmp.clone());
        block_store.init().await.unwrap();
        block_store.open().await.unwrap();

        assert!(!block_store.contains(block.cid()).await.unwrap());
        block_store.put(block.clone()).await.unwrap();

        let block_store = FsBlockStore::new(tmp.clone());
        block_store.open().await.unwrap();
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
        block_store.open().await.unwrap();

        for data in &[b"1", b"2", b"3"] {
            let data_slice = data.to_vec();
            let cid = Cid::new_v1(IpldCodec::Raw.into(), Code::Sha2_256.digest(&data_slice));
            let block = Block::new(cid, data_slice).unwrap();
            block_store.put(block.clone()).await.unwrap();
        }

        let cids = block_store.list().await.unwrap();
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

        let block = Block::new(cid, data.into()).unwrap();

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
                        bs.put(block).await
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
                Ok(Err(e)) => println!("joinhandle err: {e}"),
                _ => unreachable!("join error"),
            }
        }

        (writes, existing)
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

        let block = Block::new(cid, data.into()).unwrap();

        assert_eq!(single.list().await.unwrap().len(), 0);

        single.put(block).await.unwrap();

        // compare the multihash since we store the block named as cidv1
        assert_eq!(single.list().await.unwrap()[0].hash(), cid.hash());

        single.remove(&cid).await.unwrap().unwrap();
        assert_eq!(single.list().await.unwrap().len(), 0);
    }
}
