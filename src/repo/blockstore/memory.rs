//! Volatile memory backed repo
use crate::error::Error;
use crate::repo::{BlockPut, BlockStore};
use crate::Block;
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::StreamExt;
use libipld::Cid;
use tokio::sync::RwLock;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

/// Describes an in-memory block store.
///
/// Blocks are stored as a `HashMap` of the `Cid` and `Block`.
pub struct MemBlockStore {
    inner: Arc<RwLock<MemBlockInner>>,
}

impl std::fmt::Debug for MemBlockStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemBlockStore").finish()
    }
}

struct MemBlockInner {
    blocks: HashMap<Cid, Block>,
}

impl MemBlockStore {
    pub fn new(_: PathBuf) -> Self {
        let inner = MemBlockInner {
            blocks: HashMap::new(),
        };

        let inner = Arc::new(RwLock::new(inner));

        Self { inner }
    }
}

#[async_trait]
impl BlockStore for MemBlockStore {
    async fn init(&self) -> Result<(), Error> {
        Ok(())
    }

    async fn open(&self) -> Result<(), Error> {
        Ok(())
    }

    async fn contains(&self, cid: &Cid) -> Result<bool, Error> {
        let inner = &*self.inner.read().await;
        Ok(inner.blocks.contains_key(cid))
    }

    async fn get(&self, cid: &Cid) -> Result<Option<Block>, Error> {
        let inner = &*self.inner.read().await;
        let block = inner.blocks.get(cid).cloned();
        Ok(block)
    }

    async fn size(&self, cid: &[Cid]) -> Result<Option<usize>, Error> {
        let inner = &*self.inner.read().await;
        Ok(Some(
            inner
                .blocks
                .iter()
                .filter(|(id, _)| cid.contains(id))
                .map(|(_, b)| b.data().len())
                .sum(),
        ))
    }

    async fn total_size(&self) -> Result<usize, Error> {
        let inner = &*self.inner.read().await;
        Ok(inner.blocks.values().map(|b| b.data().len()).sum())
    }

    async fn put(&self, block: Block) -> Result<(Cid, BlockPut), Error> {
        use std::collections::hash_map::Entry;

        let inner = &mut *self.inner.write().await;
        let cid = *block.cid();
        match inner.blocks.entry(cid) {
            Entry::Occupied(_) => {
                trace!("already existing block");
                Ok((*block.cid(), BlockPut::Existed))
            }
            Entry::Vacant(ve) => {
                trace!("new block");
                let cid = *ve.key();
                ve.insert(block);
                Ok((cid, BlockPut::NewBlock))
            }
        }
    }

    async fn remove(&self, cid: &Cid) -> Result<(), Error> {
        let inner = &mut *self.inner.write().await;

        match inner.blocks.remove(cid) {
            Some(_block) => Ok(()),
            None => Err(std::io::Error::from(std::io::ErrorKind::NotFound).into()),
        }
    }

    async fn remove_many(&self, blocks: &[Cid]) -> Result<BoxStream<'static, Cid>, Error> {
        let inner = self.inner.clone();
        let blocks = blocks.to_vec();

        let stream = async_stream::stream! {
            let inner = &mut *inner.write().await;
            for cid in blocks {
                if inner.blocks.remove(&cid).is_some() {
                    yield cid;
                }
            }
        };

        Ok(stream.boxed())
    }

    async fn list(&self) -> Result<Vec<Cid>, Error> {
        let inner = &*self.inner.read().await;
        Ok(inner.blocks.keys().copied().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Block;
    use libipld::{
        multihash::{Code, MultihashDigest},
        IpldCodec,
    };

    #[tokio::test]
    async fn test_mem_blockstore() {
        let tmp = std::env::temp_dir();
        let store = MemBlockStore::new(tmp);
        let data = b"1".to_vec();
        let cid = Cid::new_v1(IpldCodec::Raw.into(), Code::Sha2_256.digest(&data));
        let block = Block::new(cid, data).unwrap();

        store.init().await.unwrap();
        store.open().await.unwrap();

        let contains = store.contains(&cid);
        assert!(!contains.await.unwrap());
        let get = store.get(&cid);
        assert_eq!(get.await.unwrap(), None);
        if store.remove(&cid).await.is_ok() {
            panic!("block should not be found")
        }

        let put = store.put(block.clone());
        assert_eq!(put.await.unwrap().0, cid.to_owned());
        let contains = store.contains(&cid);
        assert!(contains.await.unwrap());
        let get = store.get(&cid);
        assert_eq!(get.await.unwrap(), Some(block.clone()));

        store.remove(&cid).await.unwrap();
        let contains = store.contains(&cid);
        assert!(!contains.await.unwrap());
        let get = store.get(&cid);
        assert_eq!(get.await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_mem_blockstore_list() {
        let tmp = std::env::temp_dir();
        let mem_store = MemBlockStore::new(tmp);

        mem_store.init().await.unwrap();
        mem_store.open().await.unwrap();

        for data in &[b"1", b"2", b"3"] {
            let data_slice = data.to_vec();
            let cid = Cid::new_v1(IpldCodec::Raw.into(), Code::Sha2_256.digest(&data_slice));
            let block = Block::new(cid, data_slice).unwrap();
            mem_store.put(block.clone()).await.unwrap();
            assert!(mem_store.contains(block.cid()).await.unwrap());
        }

        let cids = mem_store.list().await.unwrap();
        assert_eq!(cids.len(), 3);
        for cid in cids.iter() {
            assert!(mem_store.contains(cid).await.unwrap());
        }
    }
}
