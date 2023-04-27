//! Volatile memory backed repo
use crate::error::Error;
use crate::repo::{BlockPut, BlockStore};
use crate::Block;
use async_trait::async_trait;
use hash_hasher::HashedMap;
use libipld::Cid;

use std::path::PathBuf;
use tokio::sync::Mutex;

use crate::repo::{BlockRm, BlockRmError, RepoCid};

/// Describes an in-memory block store.
///
/// Blocks are stored as a `HashMap` of the `Cid` and `Block`.
#[derive(Debug, Default)]
pub struct MemBlockStore {
    blocks: Mutex<HashedMap<RepoCid, Block>>,
}

impl MemBlockStore {
    pub fn new(_: PathBuf) -> Self {
        Default::default()
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
        let contains = self
            .blocks
            .lock()
            .await
            .contains_key(&RepoCid(cid.to_owned()));
        Ok(contains)
    }

    async fn get(&self, cid: &Cid) -> Result<Option<Block>, Error> {
        let block = self
            .blocks
            .lock()
            .await
            .get(&RepoCid(cid.to_owned()))
            .map(|block| block.to_owned());
        Ok(block)
    }

    async fn put(&self, block: Block) -> Result<(Cid, BlockPut), Error> {
        use std::collections::hash_map::Entry;
        let mut g = self.blocks.lock().await;
        match g.entry(RepoCid(*block.cid())) {
            Entry::Occupied(_) => {
                trace!("already existing block");
                Ok((*block.cid(), BlockPut::Existed))
            }
            Entry::Vacant(ve) => {
                trace!("new block");
                let cid = ve.key().0;
                ve.insert(block);
                Ok((cid, BlockPut::NewBlock))
            }
        }
    }

    async fn remove(&self, cid: &Cid) -> Result<Result<BlockRm, BlockRmError>, Error> {
        match self.blocks.lock().await.remove(&RepoCid(cid.to_owned())) {
            Some(_block) => Ok(Ok(BlockRm::Removed(*cid))),
            None => Ok(Err(BlockRmError::NotFound(*cid))),
        }
    }

    async fn list(&self) -> Result<Vec<Cid>, Error> {
        let guard = self.blocks.lock().await;
        Ok(guard.iter().map(|(cid, _block)| cid.0).collect())
    }

    async fn wipe(&self) {
        self.blocks.lock().await.clear();
    }
}

// Used for in memory repos, currently not implementing any true locking.


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
        if store.remove(&cid).await.unwrap().is_ok() {
            panic!("block should not be found")
        }

        let put = store.put(block.clone());
        assert_eq!(put.await.unwrap().0, cid.to_owned());
        let contains = store.contains(&cid);
        assert!(contains.await.unwrap());
        let get = store.get(&cid);
        assert_eq!(get.await.unwrap(), Some(block.clone()));

        store.remove(&cid).await.unwrap().unwrap();
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
