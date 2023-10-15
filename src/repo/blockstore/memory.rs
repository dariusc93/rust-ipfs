//! Volatile memory backed repo
use crate::error::Error;
use crate::repo::{BlockPut, BlockStore};
use crate::Block;
use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use libipld::Cid;

use std::collections::HashMap;
use std::path::PathBuf;

use crate::repo::{BlockRm, BlockRmError};

use super::RepoBlockCommand;

/// Describes an in-memory block store.
///
/// Blocks are stored as a `HashMap` of the `Cid` and `Block`.
#[derive(Debug)]
pub struct MemBlockStore {
    tx: futures::channel::mpsc::Sender<RepoBlockCommand>,
}

struct MemBlockTask {
    blocks: HashMap<Cid, Block>,
    rx: futures::channel::mpsc::Receiver<RepoBlockCommand>,
}

impl MemBlockStore {
    pub fn new(_: PathBuf) -> Self {
        let (tx, rx) = futures::channel::mpsc::channel(1);
        let mut task = MemBlockTask {
            blocks: HashMap::new(),
            rx,
        };

        tokio::spawn(async move {
            task.start().await;
        });

        Self { tx }
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

// Used for in memory repos, currently not implementing any true locking.

impl MemBlockTask {
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

impl MemBlockTask {
    async fn contains(&self, cid: &Cid) -> Result<bool, Error> {
        let contains = self.blocks.contains_key(cid);
        Ok(contains)
    }

    async fn get(&self, cid: &Cid) -> Result<Option<Block>, Error> {
        let block = self.blocks.get(cid).cloned();
        Ok(block)
    }

    async fn put(&mut self, block: Block) -> Result<(Cid, BlockPut), Error> {
        use std::collections::hash_map::Entry;
        match self.blocks.entry(*block.cid()) {
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

    async fn remove(&mut self, cid: &Cid) -> Result<Result<BlockRm, BlockRmError>, Error> {
        match self.blocks.remove(cid) {
            Some(_block) => Ok(Ok(BlockRm::Removed(*cid))),
            None => Ok(Err(BlockRmError::NotFound(*cid))),
        }
    }

    async fn list(&self) -> Result<Vec<Cid>, Error> {
        Ok(self.blocks.keys().cloned().collect())
    }

    async fn wipe(&mut self) {
        self.blocks.clear();
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
