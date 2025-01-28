use crate::repo::{
    BlockPut, BlockStore, DataStore, DefaultStorage, Lock, LockError, PinStore, References,
    RepoStorage,
};
use crate::{Block, PinKind, PinMode};
use anyhow::Error;
use async_trait::async_trait;
use futures::future::Either;
use futures::stream::BoxStream;
use ipld_core::cid::Cid;

#[derive(Debug)]
pub struct Storage<B, D, L> {
    blockstore: B,
    datastore: D,
    lockfile: L,
}

impl<B, D, L> Storage<B, D, L> {
    pub fn new(blockstore: B, datastore: D, lockfile: L) -> Self {
        Self {
            blockstore,
            datastore,
            lockfile,
        }
    }
}

impl<B: BlockStore + Clone, D: DataStore + Clone, L: Lock + Clone> Clone for Storage<B, D, L> {
    fn clone(&self) -> Self {
        Self {
            blockstore: self.blockstore.clone(),
            datastore: self.datastore.clone(),
            lockfile: self.lockfile.clone(),
        }
    }
}

impl<B: BlockStore, D: DataStore, L: Lock> RepoStorage for Storage<B, D, L> {}

#[async_trait]
impl<B: BlockStore, D: DataStore, L: Lock> BlockStore for Storage<B, D, L> {
    async fn init(&self) -> Result<(), anyhow::Error> {
        self.blockstore.init().await
    }

    async fn contains(&self, cid: &Cid) -> Result<bool, Error> {
        self.blockstore.contains(cid).await
    }

    async fn get(&self, cid: &Cid) -> Result<Option<Block>, Error> {
        self.blockstore.get(cid).await
    }

    async fn size(&self, cid: &[Cid]) -> Result<Option<usize>, Error> {
        self.blockstore.size(cid).await
    }

    async fn total_size(&self) -> Result<usize, Error> {
        self.blockstore.total_size().await
    }

    async fn put(&self, block: &Block) -> Result<(Cid, BlockPut), Error> {
        self.blockstore.put(block).await
    }

    async fn remove(&self, cid: &Cid) -> Result<(), Error> {
        self.blockstore.remove(cid).await
    }

    async fn remove_many(&self, blocks: BoxStream<'static, Cid>) -> BoxStream<'static, Cid> {
        self.blockstore.remove_many(blocks).await
    }

    async fn list(&self) -> BoxStream<'static, Cid> {
        self.blockstore.list().await
    }
}

#[async_trait]
impl<B: BlockStore, D: DataStore, L: Lock> DataStore for Storage<B, D, L> {
    async fn init(&self) -> Result<(), Error> {
        self.datastore.init().await
    }

    async fn contains(&self, key: &[u8]) -> Result<bool, Error> {
        self.datastore.contains(key).await
    }

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        self.datastore.get(key).await
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        self.datastore.put(key, value).await
    }

    async fn remove(&self, key: &[u8]) -> Result<(), Error> {
        self.datastore.remove(key).await
    }

    async fn iter(&self) -> BoxStream<'static, (Vec<u8>, Vec<u8>)> {
        self.datastore.iter().await
    }
}

#[async_trait]
impl<B: BlockStore, D: DataStore, L: Lock> PinStore for Storage<B, D, L> {
    async fn is_pinned(&self, block: &Cid) -> Result<bool, Error> {
        self.datastore.is_pinned(block).await
    }

    async fn insert_direct_pin(&self, target: &Cid) -> Result<(), Error> {
        self.datastore.insert_direct_pin(target).await
    }

    async fn insert_recursive_pin(
        &self,
        target: &Cid,
        referenced: References<'_>,
    ) -> Result<(), Error> {
        self.datastore
            .insert_recursive_pin(target, referenced)
            .await
    }

    async fn remove_direct_pin(&self, target: &Cid) -> Result<(), Error> {
        self.datastore.remove_direct_pin(target).await
    }

    async fn remove_recursive_pin(
        &self,
        target: &Cid,
        referenced: References<'_>,
    ) -> Result<(), Error> {
        self.datastore
            .remove_recursive_pin(target, referenced)
            .await
    }

    async fn list(
        &self,
        mode: Option<PinMode>,
    ) -> BoxStream<'static, Result<(Cid, PinMode), Error>> {
        self.datastore.list(mode).await
    }

    async fn query(
        &self,
        ids: Vec<Cid>,
        requirement: Option<PinMode>,
    ) -> Result<Vec<(Cid, PinKind<Cid>)>, Error> {
        self.datastore.query(ids, requirement).await
    }
}

impl<B: BlockStore, D: DataStore, L: Lock> Lock for Storage<B, D, L> {
    fn try_exclusive(&self) -> Result<(), LockError> {
        self.lockfile.try_exclusive()
    }
}
