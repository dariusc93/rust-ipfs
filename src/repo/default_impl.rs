#[cfg(not(target_arch = "wasm32"))]
use crate::repo::blockstore::flatfs::FsBlockStore;
#[cfg(target_arch = "wasm32")]
use crate::repo::blockstore::idb::IdbBlockStore;
use crate::repo::blockstore::memory::MemBlockStore;
#[cfg(not(target_arch = "wasm32"))]
use crate::repo::datastore::flatfs::FsDataStore;
#[cfg(target_arch = "wasm32")]
use crate::repo::datastore::idb::IdbDataStore;
use crate::repo::datastore::memory::MemDataStore;
use crate::repo::{lock, RepoTypes};
use crate::Block;

#[cfg(target_arch = "wasm32")]
use std::sync::Arc;

use async_trait::async_trait;
use either::Either;
use futures::stream::BoxStream;
use ipld_core::cid::Cid;

use crate::error::Error;

use super::{
    BlockPut, BlockStore, DataStore, Lock, LockError, PinKind, PinMode, PinStore, References,
};

#[derive(Debug)]
#[cfg(not(target_arch = "wasm32"))]
pub struct DefaultStorage {
    blockstore: Either<MemBlockStore, FsBlockStore>,
    datastore: Either<MemDataStore, FsDataStore>,
    lockfile: Either<lock::MemLock, lock::FsLock>,
}

#[derive(Debug)]
#[cfg(target_arch = "wasm32")]
pub struct DefaultStorage {
    blockstore: Either<MemBlockStore, Arc<IdbBlockStore>>,
    datastore: Either<MemDataStore, Arc<IdbDataStore>>,
    lockfile: Either<lock::MemLock, lock::MemLock>,
}

impl Default for DefaultStorage {
    fn default() -> Self {
        Self {
            blockstore: Either::Left(MemBlockStore::new(Default::default())),
            datastore: Either::Left(MemDataStore::new(Default::default())),
            lockfile: Either::Left(lock::MemLock),
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl DefaultStorage {
    /// Set path to trigger persistent storage
    #[allow(dead_code)]
    pub(crate) fn set_path(&mut self, path: impl AsRef<std::path::Path>) {
        let path = path.as_ref().to_path_buf();
        self.blockstore = Either::Right(FsBlockStore::new(path.clone()));
        self.datastore = Either::Right(FsDataStore::new(path.clone()));
        self.lockfile = Either::Right(lock::FsLock::new(path.clone()));
    }

    pub(crate) fn set_blockstore_path(&mut self, path: impl AsRef<std::path::Path>) {
        let path = path.as_ref().to_path_buf();
        self.blockstore = Either::Right(FsBlockStore::new(path.clone()));
    }

    pub(crate) fn set_datastore_path(&mut self, path: impl AsRef<std::path::Path>) {
        let path = path.as_ref().to_path_buf();
        self.datastore = Either::Right(FsDataStore::new(path.clone()));
    }

    pub(crate) fn set_lockfile(&mut self, path: impl AsRef<std::path::Path>) {
        let path = path.as_ref().to_path_buf();
        self.lockfile = Either::Right(lock::FsLock::new(path.clone()));
    }

    #[allow(dead_code)]
    pub(crate) fn remove_paths(&mut self) {
        self.blockstore = Either::Left(MemBlockStore::new(Default::default()));
        self.datastore = Either::Left(MemDataStore::new(Default::default()));
        self.lockfile = Either::Left(lock::MemLock);
    }
}

#[cfg(target_arch = "wasm32")]
impl DefaultStorage {
    /// Set path to trigger persistent storage
    pub(crate) fn set_namespace(&mut self, namespace: impl Into<Option<String>>) {
        let namespace = namespace.into();
        self.blockstore = Either::Right(Arc::new(IdbBlockStore::new(namespace.clone())));
        self.datastore = Either::Right(Arc::new(IdbDataStore::new(namespace.clone())));
        self.lockfile = Either::Right(lock::MemLock);
    }

    #[allow(dead_code)]
    pub(crate) fn remove_namespace(&mut self) {
        self.blockstore = Either::Left(MemBlockStore::new(Default::default()));
        self.datastore = Either::Left(MemDataStore::new(Default::default()));
        self.lockfile = Either::Left(lock::MemLock);
    }
}

impl Clone for DefaultStorage {
    fn clone(&self) -> Self {
        Self {
            blockstore: self.blockstore.clone(),
            datastore: self.datastore.clone(),
            lockfile: self.lockfile.clone(),
        }
    }
}

impl RepoTypes for DefaultStorage {
    type TBlockStore = DefaultStorage;
    type TDataStore = DefaultStorage;
    type TLock = DefaultStorage;
}

impl Unpin for DefaultStorage {}

#[async_trait]
impl BlockStore for DefaultStorage {
    async fn init(&self) -> Result<(), Error> {
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
impl DataStore for DefaultStorage {
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
impl PinStore for DefaultStorage {
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

impl Lock for DefaultStorage {
    fn try_exclusive(&self) -> Result<(), LockError> {
        self.lockfile.try_exclusive()
    }
}
