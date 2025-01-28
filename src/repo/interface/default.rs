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
use crate::repo::{
    lock, BlockPut, BlockStore, DataStore, Lock, LockError, PinStore, References, RepoStorage,
};
use crate::{Block, PinKind, PinMode};
use anyhow::Error;
use async_trait::async_trait;
use futures::future::Either;
use futures::stream::BoxStream;
use ipld_core::cid::Cid;

#[cfg(target_arch = "wasm32")]
use std::sync::Arc;

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

impl RepoStorage for DefaultStorage {}

#[async_trait]
impl BlockStore for DefaultStorage {
    async fn init(&self) -> Result<(), Error> {
        match self.blockstore {
            Either::Left(ref blockstore) => blockstore.init().await,
            Either::Right(ref blockstore) => blockstore.init().await,
        }
    }

    async fn contains(&self, cid: &Cid) -> Result<bool, Error> {
        match self.blockstore {
            Either::Left(ref blockstore) => blockstore.contains(cid).await,
            Either::Right(ref blockstore) => blockstore.contains(cid).await,
        }
    }

    async fn get(&self, cid: &Cid) -> Result<Option<Block>, Error> {
        match self.blockstore {
            Either::Left(ref blockstore) => blockstore.get(cid).await,
            Either::Right(ref blockstore) => blockstore.get(cid).await,
        }
    }

    async fn size(&self, cid: &[Cid]) -> Result<Option<usize>, Error> {
        match self.blockstore {
            Either::Left(ref blockstore) => blockstore.size(cid).await,
            Either::Right(ref blockstore) => blockstore.size(cid).await,
        }
    }

    async fn total_size(&self) -> Result<usize, Error> {
        match self.blockstore {
            Either::Left(ref blockstore) => blockstore.total_size().await,
            Either::Right(ref blockstore) => blockstore.total_size().await,
        }
    }

    async fn put(&self, block: &Block) -> Result<(Cid, BlockPut), Error> {
        match self.blockstore {
            Either::Left(ref blockstore) => blockstore.put(block).await,
            Either::Right(ref blockstore) => blockstore.put(block).await,
        }
    }

    async fn remove(&self, cid: &Cid) -> Result<(), Error> {
        match self.blockstore {
            Either::Left(ref blockstore) => blockstore.remove(cid).await,
            Either::Right(ref blockstore) => blockstore.remove(cid).await,
        }
    }

    async fn remove_many(&self, blocks: BoxStream<'static, Cid>) -> BoxStream<'static, Cid> {
        match self.blockstore {
            Either::Left(ref blockstore) => blockstore.remove_many(blocks).await,
            Either::Right(ref blockstore) => blockstore.remove_many(blocks).await,
        }
    }

    async fn list(&self) -> BoxStream<'static, Cid> {
        match self.blockstore {
            Either::Left(ref blockstore) => blockstore.list().await,
            Either::Right(ref blockstore) => blockstore.list().await,
        }
    }
}

#[async_trait]
impl DataStore for DefaultStorage {
    async fn init(&self) -> Result<(), Error> {
        match self.datastore {
            Either::Left(ref datastore) => datastore.init().await,
            Either::Right(ref datastore) => datastore.init().await,
        }
    }

    async fn contains(&self, key: &[u8]) -> Result<bool, Error> {
        match self.datastore {
            Either::Left(ref datastore) => datastore.contains(key).await,
            Either::Right(ref datastore) => datastore.contains(key).await,
        }
    }

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        match self.datastore {
            Either::Left(ref datastore) => datastore.get(key).await,
            Either::Right(ref datastore) => datastore.get(key).await,
        }
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        match self.datastore {
            Either::Left(ref datastore) => datastore.put(key, value).await,
            Either::Right(ref datastore) => datastore.put(key, value).await,
        }
    }

    async fn remove(&self, key: &[u8]) -> Result<(), Error> {
        match self.datastore {
            Either::Left(ref datastore) => datastore.remove(key).await,
            Either::Right(ref datastore) => datastore.remove(key).await,
        }
    }

    async fn iter(&self) -> BoxStream<'static, (Vec<u8>, Vec<u8>)> {
        match self.datastore {
            Either::Left(ref datastore) => datastore.iter().await,
            Either::Right(ref datastore) => datastore.iter().await,
        }
    }
}

#[async_trait]
impl PinStore for DefaultStorage {
    async fn is_pinned(&self, block: &Cid) -> Result<bool, Error> {
        match self.datastore {
            Either::Left(ref datastore) => datastore.is_pinned(block).await,
            Either::Right(ref datastore) => datastore.is_pinned(block).await,
        }
    }

    async fn insert_direct_pin(&self, target: &Cid) -> Result<(), Error> {
        match self.datastore {
            Either::Left(ref datastore) => datastore.insert_direct_pin(target).await,
            Either::Right(ref datastore) => datastore.insert_direct_pin(target).await,
        }
    }

    async fn insert_recursive_pin(
        &self,
        target: &Cid,
        referenced: References<'_>,
    ) -> Result<(), Error> {
        match self.datastore {
            Either::Left(ref datastore) => datastore.insert_recursive_pin(target, referenced).await,
            Either::Right(ref datastore) => {
                datastore.insert_recursive_pin(target, referenced).await
            }
        }
    }

    async fn remove_direct_pin(&self, target: &Cid) -> Result<(), Error> {
        match self.datastore {
            Either::Left(ref datastore) => datastore.remove_direct_pin(target).await,
            Either::Right(ref datastore) => datastore.remove_direct_pin(target).await,
        }
    }

    async fn remove_recursive_pin(
        &self,
        target: &Cid,
        referenced: References<'_>,
    ) -> Result<(), Error> {
        match self.datastore {
            Either::Left(ref datastore) => datastore.remove_recursive_pin(target, referenced).await,
            Either::Right(ref datastore) => {
                datastore.remove_recursive_pin(target, referenced).await
            }
        }
    }

    async fn list(
        &self,
        mode: Option<PinMode>,
    ) -> BoxStream<'static, Result<(Cid, PinMode), Error>> {
        match self.datastore {
            Either::Left(ref datastore) => datastore.list(mode).await,
            Either::Right(ref datastore) => datastore.list(mode).await,
        }
    }

    async fn query(
        &self,
        ids: Vec<Cid>,
        requirement: Option<PinMode>,
    ) -> Result<Vec<(Cid, PinKind<Cid>)>, Error> {
        match self.datastore {
            Either::Left(ref datastore) => datastore.query(ids, requirement).await,
            Either::Right(ref datastore) => datastore.query(ids, requirement).await,
        }
    }
}

impl Lock for DefaultStorage {
    fn try_exclusive(&self) -> Result<(), LockError> {
        match self.lockfile {
            Either::Left(ref lockfile) => lockfile.try_exclusive(),
            Either::Right(ref lockfile) => lockfile.try_exclusive(),
        }
    }
}
