//! Storage implementation(s) backing the [`crate::Ipfs`].
use crate::error::Error;
use crate::{Block, StorageType};
use anyhow::anyhow;
use async_trait::async_trait;
use core::fmt::Debug;
use futures::channel::mpsc::{channel, Receiver, Sender};
use futures::future::BoxFuture;
use futures::sink::SinkExt;
use futures::stream::{self, BoxStream, FuturesOrdered};
use futures::{FutureExt, StreamExt, TryFutureExt, TryStreamExt};
use futures_timeout::TimeoutExt;
use libipld::cid::Cid;
use libipld::{Ipld, IpldCodec};
use libp2p::identity::PeerId;
use parking_lot::{Mutex, RwLock};
use std::borrow::Borrow;
use std::collections::{BTreeSet, HashMap};
use std::future::IntoFuture;
#[allow(unused_imports)]
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use std::{error, fmt, io};
use tokio::sync::RwLockReadGuard;
use tracing::{log, Span};
use tracing_futures::Instrument;

#[macro_use]
#[cfg(test)]
mod common_tests;

pub mod blockstore;
pub mod datastore;
pub mod lock;

/// Path mangling done for pins and blocks
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod paths;

/// Describes the outcome of `BlockStore::put_block`.
#[derive(Debug, PartialEq, Eq)]
pub enum BlockPut {
    /// A new block was written to the blockstore.
    NewBlock,
    /// The block already exists.
    Existed,
}

/// Describes the outcome of `BlockStore::remove`.
#[derive(Debug)]
pub enum BlockRm {
    /// A block was successfully removed from the blockstore.
    Removed(Cid),
    // TODO: DownloadCancelled(Cid, Duration),
}

// pub struct BlockNotFound(Cid);
/// Describes the error variants for `BlockStore::remove`.
#[derive(Debug)]
pub enum BlockRmError {
    // TODO: Pinned(Cid),
    /// The `Cid` doesn't correspond to a block in the blockstore.
    NotFound(Cid),
}

/// This API is being discussed and evolved, which will likely lead to breakage.
#[async_trait]
pub trait BlockStore: Debug + Send + Sync + 'static {
    async fn init(&self) -> Result<(), Error>;
    /// FIXME: redundant and never called during initialization, which is expected to happen during [`init`].
    async fn open(&self) -> Result<(), Error>;
    /// Returns whether a block is present in the blockstore.
    async fn contains(&self, cid: &Cid) -> Result<bool, Error>;
    /// Returns a block from the blockstore.
    async fn get(&self, cid: &Cid) -> Result<Option<Block>, Error>;
    /// Get the size of a single block
    async fn size(&self, cid: &[Cid]) -> Result<Option<usize>, Error>;
    /// Get a total size of the block store
    async fn total_size(&self) -> Result<usize, Error>;
    /// Inserts a block in the blockstore.
    async fn put(&self, block: Block) -> Result<(Cid, BlockPut), Error>;
    /// Removes a block from the blockstore.
    async fn remove(&self, cid: &Cid) -> Result<(), Error>;
    /// Remove multiple blocks from the blockstore
    async fn remove_many(&self, blocks: BoxStream<'static, Cid>) -> BoxStream<'static, Cid>;
    /// Returns a list of the blocks (Cids), in the blockstore.
    async fn list(&self) -> BoxStream<'static, Cid>;
}

#[async_trait]
/// Generic layer of abstraction for a key-value data store.
pub trait DataStore: PinStore + Debug + Send + Sync + 'static {
    async fn init(&self) -> Result<(), Error>;
    async fn open(&self) -> Result<(), Error>;
    /// Checks if a key is present in the datastore.
    async fn contains(&self, key: &[u8]) -> Result<bool, Error>;
    /// Returns the value associated with a key from the datastore.
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error>;
    /// Puts the value under the key in the datastore.
    async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Error>;
    /// Removes a key-value pair from the datastore.
    async fn remove(&self, key: &[u8]) -> Result<(), Error>;
    /// Iterate over the k/v of the datastore
    async fn iter(&self) -> futures::stream::BoxStream<'static, (Vec<u8>, Vec<u8>)>;
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct GCConfig {
    /// How long until GC runs
    /// If duration is not set, it will not run at a timer
    pub duration: Duration,

    /// What will trigger GC
    pub trigger: GCTrigger,
}

impl Default for GCConfig {
    fn default() -> Self {
        Self {
            duration: Duration::from_secs(60 * 60),
            trigger: GCTrigger::default(),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
pub enum GCTrigger {
    /// At a specific size. If the size is at or exceeds, it will trigger GC
    At {
        size: usize,
    },

    AtStorage,

    /// No trigger
    #[default]
    None,
}

/// Errors variants describing the possible failures for `Lock::try_exclusive`.
#[derive(Debug)]
pub enum LockError {
    RepoInUse,
    LockFileOpenFailed(io::Error),
}

impl fmt::Display for LockError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let msg = match self {
            LockError::RepoInUse => "The repository is already being used by an IPFS instance.",
            LockError::LockFileOpenFailed(_) => "Failed to open repository lock file.",
        };

        write!(f, "{msg}")
    }
}

impl From<io::Error> for LockError {
    fn from(error: io::Error) -> Self {
        match error.kind() {
            // `WouldBlock` is not used by `OpenOptions` (this could change), and can therefore be
            // matched on for the fs2 error in `FsLock::try_exclusive`.
            io::ErrorKind::WouldBlock => LockError::RepoInUse,
            _ => LockError::LockFileOpenFailed(error),
        }
    }
}

impl error::Error for LockError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        if let Self::LockFileOpenFailed(error) = self {
            Some(error)
        } else {
            None
        }
    }
}

/// A trait for describing repository locking.
///
/// This ensures no two IPFS nodes can be started with the same peer ID, as exclusive access to the
/// repository is guarenteed. This is most useful when using an fs backed repo.
pub trait Lock: Debug + Send + Sync + 'static {
    // fn new(path: PathBuf) -> Self;
    fn try_exclusive(&self) -> Result<(), LockError>;
}

type References<'a> = futures::stream::BoxStream<'a, Result<Cid, crate::refs::IpldRefsError>>;

#[async_trait]
pub trait PinStore: Debug + Send + Sync + Unpin + 'static {
    async fn is_pinned(&self, block: &Cid) -> Result<bool, Error>;

    async fn insert_direct_pin(&self, target: &Cid) -> Result<(), Error>;

    async fn insert_recursive_pin(
        &self,
        target: &Cid,
        referenced: References<'_>,
    ) -> Result<(), Error>;

    async fn remove_direct_pin(&self, target: &Cid) -> Result<(), Error>;

    async fn remove_recursive_pin(
        &self,
        target: &Cid,
        referenced: References<'_>,
    ) -> Result<(), Error>;

    async fn list(
        &self,
        mode: Option<PinMode>,
    ) -> futures::stream::BoxStream<'static, Result<(Cid, PinMode), Error>>;

    // here we should have resolved ids
    // go-ipfs: doesnt start fetching the paths
    // js-ipfs: starts fetching paths
    // FIXME: there should probably be an additional Result<$inner, Error> here; the per pin error
    // is serde OR cid::Error.
    /// Returns error if any of the ids isn't pinned in the required type, otherwise returns
    /// the pin details if all of the cids are pinned in one way or the another.
    async fn query(
        &self,
        ids: Vec<Cid>,
        requirement: Option<PinMode>,
    ) -> Result<Vec<(Cid, PinKind<Cid>)>, Error>;
}

/// `PinMode` is the description of pin type for quering purposes.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum PinMode {
    Indirect,
    Direct,
    Recursive,
}

/// Helper for around the quite confusing test required in [`PinStore::list`] and
/// [`PinStore::query`].
#[derive(Debug, Clone, Copy)]
enum PinModeRequirement {
    Only(PinMode),
    Any,
}

impl From<Option<PinMode>> for PinModeRequirement {
    fn from(filter: Option<PinMode>) -> Self {
        match filter {
            Some(one) => PinModeRequirement::Only(one),
            None => PinModeRequirement::Any,
        }
    }
}

#[allow(dead_code)]
impl PinModeRequirement {
    fn is_indirect_or_any(&self) -> bool {
        use PinModeRequirement::*;
        match self {
            Only(PinMode::Indirect) | Any => true,
            Only(_) => false,
        }
    }

    fn matches<P: PartialEq<PinMode>>(&self, other: &P) -> bool {
        use PinModeRequirement::*;
        match self {
            Only(one) if other == one => true,
            Only(_) => false,
            Any => true,
        }
    }

    fn required(&self) -> Option<PinMode> {
        use PinModeRequirement::*;
        match self {
            Only(one) => Some(*one),
            Any => None,
        }
    }
}

impl<B: Borrow<Cid>> PartialEq<PinMode> for PinKind<B> {
    fn eq(&self, other: &PinMode) -> bool {
        matches!(
            (self, other),
            (PinKind::IndirectFrom(_), PinMode::Indirect)
                | (PinKind::Direct, PinMode::Direct)
                | (PinKind::Recursive(_), PinMode::Recursive)
                | (PinKind::RecursiveIntention, PinMode::Recursive)
        )
    }
}

/// `PinKind` is more specific pin description for writing purposes. Implements
/// `PartialEq<&PinMode>`. Generic over `Borrow<Cid>` to allow storing both reference and owned
/// value of Cid.
#[derive(Debug, PartialEq, Eq)]
pub enum PinKind<C: Borrow<Cid>> {
    IndirectFrom(C),
    Direct,
    Recursive(u64),
    RecursiveIntention,
}

impl<C: Borrow<Cid>> PinKind<C> {
    fn as_ref(&self) -> PinKind<&'_ Cid> {
        use PinKind::*;
        match self {
            IndirectFrom(c) => PinKind::IndirectFrom(c.borrow()),
            Direct => PinKind::Direct,
            Recursive(count) => PinKind::Recursive(*count),
            RecursiveIntention => PinKind::RecursiveIntention,
        }
    }
}

type SubscriptionsMap = HashMap<Cid, Vec<futures::channel::oneshot::Sender<Result<Block, String>>>>;

/// Describes a repo.
///
/// Consolidates a blockstore, a datastore and a subscription registry.

#[allow(clippy::type_complexity)]
#[derive(Debug, Clone)]
pub struct Repo {
    pub(crate) inner: Arc<RepoInner>,
}

#[derive(Debug)]
pub(crate) struct RepoInner {
    online: AtomicBool,
    initialized: AtomicBool,
    max_storage_size: AtomicUsize,
    block_store: Box<dyn BlockStore>,
    data_store: Box<dyn DataStore>,
    events: RwLock<Option<Sender<RepoEvent>>>,
    pub(crate) subscriptions: Mutex<SubscriptionsMap>,
    lockfile: Box<dyn Lock>,
    pub(crate) gclock: tokio::sync::RwLock<()>,
}

#[cfg(feature = "beetle_bitswap")]
#[async_trait]
impl beetle_bitswap_next::Store for Repo {
    async fn get_size(&self, cid: &Cid) -> anyhow::Result<usize> {
        self.get_block_now(cid)
            .await?
            .ok_or(anyhow::anyhow!("Block doesnt exist"))
            .map(|block| block.data().len())
    }
    async fn get(&self, cid: &Cid) -> anyhow::Result<beetle_bitswap_next::Block> {
        let block = self
            .get_block_now(cid)
            .await?
            .ok_or(anyhow::anyhow!("Block doesnt exist"))?;
        Ok(beetle_bitswap_next::Block {
            cid: *block.cid(),
            data: bytes::Bytes::copy_from_slice(block.data()),
        })
    }
    async fn has(&self, cid: &Cid) -> anyhow::Result<bool> {
        self.contains(cid).await
    }
}

#[cfg(feature = "libp2p_bitswap")]
#[async_trait]
impl libp2p_bitswap_next::BitswapStore for Repo {
    type Params = libipld::DefaultParams;

    async fn contains(&mut self, cid: &Cid) -> anyhow::Result<bool> {
        self.inner.block_store.contains(cid).await
    }

    async fn get(&mut self, cid: &Cid) -> anyhow::Result<Option<Vec<u8>>> {
        self.inner
            .block_store
            .get(cid)
            .await
            .map(|block| block.map(|block| block.data().to_vec()))
    }

    async fn insert(&mut self, block: &libipld::Block<Self::Params>) -> anyhow::Result<()> {
        self.put_block(block.clone()).await.map(|_| ())
    }

    async fn missing_blocks(&mut self, cid: &Cid) -> anyhow::Result<Vec<Cid>> {
        let mut stack = vec![*cid];
        let mut missing = vec![];
        while let Some(cid) = stack.pop() {
            if let Some(data) = self.get(&cid).await? {
                let block = Block::new_unchecked(cid, data);
                block.references(&mut stack)?;
            } else {
                missing.push(cid);
            }
        }
        Ok(missing)
    }
}

/// Events used to communicate to the swarm on repo changes.
#[derive(Debug)]
pub enum RepoEvent {
    /// Signals a desired block.
    WantBlock(Option<u64>, Vec<Cid>, Vec<PeerId>),
    /// Signals a desired block is no longer wanted.
    UnwantBlock(Cid),
    /// Signals the posession of a new block.
    NewBlock(Block),
    /// Signals the removal of a block.
    RemovedBlock(Cid),
}

impl Repo {
    pub fn new(repo_type: &mut StorageType) -> Self {
        match repo_type {
            StorageType::Memory => Repo::new_memory(),
            #[cfg(not(target_arch = "wasm32"))]
            StorageType::Disk(path) => Repo::new_fs(path),
            #[cfg(target_arch = "wasm32")]
            StorageType::IndexedDb { namespace } => Repo::new_idb(namespace.take()),
            StorageType::Custom {
                blockstore,
                datastore,
                lock,
            } => Repo::new_raw(
                blockstore.take().expect("Requires blockstore"),
                datastore.take().expect("Requires datastore"),
                lock.take()
                    .expect("Requires lockfile for data and block store"),
            ),
        }
    }

    pub fn new_raw(
        block_store: Box<dyn BlockStore>,
        data_store: Box<dyn DataStore>,
        lockfile: Box<dyn Lock>,
    ) -> Self {
        let inner = RepoInner {
            initialized: AtomicBool::default(),
            online: AtomicBool::default(),
            block_store,
            data_store,
            events: Default::default(),
            subscriptions: Default::default(),
            lockfile,
            max_storage_size: Default::default(),
            gclock: Default::default(),
        };
        Repo {
            inner: Arc::new(inner),
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn new_fs(path: impl AsRef<Path>) -> Self {
        let path = path.as_ref().to_path_buf();
        let mut blockstore_path = path.clone();
        let mut datastore_path = path.clone();
        let mut lockfile_path = path;
        blockstore_path.push("blockstore");
        datastore_path.push("datastore");
        lockfile_path.push("repo_lock");

        let block_store = Box::new(blockstore::flatfs::FsBlockStore::new(blockstore_path));
        #[cfg(not(any(feature = "sled_data_store", feature = "redb_data_store")))]
        let data_store = Box::new(datastore::flatfs::FsDataStore::new(datastore_path));
        #[cfg(feature = "sled_data_store")]
        let data_store = Box::new(datastore::sled::SledDataStore::new(datastore_path));
        #[cfg(feature = "redb_data_store")]
        let data_store = Box::new(datastore::redb::RedbDataStore::new(datastore_path));
        let lockfile = Box::new(lock::FsLock::new(lockfile_path));
        Self::new_raw(block_store, data_store, lockfile)
    }

    pub fn new_memory() -> Self {
        let block_store = Box::new(blockstore::memory::MemBlockStore::new(Default::default()));
        let data_store = Box::new(datastore::memory::MemDataStore::new(Default::default()));
        let lockfile = Box::new(lock::MemLock);
        Self::new_raw(block_store, data_store, lockfile)
    }

    #[cfg(target_arch = "wasm32")]
    pub fn new_idb(namespace: Option<String>) -> Self {
        let block_store = Box::new(blockstore::idb::IdbBlockStore::new(namespace.clone()));
        let data_store = Box::new(datastore::idb::IdbDataStore::new(namespace));
        let lockfile = Box::new(lock::MemLock);
        Self::new_raw(block_store, data_store, lockfile)
    }

    pub fn set_max_storage_size(&self, size: usize) {
        self.inner.max_storage_size.store(size, Ordering::SeqCst);
    }

    pub fn max_storage_size(&self) -> usize {
        self.inner.max_storage_size.load(Ordering::SeqCst)
    }

    pub async fn migrate(&self, repo: &Self) -> Result<(), Error> {
        if self.is_online() || repo.is_online() {
            anyhow::bail!("Repository cannot be online");
        }
        let block_migration = {
            async move {
                let mut stream = self.list_blocks().await;
                while let Some(cid) = stream.next().await {
                    match self.get_block_now(&cid).await {
                        Ok(Some(block)) => match repo.inner.block_store.put(block).await {
                            Ok(_) => {}
                            Err(e) => error!("Error migrating {cid}: {e}"),
                        },
                        Ok(None) => error!("{cid} doesnt exist"),
                        Err(e) => error!("Error getting block {cid}: {e}"),
                    }
                }
            }
        };

        let data_migration = {
            async move {
                let mut data_stream = self.data_store().iter().await;
                while let Some((k, v)) = data_stream.next().await {
                    if let Err(e) = repo.data_store().put(&k, &v).await {
                        error!("Unable to migrate {k:?} into repo: {e}");
                    }
                }
            }
        };

        let pins_migration = {
            async move {
                let mut stream = self.data_store().list(None).await;
                while let Some(Ok((cid, pin_mode))) = stream.next().await {
                    match pin_mode {
                        PinMode::Direct => match repo.data_store().insert_direct_pin(&cid).await {
                            Ok(_) => {}
                            Err(e) => error!("Unable to migrate pin {cid}: {e}"),
                        },
                        PinMode::Indirect => {
                            //No need to track since we will be obtaining the reference from the pin that is recursive
                            continue;
                        }
                        PinMode::Recursive => {
                            let block = match self.get_block_now(&cid).await.map(|block| {
                                block.and_then(|block| block.decode::<IpldCodec, Ipld>().ok())
                            }) {
                                Ok(Some(block)) => block,
                                Ok(None) => continue,
                                Err(e) => {
                                    error!("Block {cid} does not exist but is pinned: {e}");
                                    continue;
                                }
                            };

                            let st = crate::refs::IpldRefs::default()
                                .with_only_unique()
                                .refs_of_resolved(self, vec![(cid, block.clone())].into_iter())
                                .map_ok(|crate::refs::Edge { destination, .. }| destination)
                                .into_stream()
                                .boxed();

                            if let Err(e) = repo.insert_recursive_pin(&cid, st).await {
                                error!("Error migrating pin {cid}: {e}");
                                continue;
                            }
                        }
                    }
                }
            }
        };

        futures::join!(block_migration, data_migration, pins_migration);

        Ok(())
    }

    pub(crate) fn initialize_channel(&self) -> Receiver<RepoEvent> {
        let mut event_guard = self.inner.events.write();
        let (sender, receiver) = channel(1);
        debug_assert!(event_guard.is_none());
        *event_guard = Some(sender);
        self.set_online();
        receiver
    }

    /// Shutdowns the repo, cancelling any pending subscriptions; Likely going away after some
    /// refactoring, see notes on [`crate::Ipfs::exit_daemon`].
    pub fn shutdown(&self) {
        let mut map = self.inner.subscriptions.lock();
        map.clear();
        drop(map);
        if let Some(mut event) = self.inner.events.write().take() {
            event.close_channel()
        }
        self.set_offline();
    }

    pub fn is_online(&self) -> bool {
        self.inner.online.load(Ordering::SeqCst)
    }

    pub(crate) fn set_online(&self) {
        if self.is_online() {
            return;
        }

        self.inner.online.store(true, Ordering::SeqCst)
    }

    pub(crate) fn set_offline(&self) {
        if !self.is_online() {
            return;
        }

        self.inner.online.store(false, Ordering::SeqCst)
    }

    fn repo_channel(&self) -> Option<Sender<RepoEvent>> {
        self.inner.events.read().clone()
    }

    pub async fn init(&self) -> Result<(), Error> {
        //Avoid initializing again
        if self.inner.initialized.load(Ordering::SeqCst) {
            return Ok(());
        }
        // Dropping the guard (even though not strictly necessary to compile) to avoid potential
        // deadlocks if `block_store` or `data_store` were to try to access `Repo.lockfile`.
        {
            log::debug!("Trying lockfile");
            self.inner.lockfile.try_exclusive()?;
            log::debug!("lockfile tried");
        }

        let f1 = self.inner.block_store.init();
        let f2 = self.inner.data_store.init();
        let (r1, r2) = futures::future::join(f1, f2).await;
        let init = &self.inner.initialized;
        if r1.is_err() {
            r1.map(|_| {
                init.store(true, Ordering::SeqCst);
            })
        } else {
            r2.map(|_| {
                init.store(true, Ordering::SeqCst);
            })
        }
    }

    pub async fn open(&self) -> Result<(), Error> {
        let f1 = self.inner.block_store.open();
        let f2 = self.inner.data_store.open();
        let (r1, r2) = futures::future::join(f1, f2).await;
        if r1.is_err() {
            r1
        } else {
            r2
        }
    }

    /// Puts a block into the block store.
    pub fn put_block(&self, block: Block) -> RepoPutBlock {
        RepoPutBlock::new(self, block).broadcast_on_new_block(true)
    }

    /// Retrives a block from the block store, or starts fetching it from the network and awaits
    /// until it has been fetched.
    #[inline]
    pub async fn get_block(
        &self,
        cid: &Cid,
        peers: &[PeerId],
        local_only: bool,
    ) -> Result<Block, Error> {
        self.get_block_with_session(None, cid, peers, local_only, None)
            .await
    }

    /// Retrives a set of blocks from the block store, or starts fetching them from the network and awaits
    /// until it has been fetched.
    #[inline]
    pub async fn get_blocks(
        &self,
        cids: &[Cid],
        peers: &[PeerId],
        local_only: bool,
    ) -> Result<BoxStream<'static, Result<Block, Error>>, Error> {
        self.get_blocks_with_session(None, cids, peers, local_only, None)
            .await
    }

    /// Get the size of listed blocks
    #[inline]
    pub async fn get_blocks_size(&self, cids: &[Cid]) -> Result<Option<usize>, Error> {
        self.inner.block_store.size(cids).await
    }

    /// Get the total size of the block store
    #[inline]
    pub async fn get_total_size(&self) -> Result<usize, Error> {
        self.inner.block_store.total_size().await
    }

    pub(crate) async fn get_blocks_with_session(
        &self,
        session: impl Into<Option<u64>>,
        cids: &[Cid],
        peers: &[PeerId],
        local_only: bool,
        timeout: impl Into<Option<Duration>>,
    ) -> Result<BoxStream<'static, Result<Block, Error>>, Error> {
        let timeout = timeout.into();
        let _guard = self.inner.gclock.read().await;
        let mut blocks = FuturesOrdered::new();
        let mut missing = cids.to_vec();
        for cid in cids {
            match self.get_block_now(cid).await {
                Ok(Some(block)) => {
                    blocks.push_back(async { Ok(block) }.boxed());
                    if let Some(index) = missing.iter().position(|c| c == cid) {
                        missing.remove(index);
                    }
                }
                Ok(None) | Err(_) => {}
            }
        }

        if missing.is_empty() {
            return Ok(blocks.boxed());
        }

        if local_only || !self.is_online() {
            anyhow::bail!("Unable to locate missing blocks {missing:?}");
        }

        // sending only fails if no one is listening anymore
        // and that is okay with us.

        let mut events = self
            .repo_channel()
            .ok_or(anyhow::anyhow!("Channel is not available"))?;

        for cid in &missing {
            let cid = *cid;
            let (tx, rx) = futures::channel::oneshot::channel();
            self.inner
                .subscriptions
                .lock()
                .entry(cid)
                .or_default()
                .push(tx);

            let timeout = timeout.unwrap_or(Duration::from_secs(60));
            let mut events = events.clone();
            let task = async move {
                let block = rx
                    .timeout(timeout)
                    .await
                    .map_err(|_| anyhow::anyhow!("Timeout while resolving {cid}"))??
                    .map_err(|e| anyhow!("{e}"))?;
                Ok::<_, anyhow::Error>(block)
            }
            .map_err(move |e| {
                _ = events.try_send(RepoEvent::UnwantBlock(cid));
                e
            })
            .boxed();
            blocks.push_back(task);
        }

        events
            .send(RepoEvent::WantBlock(
                session.into(),
                missing,
                peers.to_vec(),
            ))
            .await
            .ok();

        Ok(blocks.boxed())
    }

    pub(crate) async fn get_block_with_session(
        &self,
        session: impl Into<Option<u64>>,
        cid: &Cid,
        peers: &[PeerId],
        local_only: bool,
        timeout: impl Into<Option<Duration>>,
    ) -> Result<Block, Error> {
        let cids = vec![*cid];
        let mut blocks = self
            .get_blocks_with_session(session, &cids, peers, local_only, timeout)
            .await?;

        blocks
            .next()
            .await
            .ok_or(anyhow::anyhow!("Unable to locate {} block", *cid))?
    }

    /// Retrieves a block from the block store if it's available locally.
    pub async fn get_block_now(&self, cid: &Cid) -> Result<Option<Block>, Error> {
        self.inner.block_store.get(cid).await
    }

    /// Check to determine if blockstore contain a block
    pub async fn contains(&self, cid: &Cid) -> Result<bool, Error> {
        self.inner.block_store.contains(cid).await
    }

    /// Lists the blocks in the blockstore.
    pub async fn list_blocks(&self) -> BoxStream<'static, Cid> {
        self.inner.block_store.list().await
    }

    /// Remove block from the block store.
    pub async fn remove_block(&self, cid: &Cid, recursive: bool) -> Result<Vec<Cid>, Error> {
        let _guard = self.inner.gclock.read().await;

        if self.is_pinned(cid).await? {
            return Err(anyhow::anyhow!("block to remove is pinned"));
        }

        let list = match recursive {
            true => {
                let mut list = self.recursive_collections(*cid).await;
                // ensure the first root block is apart of the list
                list.insert(*cid);
                list
            }
            false => BTreeSet::from_iter(std::iter::once(*cid)),
        };

        let list = stream::iter(
            FuturesOrdered::from_iter(list.into_iter().map(|cid| async move { cid }))
                .filter_map(|cid| async move {
                    (!self.is_pinned(&cid).await.unwrap_or_default()).then_some(cid)
                })
                .collect::<Vec<Cid>>()
                .await,
        )
        .boxed();

        let removed = self
            .inner
            .block_store
            .remove_many(list)
            .await
            .collect::<Vec<_>>()
            .await;

        for cid in &removed {
            // notify ipfs task about the removed blocks
            if let Some(mut events) = self.repo_channel() {
                let _ = events.send(RepoEvent::RemovedBlock(*cid)).await;
            }
        }
        Ok(removed)
    }

    fn recursive_collections(&self, cid: Cid) -> BoxFuture<'_, BTreeSet<Cid>> {
        async move {
            let block = match self.get_block_now(&cid).await {
                Ok(Some(block)) => block,
                _ => return BTreeSet::default(),
            };

            let mut references: BTreeSet<Cid> = BTreeSet::new();
            if block.references(&mut references).is_err() {
                return BTreeSet::default();
            }

            let mut list = BTreeSet::new();

            for cid in &references {
                let mut inner_list = self.recursive_collections(*cid).await;
                list.append(&mut inner_list);
            }

            references.append(&mut list);
            references
        }
        .boxed()
    }

    /// Pins a given Cid recursively or directly (non-recursively).
    ///
    /// Pins on a block are additive in sense that a previously directly (non-recursively) pinned
    /// can be made recursive, but removing the recursive pin on the block removes also the direct
    /// pin as well.
    ///
    /// Pinning a Cid recursively (for supported dag-protobuf and dag-cbor) will walk its
    /// references and pin the references indirectly. When a Cid is pinned indirectly it will keep
    /// its previous direct or recursive pin and be indirect in addition.
    ///
    /// Recursively pinned Cids cannot be re-pinned non-recursively but non-recursively pinned Cids
    /// can be "upgraded to" being recursively pinned.
    pub fn pin(&self, cid: &Cid) -> RepoInsertPin {
        RepoInsertPin::new(self.clone(), *cid)
    }

    /// Unpins a given Cid recursively or only directly.
    ///
    /// Recursively unpinning a previously only directly pinned Cid will remove the direct pin.
    ///
    /// Unpinning an indirectly pinned Cid is not possible other than through its recursively
    /// pinned tree roots.
    pub fn remove_pin(&self, cid: &Cid) -> RepoRemovePin {
        RepoRemovePin::new(self.clone(), *cid)
    }

    pub fn fetch(&self, cid: &Cid) -> RepoFetch {
        RepoFetch::new(self.clone(), *cid)
    }

    /// Pins a given Cid recursively or directly (non-recursively).
    pub(crate) async fn insert_pin(
        &self,
        cid: &Cid,
        recursive: bool,
        local_only: bool,
    ) -> Result<(), Error> {
        let mut pin_fut = self.pin(cid);
        if recursive {
            pin_fut = pin_fut.recursive();
        }
        if local_only {
            pin_fut = pin_fut.local();
        }
        pin_fut.await
    }

    /// Inserts a direct pin for a `Cid`.
    pub(crate) async fn insert_direct_pin(&self, cid: &Cid) -> Result<(), Error> {
        self.inner.data_store.insert_direct_pin(cid).await
    }

    /// Inserts a recursive pin for a `Cid`.
    pub(crate) async fn insert_recursive_pin(
        &self,
        cid: &Cid,
        refs: References<'_>,
    ) -> Result<(), Error> {
        self.inner.data_store.insert_recursive_pin(cid, refs).await
    }

    /// Removes a direct pin for a `Cid`.
    pub(crate) async fn remove_direct_pin(&self, cid: &Cid) -> Result<(), Error> {
        self.inner.data_store.remove_direct_pin(cid).await
    }

    /// Removes a recursive pin for a `Cid`.
    pub(crate) async fn remove_recursive_pin(
        &self,
        cid: &Cid,
        refs: References<'_>,
    ) -> Result<(), Error> {
        // FIXME: not really sure why is there not an easier way to to transfer control
        self.inner.data_store.remove_recursive_pin(cid, refs).await
    }

    /// Function to perform a basic cleanup of unpinned blocks
    pub(crate) async fn cleanup(&self) -> Result<Vec<Cid>, Error> {
        let repo = self.clone();

        let blocks = repo.list_blocks().await;
        let pins = repo
            .list_pins(None)
            .await
            .filter_map(|result| futures::future::ready(result.map(|(cid, _)| cid).ok()))
            .collect::<Vec<_>>()
            .await;

        let stream = async_stream::stream! {
            for await cid in blocks {
                if pins.contains(&cid) {
                    continue;
                }
                yield cid;
            }
        }
        .boxed();

        let removed_blocks = self
            .inner
            .block_store
            .remove_many(stream)
            .await
            .collect::<Vec<_>>()
            .await;

        Ok(removed_blocks)
    }

    /// Checks if a `Cid` is pinned.
    pub async fn is_pinned(&self, cid: &Cid) -> Result<bool, Error> {
        self.inner.data_store.is_pinned(cid).await
    }

    pub async fn list_pins(
        &self,
        mode: impl Into<Option<PinMode>>,
    ) -> futures::stream::BoxStream<'static, Result<(Cid, PinMode), Error>> {
        let mode = mode.into();
        self.inner.data_store.list(mode).await
    }

    pub async fn query_pins(
        &self,
        cids: Vec<Cid>,
        requirement: impl Into<Option<PinMode>>,
    ) -> Result<Vec<(Cid, PinKind<Cid>)>, Error> {
        let requirement = requirement.into();
        self.inner.data_store.query(cids, requirement).await
    }
}

pub struct GCGuard<'a> {
    _g: RwLockReadGuard<'a, ()>,
}

impl Repo {
    /// Hold a guard to prevent GC from running until this guard has dropped
    /// Note: Until this guard drops, the GC task, if enabled, would not perform any cleanup.
    ///       If the GC task is running, this guard will await until GC finishes
    pub async fn gc_guard(&self) -> GCGuard {
        let _g = self.inner.gclock.read().await;
        GCGuard { _g }
    }

    pub fn data_store(&self) -> &dyn DataStore {
        &*self.inner.data_store
    }
}

pub struct RepoPutBlock {
    repo: Repo,
    block: Block,
    span: Option<Span>,
    broadcast_on_new_block: bool,
}

impl RepoPutBlock {
    fn new(repo: &Repo, block: Block) -> Self {
        Self {
            repo: repo.clone(),
            block,
            span: None,
            broadcast_on_new_block: true,
        }
    }

    pub fn broadcast_on_new_block(mut self, v: bool) -> Self {
        self.broadcast_on_new_block = v;
        self
    }

    pub fn span(mut self, span: Span) -> Self {
        self.span = Some(span);
        self
    }
}

impl IntoFuture for RepoPutBlock {
    type IntoFuture = BoxFuture<'static, Self::Output>;
    type Output = Result<Cid, Error>;
    fn into_future(self) -> Self::IntoFuture {
        let block = self.block;
        let span = self.span.unwrap_or(Span::current());
        let span = debug_span!(parent: &span, "put_block", cid = %block.cid());
        async move {
            let _guard = self.repo.inner.gclock.read().await;
            let (cid, res) = self.repo.inner.block_store.put(block.clone()).await?;

            if let BlockPut::NewBlock = res {
                if self.broadcast_on_new_block {
                    if let Some(mut event) = self.repo.repo_channel() {
                        _ = event.send(RepoEvent::NewBlock(block.clone())).await;
                    }
                }
                let list = self.repo.inner.subscriptions.lock().remove(&cid);
                if let Some(mut list) = list {
                    for ch in list.drain(..) {
                        let block = block.clone();
                        let _ = ch.send(Ok(block));
                    }
                }
            }

            Ok(cid)
        }
        .instrument(span)
        .boxed()
    }
}

pub struct RepoFetch {
    repo: Repo,
    cid: Cid,
    span: Option<Span>,
    providers: Vec<PeerId>,
    recursive: bool,
    timeout: Option<Duration>,
    refs: crate::refs::IpldRefs,
}

impl RepoFetch {
    pub fn new(repo: Repo, cid: Cid) -> Self {
        Self {
            repo,
            cid,
            recursive: false,
            providers: vec![],
            timeout: None,
            refs: Default::default(),
            span: None,
        }
    }

    /// Fetch blocks recursively
    pub fn recursive(mut self) -> Self {
        self.recursive = true;
        self
    }

    /// Peer that may contain the block
    pub fn provider(mut self, peer_id: PeerId) -> Self {
        if !self.providers.contains(&peer_id) {
            self.providers.push(peer_id);
        }
        self
    }

    /// List of peers that may contain the block
    pub fn providers(mut self, providers: &[PeerId]) -> Self {
        self.providers = providers.to_vec();
        self
    }

    /// Fetch blocks to a specific depth
    pub fn depth(mut self, depth: u64) -> Self {
        self.refs = self.refs.with_max_depth(depth);
        self
    }

    /// Duration to fetch the block from the network before
    /// timing out
    pub fn timeout(mut self, duration: Duration) -> Self {
        self.timeout.replace(duration);
        self.refs = self.refs.with_timeout(duration);
        self
    }

    pub fn exit_on_error(mut self) -> Self {
        self.refs = self.refs.with_exit_on_error();
        self
    }

    /// Set tracing span
    pub fn span(mut self, span: Span) -> Self {
        self.span = Some(span);
        self
    }
}

impl std::future::IntoFuture for RepoFetch {
    type Output = Result<(), anyhow::Error>;

    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let cid = self.cid;
        let span = self.span.unwrap_or(Span::current());
        let recursive = self.recursive;
        let repo = self.repo;
        let span = debug_span!(parent: &span, "fetch", cid = %cid, recursive);
        let providers = self.providers;
        let timeout = self.timeout;
        async move {
            // Although getting a block adds a guard, we will add a read guard here a head of time so we can hold it throughout this future
            let _g = repo.inner.gclock.read().await;
            let block = repo
                .get_block_with_session(None, &cid, &providers, false, timeout)
                .await?;

            if !recursive {
                return Ok(());
            }
            let ipld = block.decode::<IpldCodec, Ipld>()?;

            let mut st = self
                .refs
                .with_only_unique()
                .providers(&providers)
                .refs_of_resolved(&repo, vec![(cid, ipld.clone())])
                .map_ok(|crate::refs::Edge { destination, .. }| destination)
                .into_stream()
                .boxed();

            while let Some(_c) = st.try_next().await? {}

            Ok(())
        }
        .instrument(span)
        .boxed()
    }
}

pub struct RepoInsertPin {
    repo: Repo,
    cid: Cid,
    span: Option<Span>,
    providers: Vec<PeerId>,
    recursive: bool,
    timeout: Option<Duration>,
    local: bool,
    refs: crate::refs::IpldRefs,
}

impl RepoInsertPin {
    pub fn new(repo: Repo, cid: Cid) -> Self {
        Self {
            repo,
            cid,
            recursive: false,
            providers: vec![],
            local: false,
            timeout: None,
            refs: Default::default(),
            span: None,
        }
    }

    /// Recursively pin blocks
    pub fn recursive(mut self) -> Self {
        self.recursive = true;
        self
    }

    /// Pin local blocks only
    pub fn local(mut self) -> Self {
        self.local = true;
        self.refs = self.refs.with_existing_blocks();
        self
    }

    /// Set a flag to pin local blocks only
    pub fn set_local(mut self, local: bool) -> Self {
        self.local = local;
        if local {
            self.refs = self.refs.with_existing_blocks();
        }
        self
    }

    /// Peer that may contain the block to pin
    pub fn provider(mut self, peer_id: PeerId) -> Self {
        if !self.providers.contains(&peer_id) {
            self.providers.push(peer_id);
        }
        self
    }

    /// List of peers that may contain the block to pin
    pub fn providers(mut self, providers: &[PeerId]) -> Self {
        self.providers = providers.into();
        self
    }

    /// Pin to a specific depth of the graph
    pub fn depth(mut self, depth: u64) -> Self {
        self.refs = self.refs.with_max_depth(depth);
        self
    }

    /// Duration to fetch the block from the network before
    /// timing out
    pub fn timeout(mut self, duration: Duration) -> Self {
        self.timeout.replace(duration);
        self.refs = self.refs.with_timeout(duration);
        self
    }

    pub fn exit_on_error(mut self) -> Self {
        self.refs = self.refs.with_exit_on_error();
        self
    }

    /// Set tracing span
    pub fn span(mut self, span: Span) -> Self {
        self.span = Some(span);
        self
    }
}

impl std::future::IntoFuture for RepoInsertPin {
    type Output = Result<(), anyhow::Error>;

    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let cid = self.cid;
        let local = self.local;
        let span = self.span.unwrap_or(Span::current());
        let recursive = self.recursive;
        let repo = self.repo;
        let span = debug_span!(parent: &span, "insert_pin", cid = %cid, recursive);
        let providers = self.providers;
        let timeout = self.timeout;
        async move {
            // Although getting a block adds a guard, we will add a read guard here a head of time so we can hold it throughout this future
            let _g = repo.inner.gclock.read().await;
            let block = repo
                .get_block_with_session(None, &cid, &providers, local, timeout)
                .await?;

            if !recursive {
                repo.insert_direct_pin(&cid).await?
            } else {
                let ipld = block.decode::<IpldCodec, Ipld>()?;

                let st = self
                    .refs
                    .with_only_unique()
                    .providers(&providers)
                    .refs_of_resolved(&repo, vec![(cid, ipld.clone())])
                    .map_ok(|crate::refs::Edge { destination, .. }| destination)
                    .into_stream()
                    .boxed();

                repo.insert_recursive_pin(&cid, st).await?
            }
            Ok(())
        }
        .instrument(span)
        .boxed()
    }
}

pub struct RepoRemovePin {
    repo: Repo,
    cid: Cid,
    span: Option<Span>,
    recursive: bool,
    refs: crate::refs::IpldRefs,
}

impl RepoRemovePin {
    pub fn new(repo: Repo, cid: Cid) -> Self {
        Self {
            repo,
            cid,
            recursive: false,
            refs: Default::default(),
            span: None,
        }
    }

    /// Recursively unpin blocks
    pub fn recursive(mut self) -> Self {
        self.recursive = true;
        self
    }

    /// Set tracing span
    pub fn span(mut self, span: Span) -> Self {
        self.span = Some(span);
        self
    }
}

impl std::future::IntoFuture for RepoRemovePin {
    type Output = Result<(), anyhow::Error>;

    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let cid = self.cid;
        let span = self.span.unwrap_or(Span::current());
        let recursive = self.recursive;
        let repo = self.repo;

        let span = debug_span!(parent: &span, "remove_pin", cid = %cid, recursive);
        async move {
            let _g = repo.inner.gclock.read().await;
            if !recursive {
                repo.remove_direct_pin(&cid).await
            } else {
                // start walking refs of the root after loading it

                let block = match repo.get_block_now(&cid).await? {
                    Some(b) => b,
                    None => {
                        return Err(anyhow::anyhow!("pinned root not found: {}", cid));
                    }
                };

                let ipld = block.decode::<IpldCodec, Ipld>()?;
                let st = self
                    .refs
                    .with_only_unique()
                    .with_existing_blocks()
                    .refs_of_resolved(&repo, vec![(cid, ipld.clone())])
                    .map_ok(|crate::refs::Edge { destination, .. }| destination)
                    .into_stream()
                    .boxed();

                repo.remove_recursive_pin(&cid, st).await
            }
        }
        .instrument(span)
        .boxed()
    }
}
