//! Storage implementation(s) backing the [`crate::Ipfs`].
use crate::error::Error;
use crate::p2p::KadResult;
use crate::path::IpfsPath;
use crate::{Block, ReceiverChannel, StoragePath};
use anyhow::anyhow;
use async_trait::async_trait;
use core::fmt::Debug;
use futures::channel::{
    mpsc::{channel, Receiver, Sender},
    oneshot,
};
use futures::sink::SinkExt;
use futures::{StreamExt, TryStreamExt};
use libipld::cid::Cid;
use libipld::{Ipld, IpldCodec};
use libp2p::identity::PeerId;
use parking_lot::{Mutex, RwLock};
use std::borrow::Borrow;
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use std::{error, fmt, io};
use tracing::log;

#[macro_use]
#[cfg(test)]
mod common_tests;

pub mod blockstore;
pub mod datastore;
pub mod lock;

/// Path mangling done for pins and blocks
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
    /// Inserts a block in the blockstore.
    async fn put(&self, block: Block) -> Result<(Cid, BlockPut), Error>;
    /// Removes a block from the blockstore.
    async fn remove(&self, cid: &Cid) -> Result<Result<BlockRm, BlockRmError>, Error>;
    /// Returns a list of the blocks (Cids), in the blockstore.
    async fn list(&self) -> Result<Vec<Cid>, Error>;
    /// Wipes the blockstore.
    async fn wipe(&self) {}
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
    /// Wipes the datastore.
    async fn wipe(&self) {}
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

/// Describes a repo.
///
/// Consolidates a blockstore, a datastore and a subscription registry.

#[allow(clippy::type_complexity)]
#[derive(Debug, Clone)]
pub struct Repo {
    online: Arc<AtomicBool>,
    initialized: Arc<AtomicBool>,
    block_store: Arc<dyn BlockStore>,
    data_store: Arc<dyn DataStore>,
    events: Arc<RwLock<Option<Sender<RepoEvent>>>>,
    pub(crate) subscriptions:
        Arc<Mutex<HashMap<Cid, Vec<futures::channel::oneshot::Sender<Result<Block, String>>>>>>,
    lockfile: Arc<dyn Lock>,
}

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

/// Events used to communicate to the swarm on repo changes.
#[derive(Debug)]
pub enum RepoEvent {
    /// Signals a desired block.
    WantBlock(Option<u64>, Cid, Vec<PeerId>),
    /// Signals a desired block is no longer wanted.
    UnwantBlock(Cid),
    /// Signals the posession of a new block.
    NewBlock(
        Block,
        oneshot::Sender<Result<ReceiverChannel<KadResult>, anyhow::Error>>,
    ),
    /// Signals the removal of a block.
    RemovedBlock(Cid),
}

impl Repo {
    pub fn new(repo_type: StoragePath) -> Self {
        match repo_type {
            StoragePath::Memory => Repo::new_memory(),
            StoragePath::Disk(path) => Repo::new_fs(path),
            StoragePath::Custom {
                blockstore,
                datastore,
                lock,
            } => Repo::new_raw(blockstore, datastore, lock),
        }
    }

    pub fn new_raw(
        block_store: Arc<dyn BlockStore>,
        data_store: Arc<dyn DataStore>,
        lockfile: Arc<dyn Lock>,
    ) -> Self {
        Repo {
            initialized: Arc::default(),
            online: Arc::default(),
            block_store,
            data_store,
            events: Arc::default(),
            subscriptions: Default::default(),
            lockfile,
        }
    }

    pub fn new_fs(path: impl AsRef<Path>) -> Self {
        let path = path.as_ref().to_path_buf();
        let mut blockstore_path = path.clone();
        let mut datastore_path = path.clone();
        let mut lockfile_path = path;
        blockstore_path.push("blockstore");
        datastore_path.push("datastore");
        lockfile_path.push("repo_lock");

        let block_store = Arc::new(blockstore::flatfs::FsBlockStore::new(blockstore_path));
        #[cfg(not(feature = "sled_data_store"))]
        let data_store = Arc::new(datastore::flatfs::FsDataStore::new(datastore_path));
        #[cfg(feature = "sled_data_store")]
        let data_store = Arc::new(datastore::sled::SledDataStore::new(datastore_path));
        let lockfile = Arc::new(lock::FsLock::new(lockfile_path));
        Self::new_raw(block_store, data_store, lockfile)
    }

    pub fn new_memory() -> Self {
        let block_store = Arc::new(blockstore::memory::MemBlockStore::new(Default::default()));
        let data_store = Arc::new(datastore::memory::MemDataStore::new(Default::default()));
        let lockfile = Arc::new(lock::MemLock);
        Self::new_raw(block_store, data_store, lockfile)
    }

    pub async fn migrate(&self, repo: &Self) -> Result<(), Error> {
        if self.is_online() || repo.is_online() {
            anyhow::bail!("Repository cannot be online");
        }
        let block_migration = {
            let this = self.clone();
            let external = repo.clone();
            async move {
                if let Ok(list) = this.list_blocks().await {
                    for cid in list {
                        match this.get_block_now(&cid).await {
                            Ok(Some(block)) => match external.block_store.put(block).await {
                                Ok(_) => {}
                                Err(e) => error!("Error migrating {cid}: {e}"),
                            },
                            Ok(None) => error!("{cid} doesnt exist"),
                            Err(e) => error!("Error getting block {cid}: {e}"),
                        }
                    }
                }
            }
        };

        let data_migration = {
            let this = self.clone();
            let external = repo.clone();
            async move {
                let mut data_stream = this.data_store().iter().await;
                while let Some((k, v)) = data_stream.next().await {
                    if let Err(e) = external.data_store().put(&k, &v).await {
                        error!("Unable to migrate {k:?} into repo: {e}");
                    }
                }
            }
        };

        let pins_migration = {
            let this = self.clone();
            let external = repo.clone();
            async move {
                let mut stream = this.data_store().list(None).await;
                while let Some(Ok((cid, pin_mode))) = stream.next().await {
                    match pin_mode {
                        PinMode::Direct => {
                            match external.data_store().insert_direct_pin(&cid).await {
                                Ok(_) => {}
                                Err(e) => error!("Unable to migrate pin {cid}: {e}"),
                            }
                        }
                        PinMode::Indirect => {
                            //No need to track since we will be obtaining the reference from the pin that is recursive
                            continue;
                        }
                        PinMode::Recursive => {
                            let block = match this.get_block_now(&cid).await.map(|block| {
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

                            if let Err(e) = external.insert_recursive_pin(&cid, st).await {
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
        let mut event_guard = self.events.write();
        let (sender, receiver) = channel(1);
        debug_assert!(event_guard.is_none());
        *event_guard = Some(sender);
        self.set_online();
        receiver
    }

    /// Shutdowns the repo, cancelling any pending subscriptions; Likely going away after some
    /// refactoring, see notes on [`crate::Ipfs::exit_daemon`].
    pub fn shutdown(&self) {
        let mut map = self.subscriptions.lock();
        map.clear();
        drop(map);
        if let Some(mut event) = self.events.write().take() {
            event.close_channel()
        }
        self.set_offline();
    }

    pub fn is_online(&self) -> bool {
        self.online.load(Ordering::SeqCst)
    }

    pub(crate) fn set_online(&self) {
        if self.is_online() {
            return;
        }

        self.online.store(true, Ordering::SeqCst)
    }

    pub(crate) fn set_offline(&self) {
        if !self.is_online() {
            return;
        }

        self.online.store(false, Ordering::SeqCst)
    }

    fn repo_channel(&self) -> Option<Sender<RepoEvent>> {
        self.events.read().clone()
    }

    pub async fn init(&self) -> Result<(), Error> {
        //Avoid initializing again
        if self.initialized.load(Ordering::SeqCst) {
            return Ok(());
        }
        // Dropping the guard (even though not strictly necessary to compile) to avoid potential
        // deadlocks if `block_store` or `data_store` were to try to access `Repo.lockfile`.
        {
            log::debug!("Trying lockfile");
            self.lockfile.try_exclusive()?;
            log::debug!("lockfile tried");
        }

        let f1 = self.block_store.init();
        let f2 = self.data_store.init();
        let (r1, r2) = futures::future::join(f1, f2).await;
        let init = self.initialized.clone();
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
        let f1 = self.block_store.open();
        let f2 = self.data_store.open();
        let (r1, r2) = futures::future::join(f1, f2).await;
        if r1.is_err() {
            r1
        } else {
            r2
        }
    }

    /// Puts a block into the block store.
    pub async fn put_block(&self, block: Block) -> Result<(Cid, BlockPut), Error> {
        let (cid, res) = self.block_store.put(block.clone()).await?;

        if let BlockPut::NewBlock = res {
            let list = self.subscriptions.lock().remove(&cid);
            if let Some(mut list) = list {
                for ch in list.drain(..) {
                    let block = block.clone();
                    tokio::spawn(async move {
                        let _ = ch.send(Ok(block));
                    });
                }
            }
        }

        Ok((cid, res))
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

    pub(crate) async fn get_block_with_session(
        &self,
        session: Option<u64>,
        cid: &Cid,
        peers: &[PeerId],
        local_only: bool,
        timeout: Option<Duration>,
    ) -> Result<Block, Error> {
        if let Some(block) = self.get_block_now(cid).await? {
            Ok(block)
        } else {
            if local_only || !self.is_online() {
                anyhow::bail!("Unable to locate block {cid}");
            }

            let (tx, rx) = futures::channel::oneshot::channel();

            self.subscriptions.lock().entry(*cid).or_default().push(tx);

            // sending only fails if no one is listening anymore
            // and that is okay with us.

            let mut events = self
                .repo_channel()
                .ok_or(anyhow::anyhow!("Channel is not available"))?;

            events
                .send(RepoEvent::WantBlock(session, *cid, peers.to_vec()))
                .await
                .ok();

            let timeout = timeout.unwrap_or(Duration::from_secs(60));
            tokio::time::timeout(timeout, rx)
                .await??
                .map_err(|e| anyhow!("{e}"))
        }
    }

    /// Retrieves a block from the block store if it's available locally.
    pub async fn get_block_now(&self, cid: &Cid) -> Result<Option<Block>, Error> {
        self.block_store.get(cid).await
    }

    /// Check to determine if blockstore contain a block
    pub async fn contains(&self, cid: &Cid) -> Result<bool, Error> {
        self.block_store.contains(cid).await
    }

    /// Lists the blocks in the blockstore.
    pub async fn list_blocks(&self) -> Result<Vec<Cid>, Error> {
        self.block_store.list().await
    }

    /// Remove block from the block store.
    pub async fn remove_block(&self, cid: &Cid) -> Result<Cid, Error> {
        if self.is_pinned(cid).await? {
            return Err(anyhow::anyhow!("block to remove is pinned"));
        }

        // FIXME: Need to change location of pinning logic.
        // I like this pattern of the repo abstraction being some sort of
        // "clearing house" for the underlying result enums, but this
        // could potentially be pushed out out of here up to Ipfs, idk
        match self.block_store.remove(cid).await? {
            Ok(success) => match success {
                BlockRm::Removed(_cid) => {
                    // sending only fails if the background task has exited
                    if let Some(mut events) = self.repo_channel() {
                        events.send(RepoEvent::RemovedBlock(*cid)).await.ok();
                    }
                    Ok(*cid)
                }
            },
            Err(err) => match err {
                BlockRmError::NotFound(_cid) => Err(anyhow::anyhow!("block not found")),
            },
        }
    }

    /// Get an ipld path from the datastore.
    pub async fn get_ipns(&self, ipns: &PeerId) -> Result<Option<IpfsPath>, Error> {
        use std::str::FromStr;

        let data_store = &self.data_store;
        let key = ipns.to_owned();
        // FIXME: needless vec<u8> creation
        let key = format!("ipns/{key}");
        let bytes = data_store.get(key.as_bytes()).await?;
        match bytes {
            Some(ref bytes) => {
                let string = String::from_utf8_lossy(bytes);
                let path = IpfsPath::from_str(&string)?;
                Ok(Some(path))
            }
            None => Ok(None),
        }
    }

    /// Put an ipld path into the datastore.
    pub async fn put_ipns(&self, ipns: &PeerId, path: &IpfsPath) -> Result<(), Error> {
        let string = path.to_string();
        let value = string.as_bytes();
        // FIXME: needless vec<u8> creation
        let key = format!("ipns/{ipns}");
        self.data_store.put(key.as_bytes(), value).await
    }

    /// Remove an ipld path from the datastore.
    pub async fn remove_ipns(&self, ipns: &PeerId) -> Result<(), Error> {
        // FIXME: us needing to clone the peerid is wasteful to pass it as a reference only to be
        // cloned again
        let key = format!("ipns/{ipns}");
        self.data_store.remove(key.as_bytes()).await
    }

    /// Pins a given Cid recursively or directly (non-recursively).
    pub async fn insert_pin(
        &self,
        cid: &Cid,
        recursive: bool,
        local_only: bool,
    ) -> Result<(), Error> {
        // Can download if `local_only` is false
        let block = self.get_block(cid, &[], local_only).await?;

        if !recursive {
            self.insert_direct_pin(cid).await?
        } else {
            let ipld = block.decode::<IpldCodec, Ipld>()?;

            let st = crate::refs::IpldRefs::default()
                .with_only_unique()
                .refs_of_resolved(self, vec![(*cid, ipld.clone())].into_iter())
                .map_ok(|crate::refs::Edge { destination, .. }| destination)
                .into_stream()
                .boxed();

            self.insert_recursive_pin(cid, st).await?
        }
        Ok(())
    }

    /// Inserts a direct pin for a `Cid`.
    pub async fn insert_direct_pin(&self, cid: &Cid) -> Result<(), Error> {
        self.data_store.insert_direct_pin(cid).await
    }

    /// Inserts a recursive pin for a `Cid`.
    pub async fn insert_recursive_pin(&self, cid: &Cid, refs: References<'_>) -> Result<(), Error> {
        self.data_store.insert_recursive_pin(cid, refs).await
    }

    /// Removes a direct pin for a `Cid`.
    pub async fn remove_direct_pin(&self, cid: &Cid) -> Result<(), Error> {
        self.data_store.remove_direct_pin(cid).await
    }

    /// Removes a recursive pin for a `Cid`.
    pub async fn remove_recursive_pin(&self, cid: &Cid, refs: References<'_>) -> Result<(), Error> {
        // FIXME: not really sure why is there not an easier way to to transfer control
        self.data_store.remove_recursive_pin(cid, refs).await
    }

    /// Function to perform a basic cleanup of unpinned blocks
    pub async fn cleanup(&self) -> Result<Vec<Cid>, Error> {
        let mut removed_blocks = vec![];
        let blocks = self.list_blocks().await?;
        for cid in blocks {
            if !self.is_pinned(&cid).await? {
                if let Ok(cid) = self.remove_block(&cid).await {
                    removed_blocks.push(cid);
                }
            }
        }
        Ok(removed_blocks)
    }

    /// Checks if a `Cid` is pinned.
    pub async fn is_pinned(&self, cid: &Cid) -> Result<bool, Error> {
        self.data_store.is_pinned(cid).await
    }

    pub async fn list_pins(
        &self,
        mode: Option<PinMode>,
    ) -> futures::stream::BoxStream<'static, Result<(Cid, PinMode), Error>> {
        self.data_store.list(mode).await
    }

    pub async fn query_pins(
        &self,
        cids: Vec<Cid>,
        requirement: Option<PinMode>,
    ) -> Result<Vec<(Cid, PinKind<Cid>)>, Error> {
        self.data_store.query(cids, requirement).await
    }
}

impl Repo {
    pub fn data_store(&self) -> &dyn DataStore {
        &*self.data_store
    }
}
