mod default;

use crate::Block;
use anyhow::Error;
use async_trait::async_trait;
use futures::stream::BoxStream;
use ipld_core::cid::Cid;
use std::borrow::Borrow;
use std::fmt::Debug;
use std::{error, fmt, io};

pub trait RepoStorage: BlockStore + DataStore + PinStore + Debug + Send + Sync {}

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
pub trait BlockStore: Debug + Send + Sync {
    async fn init(&self) -> Result<(), Error>;

    #[deprecated]
    async fn open(&self) -> Result<(), Error> {
        Ok(())
    }
    /// Returns whether a block is present in the blockstore.
    async fn contains(&self, cid: &Cid) -> Result<bool, Error>;
    /// Returns a block from the blockstore.
    async fn get(&self, cid: &Cid) -> Result<Option<Block>, Error>;
    /// Get the size of a single block
    async fn size(&self, cid: &[Cid]) -> Result<Option<usize>, Error>;
    /// Get a total size of the block store
    async fn total_size(&self) -> Result<usize, Error>;
    /// Inserts a block in the blockstore.
    async fn put(&self, block: &Block) -> Result<(Cid, BlockPut), Error>;
    /// Removes a block from the blockstore.
    async fn remove(&self, cid: &Cid) -> Result<(), Error>;
    /// Remove multiple blocks from the blockstore
    async fn remove_many(&self, blocks: BoxStream<'static, Cid>) -> BoxStream<'static, Cid>;
    /// Returns a list of the blocks (Cids), in the blockstore.
    async fn list(&self) -> BoxStream<'static, Cid>;
}

#[async_trait]
/// Generic layer of abstraction for a key-value data store.
pub trait DataStore: PinStore + Debug + Send + Sync {
    async fn init(&self) -> Result<(), Error>;
    #[deprecated]
    async fn open(&self) -> Result<(), Error> {
        Ok(())
    }
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

/// A trait for describing repository locking.
///
/// This ensures no two IPFS nodes can be started with the same peer ID, as exclusive access to the
/// repository is guarenteed. This is most useful when using an fs backed repo.
pub trait Lock: Debug + Send + Sync {
    // fn new(path: PathBuf) -> Self;
    fn try_exclusive(&self) -> Result<(), LockError>;
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

pub(crate) type References<'a> = BoxStream<'a, Result<Cid, crate::refs::IpldRefsError>>;

#[async_trait]
pub trait PinStore: Debug + Send + Sync {
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
pub(crate) enum PinModeRequirement {
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
    pub fn is_indirect_or_any(&self) -> bool {
        use PinModeRequirement::*;
        match self {
            Only(PinMode::Indirect) | Any => true,
            Only(_) => false,
        }
    }

    pub fn matches<P: PartialEq<PinMode>>(&self, other: &P) -> bool {
        use PinModeRequirement::*;
        match self {
            Only(one) if other == one => true,
            Only(_) => false,
            Any => true,
        }
    }

    pub fn required(&self) -> Option<PinMode> {
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
    pub fn as_ref(&self) -> PinKind<&'_ Cid> {
        match self {
            PinKind::IndirectFrom(c) => PinKind::IndirectFrom(c.borrow()),
            PinKind::Direct => PinKind::Direct,
            PinKind::Recursive(count) => PinKind::Recursive(*count),
            PinKind::RecursiveIntention => PinKind::RecursiveIntention,
        }
    }
}
