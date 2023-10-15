//! Adaptation for `ipfs-unixfs` crate functionality on top of [`crate::Ipfs`].
//!
//! Adding files and directory structures is supported but not exposed via an API. See examples and
//! `ipfs-http`.

use std::{ops::Range, path::PathBuf, time::Duration};

use anyhow::Error;
use either::Either;
use futures::{stream::BoxStream, Stream};
use libipld::Cid;
use libp2p::PeerId;
use ll::file::FileReadFailed;
pub use rust_unixfs as ll;

mod add;
mod cat;
mod get;
mod ls;
pub use add::{add, add_file, AddOption};
pub use cat::{cat, StartingPoint};
pub use get::get;
pub use ls::{ls, NodeItem};

use crate::{
    dag::{ResolveError, UnexpectedResolved},
    Ipfs, IpfsPath,
};

pub struct IpfsUnixfs {
    ipfs: Ipfs,
}

pub enum AddOpt<'a> {
    Path(PathBuf),
    Stream(BoxStream<'a, std::io::Result<Vec<u8>>>),
    StreamWithName(String, BoxStream<'a, std::io::Result<Vec<u8>>>),
}

impl<'a> From<&'a str> for AddOpt<'a> {
    fn from(value: &'a str) -> Self {
        AddOpt::Path(PathBuf::from(value))
    }
}

impl From<String> for AddOpt<'_> {
    fn from(value: String) -> Self {
        AddOpt::Path(PathBuf::from(value))
    }
}

impl<'a> From<&'a std::path::Path> for AddOpt<'_> {
    fn from(path: &'a std::path::Path) -> Self {
        AddOpt::Path(path.to_path_buf())
    }
}

impl From<PathBuf> for AddOpt<'_> {
    fn from(path: PathBuf) -> Self {
        AddOpt::Path(path)
    }
}

impl<'a> From<BoxStream<'a, std::io::Result<Vec<u8>>>> for AddOpt<'a> {
    fn from(stream: BoxStream<'a, std::io::Result<Vec<u8>>>) -> Self {
        AddOpt::Stream(stream)
    }
}

impl<'a> From<(String, BoxStream<'a, std::io::Result<Vec<u8>>>)> for AddOpt<'a> {
    fn from((name, stream): (String, BoxStream<'a, std::io::Result<Vec<u8>>>)) -> Self {
        AddOpt::StreamWithName(name, stream)
    }
}

impl IpfsUnixfs {
    pub fn new(ipfs: Ipfs) -> Self {
        Self { ipfs }
    }

    /// Creates a stream which will yield the bytes of an UnixFS file from the root Cid, with the
    /// optional file byte range. If the range is specified and is outside of the file, the stream
    /// will end without producing any bytes.
    ///
    /// To create an owned version of the stream, please use `ipfs::unixfs::cat` directly.
    pub async fn cat<'a>(
        &self,
        starting_point: impl Into<StartingPoint>,
        range: Option<Range<u64>>,
        peers: &'a [PeerId],
        local: bool,
        timeout: Option<Duration>,
    ) -> Result<impl Stream<Item = Result<Vec<u8>, TraversalFailed>> + Send + 'a, TraversalFailed>
    {
        // convert early not to worry about the lifetime of parameter
        let starting_point = starting_point.into();
        cat(
            Either::Left(&self.ipfs),
            starting_point,
            range,
            peers,
            local,
            timeout,
        )
        .await
    }

    /// Add a file from either a file or stream
    ///
    /// To create an owned version of the stream, please use `ipfs::unixfs::add` or `ipfs::unixfs::add_file` directly.
    pub async fn add<'a, I: Into<AddOpt<'a>>>(
        &self,
        item: I,
        option: Option<AddOption>,
    ) -> Result<BoxStream<'a, UnixfsStatus>, Error> {
        let item = item.into();
        match item {
            AddOpt::Path(path) => add_file(Either::Left(&self.ipfs), path, option).await,
            AddOpt::Stream(stream) => {
                add(Either::Left(&self.ipfs), None, None, stream, option).await
            }
            AddOpt::StreamWithName(name, stream) => {
                add(Either::Left(&self.ipfs), Some(name), None, stream, option).await
            }
        }
    }

    /// Retreive a file and saving it to a local path.
    ///
    /// To create an owned version of the stream, please use `ipfs::unixfs::get` directly.
    pub async fn get<'a, P: AsRef<std::path::Path>>(
        &self,
        path: IpfsPath,
        dest: P,
        peers: &'a [PeerId],
        local: bool,
        timeout: Option<Duration>,
    ) -> Result<BoxStream<'a, UnixfsStatus>, Error> {
        get(Either::Left(&self.ipfs), path, dest, peers, local, timeout)
            .await
            .map_err(anyhow::Error::from)
    }

    /// List directory contents
    pub async fn ls<'a>(
        &self,
        path: IpfsPath,
        peers: &'a [PeerId],
        local: bool,
        timeout: Option<Duration>,
    ) -> Result<BoxStream<'a, NodeItem>, Error> {
        ls(Either::Left(&self.ipfs), path, peers, local, timeout).await
    }
}

#[derive(Debug)]
pub enum UnixfsStatus {
    ProgressStatus {
        written: usize,
        total_size: Option<usize>,
    },
    CompletedStatus {
        path: IpfsPath,
        written: usize,
        total_size: Option<usize>,
    },
    FailedStatus {
        written: usize,
        total_size: Option<usize>,
        error: Option<anyhow::Error>,
    },
}

/// Types of failures which can occur while walking the UnixFS graph.
#[derive(Debug, thiserror::Error)]
pub enum TraversalFailed {
    /// Failure to resolve the given path; does not happen when given a block.
    #[error("path resolving failed")]
    Resolving(#[source] ResolveError),

    /// The given path was resolved to non dag-pb block, does not happen when starting the walk
    /// from a block.
    #[error("path resolved to unexpected")]
    Path(#[source] UnexpectedResolved),

    /// Loading of a block during walk failed
    #[error("loading of {} failed", .0)]
    Loading(Cid, #[source] Error),

    #[error("Timeout while resolving {path}")]
    Timeout { path: IpfsPath },

    /// Processing of the block failed
    #[error("walk failed on {}", .0)]
    Walking(Cid, #[source] FileReadFailed),

    #[error(transparent)]
    Io(std::io::Error),
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_file_cid() {
        // note: old versions of `ipfs::unixfs::File` was an interface where user would provide the
        // unixfs encoded data. this test case has been migrated to put the "content" as the the
        // file data instead of the unixfs encoding. the previous way used to produce
        // QmSy5pnHk1EnvE5dmJSyFKG5unXLGjPpBuJJCBQkBTvBaW.
        let content = "\u{8}\u{2}\u{12}\u{12}Here is some data\n\u{18}\u{12}";

        let mut adder = rust_unixfs::file::adder::FileAdder::default();
        let (mut blocks, consumed) = adder.push(content.as_bytes());
        assert_eq!(consumed, content.len(), "should had consumed all content");
        assert_eq!(
            blocks.next(),
            None,
            "should not had produced any blocks yet"
        );

        let mut blocks = adder.finish();

        let (cid, _block) = blocks.next().unwrap();
        assert_eq!(blocks.next(), None, "should had been the last");

        assert_eq!(
            "QmQZE72h2Vdm3F5gWr9RLuzSw3rUJEkKedWEa8t8XVygT5",
            cid.to_string(),
            "matches cid from go-ipfs 0.6.0"
        );
    }
}
