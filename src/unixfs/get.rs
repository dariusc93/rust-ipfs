use either::Either;
use futures::stream::BoxStream;
use futures::{FutureExt, Stream, StreamExt, future::BoxFuture, stream::FusedStream};
use libp2p::PeerId;
#[allow(unused_imports)]
use rust_unixfs::walk::{ContinuedWalk, Walker};
use std::pin::Pin;
use std::task::Context;
use std::{
    path::{Path, PathBuf},
    task::Poll,
    time::Duration,
};
#[cfg(not(target_arch = "wasm32"))]
use tokio::io::AsyncWriteExt;
use tracing::{Instrument, Span};

use crate::repo::DefaultStorage;
use crate::{Ipfs, IpfsPath, dag::IpldDag, repo::Repo};

#[allow(unused_imports)]
use super::{TraversalFailed, UnixfsStatus};

#[must_use = "does nothing unless you `.await` or poll the stream"]
pub struct UnixfsGet {
    core: Option<Either<Ipfs, Repo<DefaultStorage>>>,
    dest: PathBuf,
    span: Span,
    path: Option<IpfsPath>,
    providers: Vec<PeerId>,
    local_only: bool,
    timeout: Option<Duration>,
    stream: Option<BoxStream<'static, UnixfsStatus>>,
}

impl UnixfsGet {
    pub fn with_ipfs(ipfs: &Ipfs, path: impl Into<IpfsPath>, dest: impl AsRef<Path>) -> Self {
        Self::with_either(Either::Left(ipfs.clone()), path, dest)
    }

    pub fn with_repo(
        repo: &Repo<DefaultStorage>,
        path: impl Into<IpfsPath>,
        dest: impl AsRef<Path>,
    ) -> Self {
        Self::with_either(Either::Right(repo.clone()), path, dest)
    }

    fn with_either(
        core: Either<Ipfs, Repo<DefaultStorage>>,
        path: impl Into<IpfsPath>,
        dest: impl AsRef<Path>,
    ) -> Self {
        let path = path.into();
        let dest = dest.as_ref().to_path_buf();
        Self {
            core: Some(core),
            dest,
            path: Some(path),
            span: Span::current(),
            providers: Vec::new(),
            local_only: false,
            timeout: None,
            stream: None,
        }
    }

    pub fn span(mut self, span: Span) -> Self {
        self.span = span;
        self
    }

    pub fn provider(mut self, peer_id: PeerId) -> Self {
        if !self.providers.contains(&peer_id) {
            self.providers.push(peer_id);
        }
        self
    }

    pub fn providers(mut self, list: &[PeerId]) -> Self {
        self.providers = list.to_vec();
        self
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    pub fn local(mut self) -> Self {
        self.local_only = true;
        self
    }

    pub fn set_local(mut self, local: bool) -> Self {
        self.local_only = local;
        self
    }
}

impl Stream for UnixfsGet {
    type Item = UnixfsStatus;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        if self.core.is_none() && self.stream.is_none() {
            return Poll::Ready(None);
        }
        loop {
            match &mut self.stream {
                None => {
                    let (repo, dag) = match self.core.take().expect("ipfs or repo is used") {
                        Either::Left(ipfs) => (ipfs.repo().clone(), ipfs.dag()),
                        Either::Right(repo) => (repo.clone(), IpldDag::from(repo.clone())),
                    };

                    let path = self.path.take().expect("starting point exist");
                    let providers = std::mem::take(&mut self.providers);
                    let local_only = self.local_only;
                    let timeout = self.timeout;
                    let dest = self.dest.clone();

                    #[cfg(not(target_arch = "wasm32"))]
                    let stream = async_stream::stream! {

                        let mut cache = None;
                        let mut total_size = None;
                        let mut written = 0;

                        let mut file = match tokio::fs::File::create(dest)
                            .await
                            .map_err(TraversalFailed::Io) {
                                Ok(f) => f,
                                Err(e) => {
                                    yield UnixfsStatus::FailedStatus { written, total_size, error: e.into() };
                                    return;
                                }
                            };

                        let block  = match dag
                            ._resolve(path.clone(), true, &providers, local_only, timeout)
                            .await
                            .map_err(TraversalFailed::Resolving)
                            .and_then(|(resolved, _)| resolved.into_unixfs_block().map_err(TraversalFailed::Path)) {
                                Ok(block) => block,
                                Err(e) => {
                                    yield UnixfsStatus::FailedStatus { written, total_size, error: e.into() };
                                    return;
                                }
                        };

                        let cid = block.cid();
                        let root_name = block.cid().to_string();

                        let mut walker = Walker::new(*cid, root_name);
                        
                        // Process the root block
                        match walker.next(block.data(), &mut cache) {
                            Ok(ContinuedWalk::Bucket(..)) => {}
                            Ok(ContinuedWalk::File(segment, _, _, _, size)) => {
                                if segment.is_first() {
                                    total_size = Some(size as usize);
                                    yield UnixfsStatus::ProgressStatus { written, total_size };
                                }
                                
                                let slice = segment.as_ref();
                                if let Err(e) = file.write_all(slice).await {
                                    yield UnixfsStatus::FailedStatus { written, total_size, error: e.into() };
                                    return;
                                }
                                if let Err(e) = file.sync_all().await {
                                    yield UnixfsStatus::FailedStatus { written, total_size, error: e.into() };
                                    return;
                                }
                                written += slice.len();
                                yield UnixfsStatus::ProgressStatus { written, total_size };
                            },
                            Ok(ContinuedWalk::Directory( .. )) | Ok(ContinuedWalk::RootDirectory( .. )) => {}, //TODO
                            Ok(ContinuedWalk::Symlink( .. )) => {},
                            Err(e) => {
                                yield UnixfsStatus::FailedStatus { written, total_size, error: e.into() };
                                return;
                            }
                        };

                        // Process remaining blocks as they arrive from the stream
                        const BATCH_SIZE: usize = 32; // Reasonable batch size for block fetching
                        
                        while walker.should_continue() {
                            // Collect CIDs for batch processing
                            let mut batch_cids = Vec::new();
                            
                            // Peek at pending links to collect a batch
                            let (first, rest) = walker.pending_links();
                            batch_cids.push(*first);
                            
                            for cid in rest.take(BATCH_SIZE - 1) {
                                batch_cids.push(*cid);
                            }
                            
                            // Fetch blocks in batch and process as they arrive
                            let mut blocks_stream = repo.get_blocks(&batch_cids)
                                .providers(&providers)
                                .set_local(local_only)
                                .timeout(timeout);
                            
                            // Process each block as it arrives from the stream
                            while let Some(result) = blocks_stream.next().await {
                                let block = match result {
                                    Ok(block) => block,
                                    Err(e) => {
                                        yield UnixfsStatus::FailedStatus { written, total_size, error: e };
                                        return;
                                    }
                                };
                                
                                let block_data = block.data();
                                
                                match walker.next(block_data, &mut cache) {
                                    Ok(ContinuedWalk::Bucket(..)) => {}
                                    Ok(ContinuedWalk::File(segment, _, _, _, size)) => {
                                        if segment.is_first() {
                                            total_size = Some(size as usize);
                                            yield UnixfsStatus::ProgressStatus { written, total_size };
                                        }
                                        
                                        let slice = segment.as_ref();
                                        if let Err(e) = file.write_all(slice).await {
                                            yield UnixfsStatus::FailedStatus { written, total_size, error: e.into() };
                                            return;
                                        }
                                        if let Err(e) = file.sync_all().await {
                                            yield UnixfsStatus::FailedStatus { written, total_size, error: e.into() };
                                            return;
                                        }
                                        
                                        written += slice.len();
                                        yield UnixfsStatus::ProgressStatus { written, total_size };
                                    },
                                    Ok(ContinuedWalk::Directory( .. )) | Ok(ContinuedWalk::RootDirectory( .. )) => {}, //TODO
                                    Ok(ContinuedWalk::Symlink( .. )) => {},
                                    Err(e) => {
                                        yield UnixfsStatus::FailedStatus { written, total_size, error: e.into() };
                                        return;
                                    }
                                };
                            }
                        }

                        yield UnixfsStatus::CompletedStatus { path, written, total_size }
                    };

                    #[cfg(target_arch = "wasm32")]
                    let stream = async_stream::stream! {
                        _ = repo;
                        _ = dag;
                        _ = path;
                        _ = providers;
                        _ = local_only;
                        _ = timeout;
                        _ = dest;
                        yield UnixfsStatus::FailedStatus { written: 0, total_size: None, error: anyhow::anyhow!("unimplemented") };
                    };

                    self.stream = Some(stream.boxed());
                }
                Some(stream) => match futures::ready!(stream.poll_next_unpin(cx)) {
                    Some(item) => {
                        if matches!(
                            item,
                            UnixfsStatus::FailedStatus { .. }
                                | UnixfsStatus::CompletedStatus { .. }
                        ) {
                            self.stream.take();
                        }
                        return Poll::Ready(Some(item));
                    }
                    None => {
                        self.stream.take();
                        return Poll::Ready(None);
                    }
                },
            }
        }
    }
}

impl std::future::IntoFuture for UnixfsGet {
    type Output = Result<(), anyhow::Error>;

    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(mut self) -> Self::IntoFuture {
        let span = self.span.clone();
        async move {
            while let Some(status) = self.next().await {
                match status {
                    UnixfsStatus::FailedStatus { error, .. } => {
                        return Err(error);
                    }
                    UnixfsStatus::CompletedStatus { .. } => return Ok(()),
                    _ => {}
                }
            }
            Err::<_, anyhow::Error>(anyhow::anyhow!("Unable to get file"))
        }
        .instrument(span)
        .boxed()
    }
}

impl FusedStream for UnixfsGet {
    fn is_terminated(&self) -> bool {
        self.stream.is_none() && self.core.is_none()
    }
}