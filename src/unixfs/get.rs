use std::{
    path::{Path, PathBuf},
    task::Poll,
    time::Duration,
};

use either::Either;
use futures::{future::BoxFuture, FutureExt, Stream, StreamExt};
use libp2p::PeerId;
use rust_unixfs::walk::{ContinuedWalk, Walker};
use tokio::io::AsyncWriteExt;
use tracing::{Instrument, Span};

use crate::{dag::IpldDag, repo::Repo, Ipfs, IpfsPath};

use super::{StatusStreamState, TraversalFailed, UnixfsStatus};

pub struct UnixfsGet {
    core: Option<Either<Ipfs, Repo>>,
    dest: PathBuf,
    span: Span,
    path: Option<IpfsPath>,
    providers: Vec<PeerId>,
    local_only: bool,
    timeout: Option<Duration>,
    stream: StatusStreamState,
}

impl UnixfsGet {
    pub fn with_ipfs(ipfs: &Ipfs, path: impl Into<IpfsPath>, dest: impl AsRef<Path>) -> Self {
        Self::with_either(Either::Left(ipfs.clone()), path, dest)
    }

    pub fn with_repo(repo: &Repo, path: impl Into<IpfsPath>, dest: impl AsRef<Path>) -> Self {
        Self::with_either(Either::Right(repo.clone()), path, dest)
    }

    fn with_either(
        core: Either<Ipfs, Repo>,
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
            stream: StatusStreamState::None,
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
        self.providers = list.to_owned();
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
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        loop {
            match &mut self.stream {
                StatusStreamState::None => {
                    let core = self.core.take().expect("ipfs or repo is used");

                    let path = self.path.take().expect("starting point exist");
                    let providers = std::mem::take(&mut self.providers);
                    let local_only = self.local_only;
                    let timeout = self.timeout;
                    let dest = self.dest.clone();

                    let stream = async_stream::stream! {
                        let (repo, dag, session) = match core {
                            Either::Left(ipfs) => (
                                ipfs.repo().clone(),
                                ipfs.dag(),
                                Some(
                                    crate::BITSWAP_ID.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
                                ),
                            ),
                            Either::Right(repo) => {
                                let session = repo.is_online().await.then(|| {
                                    crate::BITSWAP_ID.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                                });
                                (repo.clone(), IpldDag::from(repo.clone()), session)
                            }
                        };

                        let mut cache = None;
                        let mut total_size = None;
                        let mut written = 0;

                        let mut file = match tokio::fs::File::create(dest)
                            .await
                            .map_err(TraversalFailed::Io) {
                                Ok(f) => f,
                                Err(e) => {
                                    yield UnixfsStatus::FailedStatus { written, total_size, error: Some(anyhow::Error::from(e)) };
                                    return;
                                }
                            };

                        let block  = match dag
                            .resolve_with_session(session, path.clone(), true, &providers, local_only, timeout)
                            .await
                            .map_err(TraversalFailed::Resolving)
                            .and_then(|(resolved, _)| resolved.into_unixfs_block().map_err(TraversalFailed::Path)) {
                                Ok(block) => block,
                                Err(e) => {
                                    yield UnixfsStatus::FailedStatus { written, total_size, error: Some(anyhow::Error::from(e)) };
                                    return;
                                }
                        };

                        let cid = block.cid();
                        let root_name = block.cid().to_string();

                        let mut walker = Walker::new(*cid, root_name);

                        while walker.should_continue() {
                            let (next, _) = walker.pending_links();
                            let block = match repo.get_block_with_session(session, next, &providers, local_only, timeout).await {
                                Ok(block) => block,
                                Err(e) => {
                                    yield UnixfsStatus::FailedStatus { written, total_size, error: Some(e) };
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
                                    // even if the largest of files can have 256 kB blocks and about the same
                                    // amount of content, try to consume it in small parts not to grow the buffers
                                    // too much.

                                    let mut n = 0usize;
                                    let slice = segment.as_ref();
                                    let total = slice.len();

                                    while n < total {
                                        let next = &slice[n..];
                                        n += next.len();
                                        if let Err(e) = file.write_all(next).await {
                                            yield UnixfsStatus::FailedStatus { written, total_size, error: Some(anyhow::Error::from(e)) };
                                            return;
                                        }
                                        if let Err(e) = file.sync_all().await {
                                            yield UnixfsStatus::FailedStatus { written, total_size, error: Some(anyhow::Error::from(e)) };
                                            return;
                                        }

                                        written += n;
                                        yield UnixfsStatus::ProgressStatus { written, total_size };
                                    }

                                    if segment.is_last() {
                                        yield UnixfsStatus::ProgressStatus { written, total_size };
                                    }
                                },
                                Ok(ContinuedWalk::Directory( .. )) | Ok(ContinuedWalk::RootDirectory( .. )) => {}, //TODO
                                Ok(ContinuedWalk::Symlink( .. )) => {},
                                Err(e) => {
                                    yield UnixfsStatus::FailedStatus { written, total_size, error: Some(anyhow::Error::from(e)) };
                                    return;
                                }
                            };
                        };

                        yield UnixfsStatus::CompletedStatus { path, written, total_size }
                    };

                    self.stream = StatusStreamState::Pending {
                        stream: stream.boxed(),
                    };
                }
                StatusStreamState::Pending { stream } => {
                    match futures::ready!(stream.poll_next_unpin(cx)) {
                        Some(item) => {
                            if matches!(
                                item,
                                UnixfsStatus::FailedStatus { .. }
                                    | UnixfsStatus::CompletedStatus { .. }
                            ) {
                                self.stream = StatusStreamState::Done;
                            }
                            return Poll::Ready(Some(item));
                        }
                        None => {
                            self.stream = StatusStreamState::Done;
                            return Poll::Ready(None);
                        }
                    }
                }
                StatusStreamState::Done => return Poll::Ready(None),
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
                        let error = error
                            .unwrap_or(anyhow::anyhow!("Unknown error while writting to disk"));
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
