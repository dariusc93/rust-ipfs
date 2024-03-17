use crate::{dag::IpldDag, repo::Repo, Block, Ipfs};
use async_stream::stream;
use bytes::Bytes;
use either::Either;
use futures::future::BoxFuture;
use futures::stream::{BoxStream, FusedStream, Stream};
use futures::{FutureExt, StreamExt, TryStreamExt};
use libp2p::PeerId;
use rust_unixfs::file::visit::IdleFileVisit;
use std::ops::Range;
use std::task::Poll;
use std::{borrow::Borrow, time::Duration};
use tracing::{Instrument, Span};

use super::TraversalFailed;

/// IPFS cat operation, producing a stream of file bytes. This is generic over the different kinds
/// of ways to own an `Ipfs` value in order to support both operating with borrowed `Ipfs` value
/// and an owned value. Passing an owned value allows the return value to be `'static`, which can
/// be helpful in some contexts, like the http.
///
/// Returns a stream of bytes on the file pointed with the Cid.
#[must_use = "do nothing unless you `.await` or poll the stream"]
pub struct UnixfsCat {
    core: Option<Either<Ipfs, Repo>>,
    span: Span,
    starting_point: Option<StartingPoint>,
    range: Option<Range<u64>>,
    providers: Vec<PeerId>,
    local_only: bool,
    timeout: Option<Duration>,
    stream: Option<BoxStream<'static, Result<Bytes, TraversalFailed>>>,
}

impl UnixfsCat {
    pub fn with_ipfs(ipfs: &Ipfs, starting_point: impl Into<StartingPoint>) -> Self {
        Self::with_either(Either::Left(ipfs.clone()), starting_point)
    }

    pub fn with_repo(repo: &Repo, starting_point: impl Into<StartingPoint>) -> Self {
        Self::with_either(Either::Right(repo.clone()), starting_point)
    }

    fn with_either(core: Either<Ipfs, Repo>, starting_point: impl Into<StartingPoint>) -> Self {
        let starting_point = starting_point.into();
        Self {
            core: Some(core),
            starting_point: Some(starting_point),
            span: Span::current(),
            range: None,
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

/// The starting point for unixfs walks. Can be converted from IpfsPath and Blocks, and Cids can be
/// converted to IpfsPath.
pub enum StartingPoint {
    Left(crate::IpfsPath),
    Right(Block),
}

impl<T: Into<crate::IpfsPath>> From<T> for StartingPoint {
    fn from(a: T) -> Self {
        Self::Left(a.into())
    }
}

impl From<Block> for StartingPoint {
    fn from(b: Block) -> Self {
        Self::Right(b)
    }
}

impl Stream for UnixfsCat {
    type Item = Result<Bytes, TraversalFailed>;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        loop {
            match &mut self.stream {
                Some(stream) => match futures::ready!(stream.poll_next_unpin(cx)) {
                    None => {
                        self.stream.take();
                        return Poll::Ready(None);
                    }
                    task => return Poll::Ready(task),
                },
                None => {
                    let Some(core) = self.core.take() else {
                        return Poll::Ready(None);
                    };

                    let (repo, dag, session) = match core {
                        Either::Left(ipfs) => (
                            ipfs.repo().clone(),
                            ipfs.dag(),
                            Some(
                                crate::BITSWAP_ID.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
                            ),
                        ),
                        Either::Right(repo) => {
                            let session = repo.is_online().then(|| {
                                crate::BITSWAP_ID.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                            });
                            (repo.clone(), IpldDag::from(repo.clone()), session)
                        }
                    };

                    let mut visit = IdleFileVisit::default();

                    if let Some(range) = self.range.clone() {
                        visit = visit.with_target_range(range);
                    }

                    let starting_point = self.starting_point.take().expect("starting point exist");
                    let providers = std::mem::take(&mut self.providers);
                    let local_only = self.local_only;
                    let timeout = self.timeout;

                    // using async_stream here at least to get on faster; writing custom streams is not too easy
                    // but this might be easy enough to write open.
                    let stream = stream! {

                        // Get the root block to start the traversal. The stream does not expose any of the file
                        // metadata. To get to it the user needs to create a Visitor over the first block.
                        let block = match starting_point {
                            StartingPoint::Left(path) => match dag
                                .resolve_with_session(session, path.clone(), true, &providers, local_only, timeout)
                                .await
                                .map_err(TraversalFailed::Resolving)
                                .and_then(|(resolved, _)| {
                                    resolved.into_unixfs_block().map_err(TraversalFailed::Path)
                                }) {
                                    Ok(block) => block,
                                    Err(e) => {
                                        yield Err(e);
                                        return;
                                    }
                                },
                            StartingPoint::Right(block) => block,
                        };

                        let mut cache = None;
                        // Start the visit from the root block. We need to move the both components as Options into the
                        // stream as we can't yet return them from this Future context.
                        let (visit, bytes) = match visit.start(block.data()) {
                            Ok((bytes, _, _, visit)) => {
                                let bytes = if !bytes.is_empty() {
                                    Some(Bytes::copy_from_slice(bytes))
                                } else {
                                    None
                                };

                                (visit, bytes)
                            }
                            Err(e) => {
                                yield Err(TraversalFailed::Walking(*block.cid(), e));
                                return;
                            }
                        };

                        if let Some(bytes) = bytes {
                            yield Ok(bytes);
                        }

                        let mut visit = match visit {
                            Some(visit) => visit,
                            None => return,
                        };

                        loop {
                            // TODO: if it was possible, it would make sense to start downloading N of these
                            // we could just create an FuturesUnordered which would drop the value right away. that
                            // would probably always cost many unnecessary clones, but it would be nice to "shut"
                            // the subscriber so that it will only resolve to a value but still keep the operation
                            // going. Not that we have any "operation" concept of the Want yet.
                            let (next, _) = visit.pending_links();

                            let borrow = repo.borrow();
                            let block = match borrow.get_block_with_session(session, next, &providers, local_only, timeout).await {
                                Ok(block) => block,
                                Err(e) => {
                                    yield Err(TraversalFailed::Loading(*next, e));
                                    return;
                                },
                            };

                            match visit.continue_walk(block.data(), &mut cache) {
                                Ok((bytes, next_visit)) => {
                                    if !bytes.is_empty() {
                                        // TODO: manual implementation could allow returning just the slice
                                        yield Ok(Bytes::copy_from_slice(bytes));
                                    }

                                    match next_visit {
                                        Some(v) => visit = v,
                                        None => return,
                                    }
                                }
                                Err(e) => {
                                    yield Err(TraversalFailed::Walking(*block.cid(), e));
                                    return;
                                }
                            }
                        }
                    }.boxed();

                    self.stream.replace(stream);
                }
            }
        }
    }
}

impl std::future::IntoFuture for UnixfsCat {
    type Output = Result<Bytes, TraversalFailed>;

    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(mut self) -> Self::IntoFuture {
        let span = self.span.clone();
        async move {
            let mut data = vec![];
            while let Some(bytes) = self.try_next().await? {
                data.extend(bytes);
            }
            Ok(data.into())
        }
        .instrument(span)
        .boxed()
    }
}

impl FusedStream for UnixfsCat {
    fn is_terminated(&self) -> bool {
        self.stream.is_none() && self.core.is_none()
    }
}
