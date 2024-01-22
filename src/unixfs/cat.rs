use crate::{dag::IpldDag, repo::Repo, Block, Ipfs};
use async_stream::stream;
use either::Either;
use futures::future::BoxFuture;
use futures::stream::{BoxStream, Stream};
use futures::{FutureExt, StreamExt, TryStreamExt};
use libp2p::PeerId;
use rust_unixfs::file::visit::IdleFileVisit;
use std::num::NonZeroU8;
use std::ops::Range;
use std::{borrow::Borrow, time::Duration};
use tracing::{Instrument, Span};

use super::TraversalFailed;

/// IPFS cat operation, producing a stream of file bytes. This is generic over the different kinds
/// of ways to own an `Ipfs` value in order to support both operating with borrowed `Ipfs` value
/// and an owned value. Passing an owned value allows the return value to be `'static`, which can
/// be helpful in some contexts, like the http.
///
/// Returns a stream of bytes on the file pointed with the Cid.
pub fn cat<'a>(
    which: Either<&Ipfs, &Repo>,
    starting_point: impl Into<StartingPoint> + Send + 'a,
    range: Option<Range<u64>>,
    providers: &'a [PeerId],
    local_only: bool,
    timeout: Option<Duration>,
    retry: Option<NonZeroU8>,
) -> UnixfsCat<'a> {
    let (repo, dag, session) = match which {
        Either::Left(ipfs) => (
            ipfs.repo().clone(),
            ipfs.dag(),
            Some(crate::BITSWAP_ID.fetch_add(1, std::sync::atomic::Ordering::SeqCst)),
        ),
        Either::Right(repo) => {
            let session = repo
                .is_online()
                .then(|| crate::BITSWAP_ID.fetch_add(1, std::sync::atomic::Ordering::SeqCst));
            (repo.clone(), IpldDag::from(repo.clone()), session)
        }
    };

    let mut visit = IdleFileVisit::default();
    if let Some(range) = range {
        visit = visit.with_target_range(range);
    }

    // using async_stream here at least to get on faster; writing custom streams is not too easy
    // but this might be easy enough to write open.
    let stream = stream! {

        // Get the root block to start the traversal. The stream does not expose any of the file
        // metadata. To get to it the user needs to create a Visitor over the first block.
        let block = match starting_point.into() {
            StartingPoint::Left(path) => match dag
                .resolve_with_session(session, path.clone(), true, providers, local_only, timeout, retry)
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
                    Some(bytes.to_vec())
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
            let block = match borrow.get_block_with_session(session, next, providers, local_only, timeout, retry).await {
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
                        yield Ok(bytes.to_vec());
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
    };

    UnixfsCat {
        stream: stream.boxed(),
        span: None,
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

pub struct UnixfsCat<'a> {
    stream: BoxStream<'a, Result<Vec<u8>, TraversalFailed>>,
    span: Option<Span>,
}

impl<'a> UnixfsCat<'a> {
    pub fn span(mut self, span: Span) -> Self {
        self.span = Some(span);
        self
    }
}

impl<'a> Stream for UnixfsCat<'a> {
    type Item = Result<Vec<u8>, TraversalFailed>;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.stream.poll_next_unpin(cx)
    }
}

impl<'a> std::future::IntoFuture for UnixfsCat<'a> {
    type Output = Result<Vec<u8>, TraversalFailed>;

    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(mut self) -> Self::IntoFuture {
        let span = self.span.unwrap_or(Span::current());
        async move {
            let mut data = vec![];
            while let Some(bytes) = self.stream.try_next().await? {
                data.extend(bytes);
            }
            Ok(data)
        }
        .instrument(span)
        .boxed()
    }
}
