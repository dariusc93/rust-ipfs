use std::{path::Path, time::Duration};

use either::Either;
use futures::{future::BoxFuture, stream::BoxStream, FutureExt, Stream, StreamExt};
use libp2p::PeerId;
use rust_unixfs::walk::{ContinuedWalk, Walker};
use tokio::io::AsyncWriteExt;
use tracing::{Instrument, Span};

use crate::{dag::IpldDag, repo::Repo, Ipfs, IpfsPath};

use super::{TraversalFailed, UnixfsStatus};

pub fn get<'a, P: AsRef<Path>>(
    which: Either<&Ipfs, &Repo>,
    path: IpfsPath,
    dest: P,
    providers: &'a [PeerId],
    local_only: bool,
    timeout: Option<Duration>,
) -> UnixfsGet<'a> {
    let (repo, dag, session) = match which {
        Either::Left(ipfs) => (
            ipfs.repo().clone(),
            ipfs.dag(),
            Some(crate::BITSWAP_ID.fetch_add(1, std::sync::atomic::Ordering::SeqCst)),
        ),
        Either::Right(repo) => {
            let session = repo
                .is_online()
                .then_some(crate::BITSWAP_ID.fetch_add(1, std::sync::atomic::Ordering::SeqCst));
            (repo.clone(), IpldDag::from(repo.clone()), session)
        }
    };

    let dest = dest.as_ref().to_path_buf();

    let stream = async_stream::stream! {

        let mut cache = None;
        let mut total_size = None;
        let mut written = 0;

        let mut file = match tokio::fs::File::create(dest)
            .await
            .map_err(TraversalFailed::Io) {
                Ok(f) => f,
                Err(e) => {
                    yield UnixfsStatus::FailedStatus { written, total_size, error: Some(e.into()) };
                    return;
                }
            };

        let block  = match dag
            .resolve_with_session(session, path.clone(), true, providers, local_only, timeout)
            .await
            .map_err(TraversalFailed::Resolving)
            .and_then(|(resolved, _)| resolved.into_unixfs_block().map_err(TraversalFailed::Path)) {
                Ok(block) => block,
                Err(e) => {
                    yield UnixfsStatus::FailedStatus { written, total_size, error: Some(e.into()) };
                    return;
                }
        };

        let cid = block.cid();
        let root_name = block.cid().to_string();

        let mut walker = Walker::new(*cid, root_name);

        while walker.should_continue() {
            let (next, _) = walker.pending_links();
            let block = match repo.get_block_with_session(session, next, providers, local_only, timeout).await {
                Ok(block) => block,
                Err(e) => {
                    yield UnixfsStatus::FailedStatus { written, total_size, error: Some(anyhow::anyhow!("{e}")) };
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
                            yield UnixfsStatus::FailedStatus { written, total_size, error: Some(anyhow::anyhow!("{e}")) };
                            return;
                        }
                        if let Err(e) = file.sync_all().await {
                            yield UnixfsStatus::FailedStatus { written, total_size, error: Some(anyhow::anyhow!("{e}")) };
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
                    yield UnixfsStatus::FailedStatus { written, total_size, error: Some(anyhow::anyhow!("{e}")) };
                    return;
                }
            };
        };

        yield UnixfsStatus::CompletedStatus { path, written, total_size };
    };

    UnixfsGet {
        stream: stream.boxed(),
        span: None,
    }
}

pub struct UnixfsGet<'a> {
    stream: BoxStream<'a, UnixfsStatus>,
    span: Option<Span>,
}

impl<'a> UnixfsGet<'a> {
    pub fn span(mut self, span: Span) -> Self {
        self.span = Some(span);
        self
    }
}

impl<'a> Stream for UnixfsGet<'a> {
    type Item = UnixfsStatus;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.stream.poll_next_unpin(cx)
    }
}

impl<'a> std::future::IntoFuture for UnixfsGet<'a> {
    type Output = Result<(), anyhow::Error>;

    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(mut self) -> Self::IntoFuture {
        let span = self.span.unwrap_or(Span::current());
        async move {
            while let Some(status) = self.stream.next().await {
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
