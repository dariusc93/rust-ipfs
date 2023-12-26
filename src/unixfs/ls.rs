use std::{time::Duration, num::NonZeroU8};

use either::Either;
use futures::{future::BoxFuture, stream::BoxStream, FutureExt, Stream, StreamExt};
use libipld::Cid;
use libp2p::PeerId;
use rust_unixfs::walk::{ContinuedWalk, Walker};
use tracing::{Instrument, Span};

use crate::{dag::IpldDag, repo::Repo, Ipfs, IpfsPath};

#[derive(Debug)]
pub enum NodeItem {
    Error { error: anyhow::Error },
    RootDirectory { cid: Cid, path: String },
    Directory { cid: Cid, path: String },
    File { cid: Cid, file: String, size: usize },
}

pub fn ls<'a>(
    which: Either<&Ipfs, &Repo>,
    path: IpfsPath,
    providers: &'a [PeerId],
    local_only: bool,
    timeout: Option<Duration>,
    retry: Option<NonZeroU8>,
) -> UnixfsLs<'a> {
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

    let stream = async_stream::stream! {

        let resolved = match dag
            .resolve_with_session(session, path.clone(), true, providers, local_only, timeout, retry)
            .await {
                Ok((resolved, _)) => resolved,
                Err(e) => {
                    yield NodeItem::Error { error: e.into() };
                    return;
                }
            };

        let block = match resolved.into_unixfs_block() {
            Ok(block) => block,
            Err(e) => {
                yield NodeItem::Error { error: e.into() };
                return;
            }
        };

        let cid = block.cid();
        let root_name = cid.to_string();

        let mut walker = Walker::new(*cid, root_name);
        let mut cache = None;
        let mut root_directory = String::new();
        while walker.should_continue() {
            let (next, _) = walker.pending_links();
            let block = match repo.get_block_with_session(session, next, providers, local_only, timeout, retry).await {
                Ok(block) => block,
                Err(error) => {
                    yield NodeItem::Error { error };
                    return;
                }
            };
            let block_data = block.data();

            match walker.next(block_data, &mut cache) {
                Ok(ContinuedWalk::Bucket(..)) => {}
                Ok(ContinuedWalk::File(_, cid, path, _, size)) => {
                    let file = path.to_string_lossy().to_string().replace(&format!("{root_directory}/"), "");
                    yield NodeItem::File { cid: *cid, file, size: size as _ };
                },
                Ok(ContinuedWalk::RootDirectory( cid, path, _)) => {
                    let path = path.to_string_lossy().to_string();
                    root_directory = path.clone();
                    yield NodeItem::RootDirectory { cid: *cid, path };
                }
                Ok(ContinuedWalk::Directory( cid, path, _)) => {
                    let path = path.to_string_lossy().to_string().replace(&format!("{root_directory}/"), "");
                    yield NodeItem::Directory { cid: *cid, path };
                }
                Ok(ContinuedWalk::Symlink( .. )) => {},
                Err(error) => {
                    yield NodeItem::Error { error: anyhow::anyhow!("{error}") };
                    return;
                }
            };
        };

    };

    UnixfsLs {
        stream: stream.boxed(),
        span: None,
    }
}

pub struct UnixfsLs<'a> {
    stream: BoxStream<'a, NodeItem>,
    span: Option<Span>,
}

impl<'a> UnixfsLs<'a> {
    pub fn span(mut self, span: Span) -> Self {
        self.span = Some(span);
        self
    }
}

impl<'a> Stream for UnixfsLs<'a> {
    type Item = NodeItem;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.stream.poll_next_unpin(cx)
    }
}

impl<'a> std::future::IntoFuture for UnixfsLs<'a> {
    type Output = Result<Vec<NodeItem>, anyhow::Error>;

    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(mut self) -> Self::IntoFuture {
        let span = self.span.unwrap_or(Span::current());
        async move {
            let mut items = vec![];
            while let Some(status) = self.stream.next().await {
                match status {
                    NodeItem::Error { error } => return Err(error),
                    item => items.push(item),
                }
            }
            Ok(items)
        }
        .instrument(span)
        .boxed()
    }
}
