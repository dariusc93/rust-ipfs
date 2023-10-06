use std::time::Duration;

use either::Either;
use futures::{stream::BoxStream, StreamExt};
use libipld::Cid;
use libp2p::PeerId;
use rust_unixfs::walk::{ContinuedWalk, Walker};

use crate::{dag::IpldDag, repo::Repo, Ipfs, IpfsPath};

#[derive(Debug)]
pub enum NodeItem {
    Error { error: anyhow::Error },
    RootDirectory { cid: Cid, path: String },
    Directory { cid: Cid, path: String },
    File { cid: Cid, file: String, size: usize },
}

pub async fn ls<'a>(
    which: Either<&Ipfs, &Repo>,
    path: IpfsPath,
    providers: &'a [PeerId],
    local_only: bool,
    timeout: Option<Duration>,
) -> anyhow::Result<BoxStream<'a, NodeItem>> {
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

    let (resolved, _) = dag
        .resolve_with_session(session, path.clone(), true, providers, local_only, timeout)
        .await?;

    let block = resolved.into_unixfs_block()?;

    let cid = block.cid();
    let root_name = block.cid().to_string();

    let mut walker = Walker::new(*cid, root_name);

    let stream = async_stream::stream! {
        let mut cache = None;
        let mut root_directory = String::new();
        while walker.should_continue() {
            let (next, _) = walker.pending_links();
            let block = match repo.get_block_with_session(session, next, providers, local_only, timeout).await {
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

    Ok(stream.boxed())
}
