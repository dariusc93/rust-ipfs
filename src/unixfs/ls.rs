use futures::{stream::BoxStream, StreamExt};
use libipld::Cid;
use libp2p::PeerId;
use rust_unixfs::walk::{ContinuedWalk, Walker};

use crate::{Ipfs, IpfsPath};

#[derive(Debug)]
pub enum NodeItem {
    Error { error: anyhow::Error },
    RootDirectory { cid: Cid, path: String },
    Directory { cid: Cid, path: String },
    File { cid: Cid, file: String, size: usize },
}

pub async fn ls<'a>(
    ipfs: &Ipfs,
    path: IpfsPath,
    providers: &'a [PeerId],
    local_only: bool,
) -> anyhow::Result<BoxStream<'a, NodeItem>> {
    let ipfs = ipfs.clone();

    let (resolved, _) = ipfs
        .dag()
        .resolve(path.clone(), true, providers, local_only)
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
            let block = match ipfs.repo().get_block(next, providers, local_only).await {
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
