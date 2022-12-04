use std::{borrow::Borrow, path::Path};

use futures::{stream::BoxStream, StreamExt};
use ipfs_unixfs::walk::{ContinuedWalk, Walker};
use tokio::io::AsyncWriteExt;

use crate::{Ipfs, IpfsPath, IpfsTypes};

use super::UnixfsStatus;

#[inline]
pub async fn get<'a, Types, MaybeOwned, P: AsRef<Path>>(
    ipfs: MaybeOwned,
    path: IpfsPath,
    dest: P,
) -> anyhow::Result<BoxStream<'a, anyhow::Result<UnixfsStatus>>>
where
    Types: IpfsTypes,
    MaybeOwned: Borrow<Ipfs<Types>> + Send + 'a,
{
    let dest = dest.as_ref();
    if dest.is_file() {
        anyhow::bail!(std::io::Error::from(std::io::ErrorKind::AlreadyExists))
    }
    let mut file = tokio::fs::File::create(dest).await?;
    let ipfs = ipfs.borrow().clone();

    let (resolved, _) = ipfs.dag().resolve(path.clone(), true).await?;

    let block = resolved.into_unixfs_block()?;

    let cid = block.cid();
    let root_name = block.cid().to_string();

    let mut walker = Walker::new(*cid, root_name);

    let stream = async_stream::try_stream! {
        let ipfs = ipfs.clone();
        let path = path.clone();
        let mut cache = None;
        let mut total_size = None;
        let mut written = 0;
        while walker.should_continue() {
            let (next, _) = walker.pending_links();
            let block = ipfs.get_block(next).await?;
            let block_data = block.data();

            match walker.next(&block_data, &mut cache)? {
                ContinuedWalk::Bucket(..) => {}
                ContinuedWalk::File(segment, _, path, metadata, size) => {

                    if segment.is_first() {

                        file.set_len(size).await?;
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
                        file.write_all(&next).await?;
                        file.sync_all().await?;

                        written += n;
                        yield UnixfsStatus::ProgressStatus { written, total_size };
                    }

                    if segment.is_last() {
                        file.flush().await?;
                        yield UnixfsStatus::ProgressStatus { written, total_size };
                    }
                },
                ContinuedWalk::Directory( .. ) | ContinuedWalk::RootDirectory( .. ) => {}, //TODO
                ContinuedWalk::Symlink( .. ) => {},
            };
        };

        yield UnixfsStatus::CompletedStatus { path, written, total_size };
    };

    Ok(stream.boxed())
}
