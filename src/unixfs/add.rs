use std::path::Path;

use crate::{repo::Repo, Block};
use either::Either;
use futures::{stream::BoxStream, Stream, StreamExt};
use rust_unixfs::file::adder::{Chunker, FileAdderBuilder};
use tokio_util::io::ReaderStream;

use crate::{Ipfs, IpfsPath};

use super::UnixfsStatus;

#[derive(Clone, Debug, Copy)]
pub struct AddOption {
    pub chunk: Option<Chunker>,
    pub pin: bool,
    pub provide: bool,
    pub wrap: bool,
}

impl Default for AddOption {
    fn default() -> Self {
        Self {
            chunk: Some(Chunker::Size(256 * 1024)),
            pin: false,
            provide: false,
            wrap: false,
        }
    }
}

pub async fn add_file<'a, P: AsRef<Path>>(
    which: Either<&Ipfs, &Repo>,
    path: P,
    opt: Option<AddOption>,
) -> anyhow::Result<BoxStream<'a, UnixfsStatus>>
where
{
    let path = path.as_ref().to_path_buf();

    let file = tokio::fs::File::open(&path).await?;

    let size = file.metadata().await?.len() as usize;

    let stream = ReaderStream::new(file).map(|x| x.map(|x| x.into()));

    let name = path.file_name().map(|f| f.to_string_lossy().to_string());

    add(which, name, Some(size), stream.boxed(), opt).await
}

pub async fn add<'a>(
    which: Either<&Ipfs, &Repo>,
    name: Option<String>,
    total_size: Option<usize>,
    mut stream: impl Stream<Item = std::result::Result<Vec<u8>, std::io::Error>> + Unpin + Send + 'a,
    opt: Option<AddOption>,
) -> anyhow::Result<BoxStream<'a, UnixfsStatus>> {
    let (ipfs, repo) = match which {
        Either::Left(ipfs) => {
            let repo = ipfs.repo().clone();
            let ipfs = ipfs.clone();
            (Some(ipfs), repo)
        }
        Either::Right(repo) => (None, repo.clone()),
    };

    let stream = async_stream::stream! {

        let mut adder = FileAdderBuilder::default()
            .with_chunker(opt.map(|o| o.chunk.unwrap_or_default()).unwrap_or_default())
            .build();

        let mut written = 0;
        yield UnixfsStatus::ProgressStatus { written, total_size };

        while let Some(buffer) = stream.next().await {
            let buffer = match buffer {
                Ok(buf) => buf,
                Err(e) => {
                    yield UnixfsStatus::FailedStatus { written, total_size, error: Some(anyhow::anyhow!("{e}")) };
                    return;
                }
            };

            let mut total = 0;
            while total < buffer.len() {
                let (blocks, consumed) = adder.push(&buffer[total..]);
                for (cid, block) in blocks {
                    let block = match Block::new(cid, block) {
                        Ok(block) => block,
                        Err(e) => {
                            yield UnixfsStatus::FailedStatus { written, total_size, error: Some(anyhow::anyhow!("{e}")) };
                            return;
                        }
                    };
                    let _cid = match repo.put_block(block).await {
                        Ok(cid) => cid,
                        Err(e) => {
                            yield UnixfsStatus::FailedStatus { written, total_size, error: Some(anyhow::anyhow!("{e}")) };
                            return;
                        }
                    };
                }
                total += consumed;
                written += consumed;
            }

            yield UnixfsStatus::ProgressStatus { written, total_size };
        }

        let blocks = adder.finish();
        let mut last_cid = None;

        for (cid, block) in blocks {
            let block = match Block::new(cid, block) {
                Ok(block) => block,
                Err(e) => {
                    yield UnixfsStatus::FailedStatus { written, total_size, error: Some(anyhow::anyhow!("{e}")) };
                    return;
                }
            };
            let _cid = match repo.put_block(block).await {
                Ok(cid) => cid,
                Err(e) => {
                    yield UnixfsStatus::FailedStatus { written, total_size, error: Some(anyhow::anyhow!("{e}")) };
                    return;
                }
            };
            last_cid = Some(cid);
        }

        let cid = match last_cid {
            Some(cid) => cid,
            None => {
                yield UnixfsStatus::FailedStatus { written, total_size, error: None };
                return;
            }
        };

        let mut path = IpfsPath::from(cid);

        if let Some(opt) = opt {

            if opt.wrap {
                if let Some(name) = name {
                    let result = {
                        let repo = repo.clone();
                        async move {
                            let mut opts = rust_unixfs::dir::builder::TreeOptions::default();
                            opts.wrap_with_directory();

                            let mut tree = rust_unixfs::dir::builder::BufferingTreeBuilder::new(opts);
                            tree.put_link(&name, cid, written as _)?;

                            let mut iter = tree.build();
                            let mut cids = Vec::new();

                            while let Some(node) = iter.next_borrowed() {
                                let node = node?;
                                let block = Block::new(node.cid.to_owned(), node.block.into())?;

                                repo.put_block(block).await?;

                                cids.push(*node.cid);
                            }
                            let cid = cids.last().ok_or(anyhow::anyhow!("no cid available"))?;
                            let path = IpfsPath::from(*cid).sub_path(&name)?;

                            Ok::<_, anyhow::Error>(path)
                        }
                    };

                    path = match result.await {
                        Ok(path) => path,
                        Err(e) => {
                            yield UnixfsStatus::FailedStatus { written, total_size, error: Some(anyhow::anyhow!("{e}")) };
                            return;
                        }
                    };

                }
            }

            let cid = path.root().cid().copied().expect("Cid is apart of the path");

            if opt.pin {
                if let Ok(false) = repo.is_pinned(&cid).await {
                    if let Err(e) = repo.insert_pin(&cid, true, true).await {
                        error!("Unable to pin {cid}: {e}");
                    }
                }
            }

            tokio::spawn(async move {
                if opt.provide {
                    if let Some(ipfs) = ipfs {
                        if let Err(e) = ipfs.provide(cid).await {
                            error!("Unable to provide {cid}: {e}");
                        }
                    }
                }
            });
        }

        yield UnixfsStatus::CompletedStatus { path, written, total_size }
    };

    Ok(stream.boxed())
}
