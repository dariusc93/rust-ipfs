use std::path::{Path, PathBuf};

use crate::{repo::Repo, Block};
use either::Either;
use futures::{future::BoxFuture, stream::BoxStream, FutureExt, Stream, StreamExt, TryFutureExt};
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

pub enum AddOpt<'a> {
    File(PathBuf),
    Stream {
        name: Option<String>,
        total: Option<usize>,
        stream: BoxStream<'a, std::result::Result<Vec<u8>, std::io::Error>>,
    },
}

impl<'a> From<PathBuf> for AddOpt<'a> {
    fn from(path: PathBuf) -> Self {
        AddOpt::File(path)
    }
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

pub struct UnixfsAddFuture<'a> {
    stream: BoxStream<'a, UnixfsStatus>,
}

pub fn add_file<'a, P: AsRef<Path>>(
    which: Either<&Ipfs, &Repo>,
    path: P,
    opt: Option<AddOption>,
) -> UnixfsAddFuture<'a>
where
{
    let path = path.as_ref().to_path_buf();
    add(which, path.into(), opt)
}

pub fn add<'a>(
    which: Either<&Ipfs, &Repo>,
    options: AddOpt<'a>,
    opt: Option<AddOption>,
) -> UnixfsAddFuture<'a> {
    let (ipfs, repo) = match which {
        Either::Left(ipfs) => {
            let repo = ipfs.repo().clone();
            let ipfs = ipfs.clone();
            (Some(ipfs), repo)
        }
        Either::Right(repo) => (None, repo.clone()),
    };

    let stream = async_stream::stream! {

        let mut written = 0;

        let (name, total_size, mut stream) = match options {
            AddOpt::File(path) => match tokio::fs::File::open(path.clone())
                .and_then(|file| async move {
                    let size = file.metadata().await?.len() as usize;

                    let stream = ReaderStream::new(file).map(|x| x.map(|x| x.into()));

                    let name: Option<String> = path.file_name().map(|f| f.to_string_lossy().to_string());

                    Ok((name, Some(size), stream.boxed()))
                }).await {
                    Ok(s) => s,
                    Err(e) => {
                        yield UnixfsStatus::FailedStatus { written, total_size: None, error: Some(anyhow::anyhow!("{e}")) };
                        return;
                    }
                },
            AddOpt::Stream { name, total, stream } => (name, total, stream),
        };

        let mut adder = FileAdderBuilder::default()
            .with_chunker(opt.map(|o| o.chunk.unwrap_or_default()).unwrap_or_default())
            .build();

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

    UnixfsAddFuture {
        stream: stream.boxed(),
    }
}

impl<'a> Stream for UnixfsAddFuture<'a> {
    type Item = UnixfsStatus;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.stream.poll_next_unpin(cx)
    }
}

impl<'a> std::future::IntoFuture for UnixfsAddFuture<'a> {
    type Output = Result<IpfsPath, anyhow::Error>;

    type IntoFuture = BoxFuture<'a, Self::Output>;

    fn into_future(mut self) -> Self::IntoFuture {
        async move {
            while let Some(status) = self.stream.next().await {
                match status {
                    UnixfsStatus::CompletedStatus { path, .. } => return Ok(path),
                    UnixfsStatus::FailedStatus { error, .. } => {
                        return Err(error.unwrap_or(anyhow::anyhow!("Unable to add file")));
                    }
                    _ => {}
                }
            }
            Err::<_, anyhow::Error>(anyhow::anyhow!("Unable to add file"))
        }
        .boxed()
    }
}
