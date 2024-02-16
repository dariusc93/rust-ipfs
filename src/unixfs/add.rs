use std::{path::PathBuf, task::Poll};

use crate::{repo::Repo, Block};
use bytes::Bytes;
use either::Either;
use futures::{future::BoxFuture, stream::BoxStream, FutureExt, Stream, StreamExt, TryFutureExt};
use rust_unixfs::file::adder::{Chunker, FileAdderBuilder};
use tokio_util::io::ReaderStream;
use tracing::{Instrument, Span};

use crate::{Ipfs, IpfsPath};

use super::UnixfsStatus;

pub enum AddOpt {
    File(PathBuf),
    Stream {
        name: Option<String>,
        total: Option<usize>,
        stream: BoxStream<'static, std::result::Result<Bytes, std::io::Error>>,
    },
}

impl From<PathBuf> for AddOpt {
    fn from(path: PathBuf) -> Self {
        AddOpt::File(path)
    }
}

pub struct UnixfsAdd {
    core: Option<Either<Ipfs, Repo>>,
    opt: Option<AddOpt>,
    span: Span,
    chunk: Chunker,
    pin: bool,
    provide: bool,
    wrap: bool,
    stream: AddStreamState,
}

enum AddStreamState {
    None,
    Pending {
        stream: BoxStream<'static, UnixfsStatus>,
    },
    Done,
}

impl UnixfsAdd {
    pub fn with_ipfs(ipfs: &Ipfs, opt: impl Into<AddOpt>) -> Self {
        Self::with_either(Either::Left(ipfs.clone()), opt)
    }

    pub fn with_repo(repo: &Repo, opt: impl Into<AddOpt>) -> Self {
        Self::with_either(Either::Right(repo.clone()), opt)
    }

    fn with_either(core: Either<Ipfs, Repo>, opt: impl Into<AddOpt>) -> Self {
        let opt = opt.into();
        Self {
            core: Some(core),
            opt: Some(opt),
            span: Span::current(),
            chunk: Chunker::Size(256 * 1024),
            pin: true,
            provide: false,
            wrap: false,
            stream: AddStreamState::None,
        }
    }

    pub fn span(mut self, span: Span) -> Self {
        self.span = span;
        self
    }

    pub fn chunk(mut self, chunk: Chunker) -> Self {
        self.chunk = chunk;
        self
    }

    pub fn pin(mut self, pin: bool) -> Self {
        self.pin = pin;
        self
    }

    pub fn provide(mut self) -> Self {
        self.provide = true;
        self
    }

    pub fn wrap(mut self) -> Self {
        self.wrap = true;
        self
    }
}

impl Stream for UnixfsAdd {
    type Item = UnixfsStatus;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        loop {
            match &mut self.stream {
                AddStreamState::None => {
                    let (ipfs, repo) = match self.core.take().expect("ipfs or repo is used") {
                        Either::Left(ipfs) => {
                            let repo = ipfs.repo().clone();
                            let ipfs = ipfs.clone();
                            (Some(ipfs), repo)
                        }
                        Either::Right(repo) => (None, repo.clone()),
                    };
                    let option = self.opt.take().expect("option already constructed");
                    let chunk = self.chunk;
                    let pin = self.pin;
                    let provide = self.provide;
                    let wrap = self.wrap;

                    let stream = async_stream::stream! {

                        let mut written = 0;

                        let (name, total_size, mut stream) = match option {
                            AddOpt::File(path) => match tokio::fs::File::open(path.clone())
                                .and_then(|file| async move {
                                    let size = file.metadata().await?.len() as usize;

                                    let stream = ReaderStream::new(file);

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
                            .with_chunker(chunk)
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

                        if wrap {
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

                        if pin {
                            if let Ok(false) = repo.is_pinned(&cid).await {
                                if let Err(e) = repo.insert_pin(&cid, true, true).await {
                                    error!("Unable to pin {cid}: {e}");
                                }
                            }
                        }

                        if provide {
                            if let Some(ipfs) = ipfs {
                                if let Err(e) = ipfs.provide(cid).await {
                                    error!("Unable to provide {cid}: {e}");
                                }
                            }
                        }


                        yield UnixfsStatus::CompletedStatus { path, written, total_size }
                    };

                    self.stream = AddStreamState::Pending {
                        stream: stream.boxed(),
                    };
                }
                AddStreamState::Pending { stream } => {
                    match futures::ready!(stream.poll_next_unpin(cx)) {
                        Some(item) => {
                            if matches!(
                                item,
                                UnixfsStatus::FailedStatus { .. }
                                    | UnixfsStatus::CompletedStatus { .. }
                            ) {
                                self.stream = AddStreamState::Done;
                            }
                            return Poll::Ready(Some(item));
                        }
                        None => {
                            self.stream = AddStreamState::Done;
                            return Poll::Ready(None);
                        }
                    }
                }
                AddStreamState::Done => return Poll::Ready(None),
            }
        }
    }
}

impl std::future::IntoFuture for UnixfsAdd {
    type Output = Result<IpfsPath, anyhow::Error>;

    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(mut self) -> Self::IntoFuture {
        let span = self.span.clone();
        async move {
            while let Some(status) = self.next().await {
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
        .instrument(span)
        .boxed()
    }
}
