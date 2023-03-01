use std::path::Path;

use crate::Block;
use futures::{stream::BoxStream, Stream, StreamExt};
use rust_unixfs::file::adder::{Chunker, FileAdderBuilder};
use tokio_util::io::ReaderStream;

use crate::{Ipfs, IpfsPath};

use super::UnixfsStatus;

#[derive(Clone, Debug)]
pub struct AddOption {
    pub chunk: Option<Chunker>,
    pub pin: bool,
    pub provide: bool,
}

impl Default for AddOption {
    fn default() -> Self {
        Self {
            chunk: Some(Chunker::Size(1024 * 1024)),
            pin: false,
            provide: false,
        }
    }
}

pub async fn add_file<'a, P: AsRef<Path>>(
    ipfs: &Ipfs,
    path: P,
    opt: Option<AddOption>,
) -> anyhow::Result<BoxStream<'a, UnixfsStatus>>
where
{
    let path = path.as_ref();

    let file = tokio::fs::File::open(path).await?;

    let size = file.metadata().await?.len() as usize;

    let stream = ReaderStream::new(file).map(|x| x.map(|x| x.into()));

    add(ipfs, Some(size), stream.boxed(), opt).await
}

pub async fn add<'a>(
    ipfs: &Ipfs,
    total_size: Option<usize>,
    mut stream: impl Stream<Item = std::result::Result<Vec<u8>, std::io::Error>> + Unpin + Send + 'a,
    opt: Option<AddOption>,
) -> anyhow::Result<BoxStream<'a, UnixfsStatus>> {
    let ipfs = ipfs.clone();
    let stream = async_stream::stream! {
        let mut adder = FileAdderBuilder::default()
            .with_chunker(opt.clone().map(|o| o.chunk.unwrap_or_default()).unwrap_or_default())
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
                    let _cid = match ipfs.put_block(block).await {
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
            let _cid = match ipfs.put_block(block).await {
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

        if let Some(opt) = opt.clone() {
            if opt.pin {
                if let Ok(false) = ipfs.is_pinned(&cid).await {
                    if let Err(_e) = ipfs.insert_pin(&cid, true).await {}
                }
            }
        }

        yield UnixfsStatus::CompletedStatus { path: IpfsPath::from(cid), written, total_size }
    };

    Ok(stream.boxed())
}
