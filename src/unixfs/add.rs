use std::{borrow::Borrow, path::Path};

use futures::{stream::BoxStream, StreamExt};
use ipfs_bitswap::Block;
use ipfs_unixfs::file::adder::{Chunker, FileAdderBuilder};
use tokio_util::io::ReaderStream;

use crate::{Ipfs, IpfsPath, IpfsTypes};

pub struct AddOption {
    pub chunk: Option<Chunker>,
}

impl Default for AddOption {
    fn default() -> Self {
        Self {
            chunk: Some(Chunker::Size(1024 * 1024)),
        }
    }
}

pub async fn add_file<'a, Types, MaybeOwned, P: AsRef<Path>>(
    ipfs: MaybeOwned,
    path: P,
    opt: Option<AddOption>,
) -> anyhow::Result<IpfsPath>
where
    Types: IpfsTypes,
    MaybeOwned: Borrow<Ipfs<Types>> + Send + 'a,
{
    let path = path.as_ref();

    let file = tokio::fs::File::open(path).await?;

    let stream = ReaderStream::new(file)
        .filter_map(|x| async { x.ok() })
        .map(|x| x.into());

    add(ipfs, stream.boxed(), opt).await
}

pub async fn add<'a, Types, MaybeOwned>(
    ipfs: MaybeOwned,
    mut stream: BoxStream<'a, Vec<u8>>,
    opt: Option<AddOption>,
) -> anyhow::Result<IpfsPath>
where
    Types: IpfsTypes,
    MaybeOwned: Borrow<Ipfs<Types>> + Send + 'a,
{
    let ipfs = ipfs.borrow();

    let mut adder = FileAdderBuilder::default()
        .with_chunker(opt.map(|o| o.chunk.unwrap_or_default()).unwrap_or_default())
        .build();

    let mut last_cid = None;

    while let Some(buffer) = stream.next().await {
        let mut total = 0;

        while total < buffer.len() {
            let (blocks, consumed) = adder.push(&buffer[total..]);
            for (cid, block) in blocks {
                let block = Block::new(cid, block)?;
                let _cid = ipfs.put_block(block).await?;
            }
            total += consumed;
        }
    }

    let blocks = adder.finish();
    for (cid, block) in blocks {
        let block = Block::new(cid, block)?;
        let cid = ipfs.put_block(block).await?;
        last_cid = Some(cid);
    }

    let cid = last_cid.ok_or_else(|| anyhow::anyhow!("Cid was never set"))?;

    Ok(IpfsPath::from(cid))
}
