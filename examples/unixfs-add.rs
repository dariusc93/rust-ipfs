use std::path::PathBuf;

use clap::Parser;
use futures::{pin_mut, StreamExt};
use ipfs::{
    unixfs::ll::{
        dir::builder::{BufferingTreeBuilder, TreeOptions},
        file::adder::FileAdder,
    },
    Block,
};
use ipfs::{Ipfs, IpfsOptions, IpfsPath, TestTypes, UninitializedIpfs};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    task,
};

#[derive(Debug, Parser)]
#[clap(name = "unixfs-add")]
struct Opt {
    file: PathBuf,
    #[clap(long)]
    dest: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opt = Opt::parse();

    tracing_subscriber::fmt::init();

    let opts = IpfsOptions::inmemory_with_generated_keys();

    let (ipfs, fut): (Ipfs<TestTypes>, _) = UninitializedIpfs::new(opts).start().await?;
    task::spawn(fut);

    let mut adder = FileAdder::default();

    let file = tokio::fs::File::open(&opt.file).await?;

    let mut file_buf = BufReader::with_capacity(adder.size_hint(), file);

    let mut written = 0;
    let mut last_cid = None;

    {
        let ipfs = ipfs.clone();
        loop {
            match file_buf.fill_buf().await? {
                buffer if buffer.is_empty() => {
                    let blocks = adder.finish();
                    for (cid, block) in blocks {
                        let block = Block::new(cid, block)?;
                        let cid = ipfs.put_block(block).await?;
                        last_cid = Some(cid);
                    }
                    break;
                }
                buffer => {
                    let mut total = 0;

                    while total < buffer.len() {
                        let (blocks, consumed) = adder.push(&buffer[total..]);
                        for (cid, block) in blocks {
                            let block = Block::new(cid, block)?;
                            let _cid = ipfs.put_block(block).await?;
                            // last_cid = Some(_cid);
                        }
                        total += consumed;
                        written += consumed;
                    }
                    file_buf.consume(total);
                }
            }
        }
    }

    let last_cid = last_cid.unwrap();

    let mut tree_opts = TreeOptions::default();
    tree_opts.wrap_with_directory();
    let mut tree = BufferingTreeBuilder::new(tree_opts);

    let filename = opt.file.file_name().unwrap().to_string_lossy();

    tree.put_link(&filename, last_cid, written as u64)?;

    let mut iter = tree.build();
    let mut last_cid = None;

    while let Some(node) = iter.next_borrowed() {
        let node = node?;
        let block = Block::new(*node.cid, node.block.into())?;

        ipfs.put_block(block).await?;

        last_cid = Some(*node.cid);
    }

    let last_cid = last_cid.expect("Last cid is always provided");

    println!("File located at /ipfs/{last_cid}/{filename}");
    //Fetching file using cat_unixfs
    let stream = ipfs
        .cat_unixfs(IpfsPath::from(last_cid).sub_path(&filename)?, None)
        .await?;

    pin_mut!(stream);

    if let Some(dest) = opt.dest {
        let mut file = tokio::fs::File::create(&dest).await?;
        while let Some(data) = stream.next().await {
            let bytes = data?;
            file.write_all(&bytes).await?;
            file.flush().await?;
        }

        println!("Written file to {}", dest.display());
    } else {
        let mut stdout = tokio::io::stdout();

        while let Some(data) = stream.next().await {
            let bytes = data?;
            stdout.write_all(&bytes).await?;
        }
    }

    Ok(())
}
