use std::path::PathBuf;

use clap::Parser;
use futures::StreamExt;
use rust_ipfs::UninitializedIpfsNoop as UninitializedIpfs;
use rust_ipfs::{unixfs::UnixfsStatus, Ipfs};

#[derive(Debug, Parser)]
#[clap(name = "unixfs-add")]
struct Opt {
    file: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opt = Opt::parse();

    tracing_subscriber::fmt::init();

    let ipfs: Ipfs = UninitializedIpfs::new()
        .with_default()
        .add_listening_addr("/ip4/0.0.0.0/tcp/0".parse()?)
        .with_mdns()
        .start()
        .await?;

    let mut stream = ipfs.add_unixfs(opt.file);

    while let Some(status) = stream.next().await {
        match status {
            UnixfsStatus::ProgressStatus {
                written,
                total_size,
            } => match total_size {
                Some(size) => println!("{written} out of {size} stored"),
                None => println!("{written} been stored"),
            },
            UnixfsStatus::FailedStatus {
                written,
                total_size,
                error,
            } => {
                match total_size {
                    Some(size) => println!("failed with {written} out of {size} stored"),
                    None => println!("failed with {written} stored"),
                }

                anyhow::bail!(error);
            }
            UnixfsStatus::CompletedStatus { path, written, .. } => {
                println!("{written} been stored with path {path}");
            }
        }
    }

    Ok(())
}
