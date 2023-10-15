use std::path::PathBuf;

use clap::Parser;
use futures::StreamExt;

use rust_ipfs::UninitializedIpfsNoop as UninitializedIpfs;
use rust_ipfs::{unixfs::UnixfsStatus, Ipfs, IpfsPath, Multiaddr};

#[derive(Debug, Parser)]
#[clap(name = "unixfs-get")]
struct Opt {
    path: IpfsPath,
    dest: PathBuf,
    #[clap(long)]
    connect: Option<Multiaddr>,
    #[clap(long)]
    default_bootstrap: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opt = Opt::parse();

    tracing_subscriber::fmt::init();

    let ipfs: Ipfs = UninitializedIpfs::new()
        .with_default()
        .with_mdns()
        .start()
        .await?;

    if opt.default_bootstrap {
        ipfs.default_bootstrap().await?;
    }

    if let Some(addr) = opt.connect {
        ipfs.connect(addr).await?;
    }

    let dest = opt.dest;

    let mut stream = ipfs.get_unixfs(opt.path, &dest).await?;

    while let Some(status) = stream.next().await {
        match status {
            UnixfsStatus::ProgressStatus {
                written,
                total_size,
            } => match total_size {
                Some(size) => println!("{written} out of {size} written"),
                None => println!("{written} been written"),
            },
            UnixfsStatus::FailedStatus {
                written,
                total_size,
                error,
            } => {
                match total_size {
                    Some(size) => println!("failed with {written} out of {size} written"),
                    None => println!("failed with {written} written"),
                }

                if let Some(error) = error {
                    anyhow::bail!(error);
                } else {
                    anyhow::bail!("Unknown error while writting to disk");
                }
            }
            UnixfsStatus::CompletedStatus { written, .. } => {
                let path = dest;
                println!("{written} been written successfully to {}", path.display());
                break;
            }
        }
    }

    Ok(())
}
