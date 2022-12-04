use std::{path::PathBuf, convert::TryInto};

use clap::Parser;
use futures::StreamExt;

use ipfs::{unixfs::UnixfsStatus, Ipfs, IpfsOptions, TestTypes, UninitializedIpfs, IpfsPath, Multiaddr};

#[derive(Debug, Parser)]
#[clap(name = "unixfs-add")]
struct Opt {
    path: IpfsPath,
    dest: PathBuf,
    #[clap(long)]
    connect: Option<Multiaddr>
}


#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opt = Opt::parse();

    tracing_subscriber::fmt::init();

    let opts = IpfsOptions {
        mdns: true,
        ..Default::default()
    };

    let ipfs: Ipfs<TestTypes> = UninitializedIpfs::new(opts).spawn_start().await?;

    ipfs.default_bootstrap().await?;

    if let Some(addr) = opt.connect {
        ipfs.connect(addr.try_into()?).await?;
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