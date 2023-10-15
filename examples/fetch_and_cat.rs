use clap::Parser;
use futures::pin_mut;
use futures::stream::StreamExt;
use rust_ipfs::p2p::PeerInfo;
use rust_ipfs::{Ipfs, IpfsPath, Multiaddr};

use rust_ipfs::UninitializedIpfsNoop as UninitializedIpfs;

use std::process::exit;
use tokio::io::AsyncWriteExt;

#[derive(Debug, Parser)]
#[clap(name = "fetch_and_cat")]
struct Opt {
    path: IpfsPath,
    target: Option<Multiaddr>,
    #[clap(long)]
    default_bootstrappers: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    println!("Usage: fetch_and_cat [--default-bootstrappers] <IPFS_PATH | CID> [MULTIADDR]");
    println!();
    println!(
        "Example will try to find the file by the given IPFS_PATH and print its contents to stdout."
    );
    println!();
    println!("The example has three modes in the order of precedence:");
    println!(
        "1. When --default-bootstrappers is given, use default bootstrappers to find the content"
    );
    println!("2. When IPFS_PATH and MULTIADDR are given, connect to MULTIADDR to get the file");
    println!("3. When only IPFS_PATH is given, wait to be connected to by another ipfs node");
    println!();

    let opt = Opt::parse();

    // Initialize the repo and start a daemon.
    // UninitializedIpfs will handle starting up the repository and return the facade (ipfs::Ipfs)
    let ipfs: Ipfs = UninitializedIpfs::new().with_default().start().await?;

    if opt.default_bootstrappers {
        // applications wishing to find content on the global IPFS swarm should restore the latest
        // bootstrappers which are hopefully updated between releases
        ipfs.default_bootstrap().await?;
    } else if let Some(target) = opt.target {
        ipfs.connect(target).await?;
    } else {
        let PeerInfo {
            listen_addrs: addresses,
            ..
        } = ipfs.identity(None).await?;
        assert!(!addresses.is_empty(), "Zero listening addresses");

        eprintln!("Please connect an ipfs node having {} to:\n", opt.path);

        for address in addresses {
            eprintln!(" - {address}");
        }

        eprintln!();
    }

    // Calling Ipfs::cat_unixfs returns a future of a stream, because the path resolving
    // and the initial block loading will require at least one async call before any actual file
    // content can be *streamed*.
    let stream = ipfs.cat_unixfs(opt.path, None).await.unwrap_or_else(|e| {
        eprintln!("Error: {e}");
        exit(1);
    });

    // The stream needs to be pinned on the stack to be used with StreamExt::next
    pin_mut!(stream);

    let mut stdout = tokio::io::stdout();

    loop {
        // This could be made more performant by polling the stream while writing to stdout.
        match stream.next().await {
            Some(Ok(bytes)) => {
                stdout.write_all(&bytes).await?;
            }
            Some(Err(e)) => {
                eprintln!("Error: {e}");
                exit(1);
            }
            None => break,
        }
    }
    Ok(())
}
