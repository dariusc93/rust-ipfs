use clap::Parser;
use rust_ipfs::{p2p::PeerInfo, Ipfs, IpfsOptions, PublicKey, TestTypes, UninitializedIpfs};
use tokio::sync::Notify;

#[derive(Debug, Parser)]
#[clap(name = "local-node")]
struct Opt {
    #[clap(long)]
    portmapping: bool,
    #[clap(long)]
    bootstrap: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opt = Opt::parse();
    tracing_subscriber::fmt::init();

    // Initialize the repo and start a daemon
    let opts = IpfsOptions {
        port_mapping: opt.portmapping,
        ..Default::default()
    };

    let ipfs: Ipfs<TestTypes> = UninitializedIpfs::with_opt(opts).start().await?;


    if opt.bootstrap {
        ipfs.default_bootstrap().await?;
        ipfs.bootstrap().await?;
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
    
    let PeerInfo {
        public_key: key,
        listen_addrs: addresses,
        ..
    } = ipfs.identity(None).await?;

    if let PublicKey::Ed25519(publickey) = &key {
        println!(
            "Public Key: {}",
            bs58::encode(publickey.encode()).into_string()
        );
    }

    println!("PeerID: {}", key.to_peer_id());

    for address in addresses {
        println!("Listening on: {address}");
    }

    // Used to wait until the process is terminated instead of creating a loop
    Notify::new().notified().await;

    ipfs.exit_daemon().await;
    Ok(())
}
