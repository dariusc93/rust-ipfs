use std::time::Duration;

use clap::Parser;
use libp2p::Multiaddr;
use rust_ipfs::p2p::MultiaddrExt;
use rust_ipfs::Ipfs;
use rust_ipfs::UninitializedIpfsNoop as UninitializedIpfs;

#[derive(Debug, Parser)]
#[clap(name = "relay-client")]
struct Opt {
    relay_addr: Multiaddr,
    #[clap(long)]
    connect: Option<Multiaddr>
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opt = Opt::parse();

    tracing_subscriber::fmt::init();

    let ipfs: Ipfs = UninitializedIpfs::new()
        .with_default()
        .with_relay(true)
        .add_listening_addr("/ip4/127.0.0.1/tcp/0".parse()?)
        .listen_as_external_addr()
        .start()
        .await?;

    let relay_peer_id = opt
        .relay_addr
        .peer_id()
        .expect("A peer id apart of the multiaddr");

    let relay_addr = opt.relay_addr.address();

    ipfs.add_relay(relay_peer_id, relay_addr).await?;

    ipfs.enable_relay(None).await?;

    let addresses = ipfs.external_addresses().await?;

    for addr in addresses {
        println!("- {addr}");
        println!();
    }

    if let Some(addr) = opt.connect {
        if let Err(e) = ipfs.connect(addr).await {
            println!("Error connecting: {e}");
        }
    }

    tokio::signal::ctrl_c().await?;

    Ok(())
}
