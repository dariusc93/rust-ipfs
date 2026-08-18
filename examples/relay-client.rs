use clap::Parser;
use connexa::prelude::ConnexaSwarmEvent;
use futures::StreamExt;
use rust_ipfs::p2p::MultiaddrExt;
use rust_ipfs::{Ipfs, Multiaddr};
use tokio::task::yield_now;

use rust_ipfs::Keypair;
use rust_ipfs::builder::DefaultIpfsBuilder;

#[derive(Debug, Parser)]
#[clap(name = "relay-client")]
struct Opt {
    relay: Vec<Multiaddr>,
    #[clap(long)]
    connect: Option<Multiaddr>,
    #[clap(long)]
    bootstrap: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opt = Opt::parse();

    // tracing_subscriber::fmt::init();

    let keypair = Keypair::generate_ed25519();

    // Initialize the repo and start a daemon
    let ipfs: Ipfs = DefaultIpfsBuilder::with_keypair(&keypair)?
        .with_default()
        .enable_tcp()
        .enable_quic()
        .enable_dns()
        .with_relay(true)
        .with_autorelay()
        .fd_limit(rust_ipfs::FDLimit::Max)
        .start()
        .await?;

    for mut addr in opt.relay.clone() {
        let peer_id = addr
            .extract_peer_id()
            .expect("peerid required on multiaddr");

        if let Err(e) = ipfs.add_static_relay(peer_id, addr.clone()).await {
            println!("error adding relay {addr}: {e}");
        }
    }

    if opt.bootstrap {
        ipfs.default_bootstrap().await?;
        yield_now().await;
    }

    ipfs.enable_autorelay().await?;

    if let Some(addr) = opt.connect {
        ipfs.connect(addr).await?;
    }

    let mut events = ipfs.swarm_events().await?;

    while let Some(event) = events.next().await {
        match event {
            ConnexaSwarmEvent::NewExternalAddr { address } => {
                println!("New external address: {address}");
            }
            ConnexaSwarmEvent::ExternalAddrExpired { address } => {
                println!("External address expired: {address}");
            }
            _ => {}
        }
    }

    tokio::signal::ctrl_c().await?;

    ipfs.exit_daemon().await;
    Ok(())
}
