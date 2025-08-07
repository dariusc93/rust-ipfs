use clap::Parser;
use rust_ipfs::p2p::MultiaddrExt;
use rust_ipfs::UninitializedIpfsDefault as UninitializedIpfs;
use rust_ipfs::{Ipfs, Multiaddr};

#[derive(Debug, Parser)]
#[clap(name = "rendezvous-client")]
struct Opt {
    rendezvous_server: Multiaddr,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opt = Opt::parse();

    tracing_subscriber::fmt::init();

    let ipfs: Ipfs = UninitializedIpfs::new()
        .with_rendezvous_client()
        .enable_tcp()
        .add_listening_addr("/ip4/0.0.0.0/tcp/0".parse()?)
        .start()
        .await?;

    let rendezvous_peer_id = opt
        .rendezvous_server
        .peer_id()
        .expect("A peer id apart of the multiaddr");

    ipfs.connect(opt.rendezvous_server).await?;

    ipfs.rendezvous_register_namespace("rust-ipfs", None, rendezvous_peer_id)
        .await?;

    let list = ipfs
        .rendezvous_namespace_discovery("rust-ipfs", None, rendezvous_peer_id)
        .await?;

    for (peer, addrs) in list {
        println!("Discovered peer {peer} with the following addresses");
        for addr in addrs {
            println!("- {addr}");
        }
        println!();
    }

    Ok(())
}
