use clap::Parser;
use libp2p::Multiaddr;
use rust_ipfs::p2p::MultiaddrExt;
use rust_ipfs::Ipfs;
use rust_ipfs::UninitializedIpfsNoop as UninitializedIpfs;

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
        .enable_rendezvous_client()
        .listen_as_external_addr()
        .start()
        .await?;

    let rendezvous_peer_id = opt
        .rendezvous_server
        .peer_id()
        .expect("A peer id apart of the multiaddr");

    ipfs.connect(opt.rendezvous_server).await?;

    ipfs.rendezvous_register_namespace("rust-ipfs".into(), None, rendezvous_peer_id)
        .await?;

    ipfs.rendezvous_discovery_namespace(Some("rust-ipfs".into()), false, None, rendezvous_peer_id)
        .await?;

    tokio::signal::ctrl_c().await?;

    Ok(())
}
