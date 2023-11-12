use rust_ipfs::Ipfs;

use rust_ipfs::UninitializedIpfsNoop as UninitializedIpfs;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    // Initialize the repo and start a daemon
    let ipfs: Ipfs = UninitializedIpfs::new()
        .with_default()
        .with_mdns()
        .with_relay(true)
        .with_relay_server(None)
        .with_upnp()
        .with_rendezvous_server()
        .listen_as_external_addr()
        .fd_limit(rust_ipfs::FDLimit::Max)
        .start()
        .await?;

    ipfs.default_bootstrap().await?;
    ipfs.bootstrap().await?;

    let info = ipfs.identity(None).await?;

    println!("PeerID: {}", info.public_key.to_peer_id());

    for address in info.listen_addrs {
        println!("Listening on: {address}");
    }

    // Used to wait until the process is terminated instead of creating a loop
    tokio::signal::ctrl_c().await?;

    ipfs.exit_daemon().await;
    Ok(())
}
