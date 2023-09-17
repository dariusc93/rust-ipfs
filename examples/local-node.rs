use rust_ipfs::{p2p::PeerInfo, Ipfs};

use rust_ipfs::UninitializedIpfsNoop as UninitializedIpfs;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    // Initialize the repo and start a daemon
    let ipfs: Ipfs = UninitializedIpfs::new()
        .enable_mdns()
        .enable_relay(true)
        .enable_relay_server(None)
        .enable_upnp()
        .enable_rendezvous_server()
        .listen_as_external_addr()
        .fd_limit(rust_ipfs::FDLimit::Max)
        .start()
        .await?;

    ipfs.default_bootstrap().await?;
    ipfs.bootstrap().await?;

    let PeerInfo {
        public_key: key,
        listen_addrs: addresses,
        ..
    } = ipfs.identity(None).await?;

    if let Ok(publickey) = key.clone().try_into_ed25519() {
        println!(
            "Public Key: {}",
            bs58::encode(publickey.to_bytes()).into_string()
        );
    }

    println!("PeerID: {}", key.to_peer_id());

    for address in addresses {
        println!("Listening on: {address}");
    }

    // Used to wait until the process is terminated instead of creating a loop
    tokio::signal::ctrl_c().await?;

    ipfs.exit_daemon().await;
    Ok(())
}
