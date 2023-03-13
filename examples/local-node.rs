use rust_ipfs::{p2p::PeerInfo, Ipfs, UninitializedIpfs};
use tokio::sync::Notify;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    // Initialize the repo and start a daemon
    let ipfs: Ipfs = UninitializedIpfs::new()
        .enable_mdns()
        .enable_relay(true)
        .enable_relay_server(None)
        .enable_upnp()
        .start()
        .await?;

    ipfs.default_bootstrap().await?;
    ipfs.bootstrap().await?;

    // Used to give more time after bootstrapping
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    let PeerInfo {
        public_key: key,
        listen_addrs: addresses,
        ..
    } = ipfs.identity(None).await?;

    if let Some(publickey) = key.clone().into_ed25519() {
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
