use ipfs::{Ipfs, IpfsOptions, TestTypes, UninitializedIpfs, PublicKey};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    // Initialize the repo and start a daemon
    let opts = IpfsOptions::default();
    let ipfs: Ipfs<TestTypes> = UninitializedIpfs::new(opts).spawn_start().await?;
    
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    let (key, addresses) = ipfs.identity().await?;
    if let PublicKey::Ed25519(publickey) = &key {
        println!("Public Key: {}", bs58::encode(publickey.encode()).into_string());
    }
    
    println!("PeerID: {}", key.to_peer_id());

    for address in addresses {
        println!("Listening on: {}", address);
    }

    ipfs.exit_daemon().await;
    Ok(())
}
