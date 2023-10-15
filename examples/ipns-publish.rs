#[tokio::main]
async fn main() -> anyhow::Result<()> {
    use rust_ipfs::Ipfs;
    use rust_ipfs::IpfsPath;
    use rust_ipfs::UninitializedIpfsNoop as UninitializedIpfs;
    tracing_subscriber::fmt::init();

    // Initialize the repo and start a daemon
    let ipfs: Ipfs = UninitializedIpfs::new()
        .with_mdns()
        .with_relay(true)
        .with_identify(None)
        .with_autonat()
        .with_bitswap(None)
        .with_kademlia()
        .with_ping(None)
        .default_record_key_validator()
        .start()
        .await?;

    ipfs.default_bootstrap().await?;

    ipfs.bootstrap().await?;

    let block_a = libipld::ipld!({
        "name": "alice",
        "age": 99,
    });

    let cid = ipfs.put_dag(block_a.clone()).await?;

    let ipfs_path = IpfsPath::from(cid);

    let path = ipfs.publish_ipns(&ipfs_path).await?;

    println!("{ipfs_path} been published to {path}");

    tokio::signal::ctrl_c().await?;

    Ok(())
}
