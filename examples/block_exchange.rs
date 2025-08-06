use ipld_core::ipld;

use rust_ipfs::UninitializedIpfsDefault as UninitializedIpfs;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let node_a = UninitializedIpfs::new()
        .with_default()
        .enable_memory_transport()
        .start()
        .await?;

    node_a.add_listening_address("/memory/0".parse()?).await?;

    let node_b = UninitializedIpfs::new()
        .with_default()
        .enable_memory_transport()
        .start()
        .await?;

    node_b.add_listening_address("/memory/0".parse()?).await?;

    // tokio::time::sleep(tokio::time::Duration::from_millis(1500)).await;

    let peer_id = node_a.keypair().public().to_peer_id();
    let peer_id_b = node_b.keypair().public().to_peer_id();

    let addrs = node_a.listening_addresses().await?;

    node_b.add_peer((peer_id, addrs)).await?;

    println!("Our Node (A): {peer_id}");
    println!("Their Node (B): {peer_id_b}");

    let block_a = ipld!({
        "name": "alice",
        "age": 99,
    });

    let cid = node_a.put_dag(&block_a).await?;

    println!("Block A CID: {cid}");

    let block_b = node_b.get_dag(cid).await?;

    assert_eq!(block_b, block_a);

    println!("Block from node A: {block_b:?}");

    node_a.exit_daemon().await;
    node_b.exit_daemon().await;
    Ok(())
}
