use ipld_core::ipld;

use rust_ipfs::UninitializedIpfsDefault as UninitializedIpfs;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let node_a = UninitializedIpfs::new()
        .with_default()
        .add_listening_addr("/ip4/0.0.0.0/tcp/0".parse()?)
        .start()
        .await?;
    let node_b = UninitializedIpfs::new()
        .with_default()
        .add_listening_addr("/ip4/0.0.0.0/tcp/0".parse()?)
        .start()
        .await?;

    let peer_id = node_a.keypair().public().to_peer_id();
    let peer_id_b = node_b.keypair().public().to_peer_id();

    println!("Our Node (A): {peer_id}");
    println!("Their Node (B): {peer_id_b}");

    let addrs = node_a.listening_addresses().await?;

    node_b.add_peer((peer_id, addrs)).await?;

    let block_a = ipld!({
        "name": "alice",
        "age": 99,
    });

    let cid = node_a.put_dag(&block_a).await?;

    let block_b = node_b.get_dag(cid).await?;

    assert_eq!(block_b, block_a);

    println!("Block from node A: {block_b:?}");

    node_a.exit_daemon().await;
    node_b.exit_daemon().await;
    Ok(())
}
