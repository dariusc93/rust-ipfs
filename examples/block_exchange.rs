use libipld::ipld;
use rust_ipfs::IpfsPath;

use rust_ipfs::UninitializedIpfsNoop as UninitializedIpfs;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let node_a = UninitializedIpfs::new().with_default().start().await?;
    let node_b = UninitializedIpfs::new().with_default().start().await?;

    let peer_id = node_a.keypair().map(|kp| kp.public().to_peer_id())?;

    let addrs = node_a.listening_addresses().await?;

    for addr in addrs {
        node_b.add_peer(peer_id, addr).await?;
    }

    node_b.connect(peer_id).await?;

    let block_a = ipld!({
        "name": "alice",
        "age": 99,
    });

    let cid = node_a.put_dag(block_a.clone()).await?;

    let block_b = node_b.get_dag(IpfsPath::from(cid)).await?;

    assert_eq!(block_b, block_a);

    println!("Block from node A: {block_b:?}");

    node_a.exit_daemon().await;
    node_b.exit_daemon().await;
    Ok(())
}
