use libipld::ipld;
use libp2p::swarm::dial_opts::DialOpts;
use rust_ipfs::{IpfsPath, UninitializedIpfs};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let node_a = UninitializedIpfs::new().start().await?;
    let node_b = UninitializedIpfs::new().start().await?;

    let (peer_id, addrs) = node_a
        .identity(None)
        .await
        .map(|i| (i.peer_id, i.listen_addrs))?;

    let opts = DialOpts::peer_id(peer_id).addresses(addrs).build();
    node_b.connect(opts).await?;

    let block_a = ipld!({
        "name": "alice",
        "age": 99,
    });

    let cid = node_a.put_dag(block_a.clone()).await?;

    let block_b = node_b.get_dag(IpfsPath::from(cid)).await?;

    assert_eq!(block_b, block_a); 

    node_a.exit_daemon().await;
    node_b.exit_daemon().await;
    Ok(())
}
