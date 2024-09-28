use ipld_core::ipld;
use rust_ipfs::{Ipfs, IpfsPath};

use rust_ipfs::UninitializedIpfsDefault as UninitializedIpfs;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    // Initialize the repo and start a daemon
    let ipfs: Ipfs = UninitializedIpfs::new().start().await?;

    // Create a DAG
    let cid1 = ipfs.put_dag(ipld!("block1")).await?;
    let cid2 = ipfs.put_dag(ipld!("block2")).await?;
    let root = ipld!([cid1, cid2]);
    let cid = ipfs.put_dag(root).await?;
    let path = IpfsPath::from(cid);

    // Query the DAG
    let path1 = path.sub_path("0")?;
    let path2 = path.sub_path("1")?;
    let block1 = ipfs.get_dag(path1).await?;
    let block2 = ipfs.get_dag(path2).await?;
    println!("Received block with contents: {:?}", block1);
    println!("Received block with contents: {:?}", block2);

    // Exit
    ipfs.exit_daemon().await;
    Ok(())
}
