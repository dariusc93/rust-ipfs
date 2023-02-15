use futures::join;
use rust_ipfs::{Ipfs, IpfsPath, TestTypes, UninitializedIpfs};
use libipld::ipld;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    // Initialize the repo and start a daemon
    let ipfs: Ipfs<TestTypes> = UninitializedIpfs::new().start().await?;

    // Create a DAG
    let f1 = ipfs.put_dag(ipld!("block1"));
    let f2 = ipfs.put_dag(ipld!("block2"));
    let (res1, res2) = join!(f1, f2);
    let root = ipld!([res1?, res2?]);
    let cid = ipfs.put_dag(root).await?;
    let path = IpfsPath::from(cid);

    // Query the DAG
    let path1 = path.sub_path("0")?;
    let path2 = path.sub_path("1")?;
    let f1 = ipfs.get_dag(path1);
    let f2 = ipfs.get_dag(path2);
    let (res1, res2) = join!(f1, f2);
    println!("Received block with contents: {:?}", res1?);
    println!("Received block with contents: {:?}", res2?);

    // Exit
    ipfs.exit_daemon().await;
    Ok(())
}
