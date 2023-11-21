use libipld::ipld;
use rust_ipfs::dag::IpldDag;
use rust_ipfs::repo::Repo;
use rust_ipfs::IpfsPath;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    // Initialize the repo
    let repo = Repo::new_memory(None);
    let dag = IpldDag::from(repo.clone());

    let cid1: libipld::cid::CidGeneric<64> = dag.put_dag(ipld!("block1")).await?;
    let cid2 = dag.put_dag(ipld!("block2")).await?;
    let root = ipld!([cid1, cid2]);
    let cid = dag.put_dag(root).await?;
    let path = IpfsPath::from(cid);

    // Query the DAG
    let path1 = path.sub_path("0")?;
    let path2 = path.sub_path("1")?;
    let block1 = dag.get_dag(path1).await?;
    let block2 = dag.get_dag(path2).await?;
    println!("Received block with contents: {:?}", block1);
    println!("Received block with contents: {:?}", block2);

    repo.shutdown();
    Ok(())
}
