use futures::join;
use libipld::ipld;
use rust_ipfs::dag::IpldDag;
use rust_ipfs::repo::Repo;
use rust_ipfs::IpfsPath;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    // Initialize the repo
    let repo = Repo::new_memory();
    let dag = IpldDag::from(repo.clone());

    // Create a DAG
    let f1 = dag.put_dag(ipld!("block1"));
    let f2 = dag.put_dag(ipld!("block2"));
    let (res1, res2) = join!(f1, f2);
    let root = ipld!([res1?, res2?]);
    let cid = dag.put_dag(root).await?;
    let path = IpfsPath::from(cid);

    // Query the DAG
    let path1 = path.sub_path("0")?;
    let path2 = path.sub_path("1")?;
    let f1 = dag.get_dag(path1);
    let f2 = dag.get_dag(path2);
    let (res1, res2) = join!(f1, f2);
    println!("Received block with contents: {:?}", res1?);
    println!("Received block with contents: {:?}", res2?);

    repo.shutdown();
    Ok(())
}
