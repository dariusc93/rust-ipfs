use std::time::Duration;

use clap::Parser;
use ipld_core::cid::Cid;
use rust_ipfs::builder::DefaultIpfsBuilder as IpfsBuilder;

#[derive(Parser)]
#[clap(name = "delegated_routing")]
struct Opt {
    /// CID to fetch via providers discovered over delegated routing.
    #[clap(long, default_value = "QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG")]
    cid: Cid,
    /// Delegated routing endpoint base URLs
    #[clap(long = "router")]
    routers: Vec<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let opt = Opt::parse();

    let ipfs = IpfsBuilder::new()
        .with_default()
        .enable_tcp()
        .enable_quic()
        .enable_dns()
        .enable_delegated_routing(opt.routers)
        .start()
        .await?;

    println!("routers: {:?}", ipfs.list_routers());
    println!("fetching {}...", opt.cid);

    let block = ipfs
        .get_block(opt.cid)
        .timeout(Duration::from_secs(60))
        .await?;

    println!("retrieved {} ({} bytes)", block.cid(), block.data().len());

    ipfs.exit_daemon().await;
    Ok(())
}
