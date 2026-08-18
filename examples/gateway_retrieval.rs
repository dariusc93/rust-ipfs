use std::time::Duration;

use clap::Parser;
use ipld_core::cid::Cid;
use rust_ipfs::builder::DefaultIpfsBuilder as IpfsBuilder;

#[derive(Parser)]
#[clap(name = "gateway_retrieval")]
struct Opt {
    /// CID to fetch; defaults to a well-known object on the public network.
    #[clap(long, default_value = "QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG")]
    cid: Cid,
    /// Trustless gateway base URLs (repeatable); empty uses the built-in defaults.
    #[clap(long = "gateway")]
    gateways: Vec<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let opt = Opt::parse();

    let ipfs = IpfsBuilder::new()
        .with_default()
        .enable_tcp()
        .enable_gateway_retrieval(opt.gateways)
        .start()
        .await?;

    println!("fetching {}...", opt.cid);

    let block = ipfs
        .get_block(opt.cid)
        .timeout(Duration::from_secs(30))
        .await?;

    println!("retrieved {} ({} bytes)", block.cid(), block.data().len());

    ipfs.exit_daemon().await;
    Ok(())
}
