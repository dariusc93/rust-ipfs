use clap::Parser;
use futures::StreamExt;
use libipld::multibase::{self, Base};
use rust_ipfs::Ipfs;
use rust_ipfs::UninitializedIpfsNoop as UninitializedIpfs;

#[derive(Debug, clap::Parser)]
#[clap(name = "ipns")]
struct Opt {
    key: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let opt = Opt::parse();

    // Initialize the repo and start a daemon
    let ipfs: Ipfs = UninitializedIpfs::new()
        .enable_mdns()
        .default_record_key_validator()
        .start()
        .await?;

    ipfs.default_bootstrap().await?;

    let mut stream = ipfs.dht_get(&opt.key).await?;

    while let Some(record) = stream.next().await {
        //TODO: Look into a better conversion
        let key = &record.key.as_ref()[6..];
        let encoded_key = &multibase::encode(Base::Base58Btc, key)[1..];
        println!("Record Key: {encoded_key}");
        println!("Publisher: {:?}", record.publisher);
        println!("Expires: {:?}", record.expires);
        println!("Record Value Size: {}", record.value.len());
        println!();
    }

    Ok(())
}
