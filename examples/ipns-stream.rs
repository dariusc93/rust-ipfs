use clap::Parser;
use futures::StreamExt;
// use libipld::multibase::{self, Base};
use libipld::{Cid, Multihash};
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

    let cid = resolve(&ipfs, &opt.key).await?;

    println!("{} resolves to {}", opt.key, cid);

    Ok(())
}

async fn resolve(ipfs: &Ipfs, link: &str) -> anyhow::Result<Cid> {
    let stream = ipfs.dht_get(link).await?;

    let mut records = stream
        .filter_map(|record| async move {
            let key = &record.key.as_ref()[6..];
            let record = rust_ipns::Record::decode(&record.value).ok()?;
            let mh = Multihash::from_bytes(key).ok()?;
            let cid = libipld::Cid::new_v1(0x72, mh);
            record.verify(cid).ok()?;
            Some(record)
        })
        .collect::<Vec<_>>()
        .await;

    if records.is_empty() {
        panic!()
    }

    records.sort_by_key(|record| record.sequence());

    let record = records.last().ok_or(anyhow::anyhow!(""))?;

    let cid = record.value()?;

    Ok(cid)
}
