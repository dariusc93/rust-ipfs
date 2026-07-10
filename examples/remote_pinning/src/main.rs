use clap::Parser;
use rust_ipfs::builder::DefaultIpfsBuilder as IpfsBuilder;
use rust_ipfs::pinning::{AddOptions, ListQuery, Status};

#[derive(Parser)]
#[clap(name = "remote_pinning")]
struct Opt {
    /// Pinning Service API base URL (e.g. https://api.pinata.cloud/psa)
    #[clap(long)]
    endpoint: String,
    /// Bearer token for the service
    #[clap(long)]
    token: String,
    /// Name to tag the pin with
    #[clap(long, default_value = "rust-ipfs-example")]
    name: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let opt = Opt::parse();

    let ipfs = IpfsBuilder::new()
        .with_default()
        .enable_tcp()
        .enable_quic()
        .with_upnp()
        .start()
        .await?;

    ipfs.add_listening_address("/ip4/0.0.0.0/tcp/0".parse()?)
        .await?;

    let cid = ipfs.put_dag("rust-ipfs remote pinning example").await?;
    println!("added content: {cid}");
    if let Err(e) = ipfs.provide(cid).await {
        println!("(could not announce {cid} to the DHT: {e:#})");
    }

    let service = ipfs.remote_pinning(&opt.endpoint, &opt.token);

    println!("requesting pin of {cid}...");
    let requested = service
        .add(
            cid,
            AddOptions {
                name: Some(opt.name.clone()),
                ..Default::default()
            },
        )
        .await?;
    println!(
        "  requestid={} status={:?}",
        requested.requestid, requested.status
    );

    println!("polling status...");
    let mut latest = requested.status;
    for _ in 0..5 {
        if matches!(latest, Status::Pinned | Status::Failed) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        latest = service.status(&requested.requestid).await?.status;
        println!("  status={latest:?}");
    }

    println!("listing pins on the service...");
    let listing = service
        .list(ListQuery {
            limit: Some(10),
            ..Default::default()
        })
        .await?;
    println!("  {} pin(s):", listing.count);
    for pin in &listing.results {
        println!("    {} {} {:?}", pin.requestid, pin.pin.cid, pin.status);
    }

    println!("removing our pin request...");
    service.remove(&requested.requestid).await?;
    println!("  removed {}", requested.requestid);

    ipfs.exit_daemon().await;
    Ok(())
}
