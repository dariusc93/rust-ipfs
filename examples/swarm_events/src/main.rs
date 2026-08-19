use rust_ipfs::Ipfs;
use rust_ipfs::builder::DefaultIpfsBuilder as IpfsBuilder;
use rust_ipfs::prelude::swarm::SwarmEvent;
use std::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let ipfs: Ipfs = IpfsBuilder::new()
        .set_default_listener()
        .enable_tcp()
        .swarm_events(|_, event| {
            if let SwarmEvent::NewListenAddr { address, .. } = event {
                println!("Listening on {address}");
            }
        })
        .start()
        .await?;

    tokio::time::sleep(Duration::from_secs(1)).await;

    // Exit
    ipfs.exit_daemon().await;
    Ok(())
}
