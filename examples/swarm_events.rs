use std::time::Duration;

use libp2p::swarm::SwarmEvent;
use rust_ipfs::Ipfs;
use rust_ipfs::UninitializedIpfsDefault as UninitializedIpfs;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let ipfs: Ipfs = UninitializedIpfs::new()
        .set_default_listener()
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
