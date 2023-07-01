use std::time::Duration;

use libp2p::swarm::SwarmEvent;
use rust_ipfs::{Ipfs, IpfsOptions, UninitializedIpfs};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    // Initialize the repo and start a daemon
    let opts = IpfsOptions::inmemory_with_generated_keys();
    let ipfs: Ipfs = UninitializedIpfs::with_opt(opts)
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
