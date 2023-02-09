use libp2p::swarm::SwarmEvent;
use rust_ipfs::{Ipfs, TestTypes, UninitializedIpfs};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    // Initialize the repo and start a daemon
    let ipfs: Ipfs<TestTypes> = UninitializedIpfs::new()
        .swarm_events(|_, event| {
            if let SwarmEvent::NewListenAddr { address, .. } = event {
                println!("Listening on {address}");
            }
        })
        .start()
        .await?;

    // Exit
    ipfs.exit_daemon().await;
    Ok(())
}
