use std::time::Duration;

use clap::Parser;
use futures::{pin_mut, FutureExt};
use libipld::ipld;
use libp2p::futures::StreamExt;
use rust_ipfs::{Ipfs, IpfsOptions, Protocol, TestTypes, UninitializedIpfs};
use rustyline_async::{Readline, ReadlineError};
use std::io::Write;

#[derive(Debug, Parser)]
#[clap(name = "pubsub")]
struct Opt {
    #[clap(long)]
    disable_bootstrap: bool,
    #[clap(long)]
    disable_mdns: bool,
    #[clap(long)]
    disable_relay: bool,
    #[clap(long)]
    topic: Option<String>,
    #[clap(long)]
    stdout_log: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opt = Opt::parse();
    if opt.stdout_log {
        tracing_subscriber::fmt::init();
    }

    let topic = opt.topic.unwrap_or_else(|| String::from("ipfs-chat"));

    // Initialize the repo and start a daemon
    let opts = IpfsOptions {
        // Used to discover peers locally
        mdns: !opt.disable_mdns,
        // Used, along with relay [client] for hole punching
        dcutr: !opt.disable_relay,
        // Used to connect to relays
        relay: !opt.disable_relay,
        ..Default::default()
    };

    let (ipfs, fut): (Ipfs<TestTypes>, _) = UninitializedIpfs::new(opts).start().await?;
    tokio::spawn(fut);

    if !opt.disable_bootstrap {
        ipfs.default_bootstrap().await?;
        //Until autorelay is implemented and/or functions to use relay more directly, we will manually listen to the relays (using libp2p bootstrap, though you can add your own)

        tokio::spawn({
            let ipfs = ipfs.clone();
            async move {
                let list = ipfs.get_bootstraps().await?;
                for addr in list {
                    let circuit = addr.with(Protocol::P2pCircuit);
                    ipfs.swarm_listen_on(circuit).await?;
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }

                if ipfs.bootstrap().await.is_err() {
                    //Due to no peers added to kad, we will not be able to bootstrap
                }
                Ok::<_, anyhow::Error>(())
            }
        });
    }

    let stream = ipfs.pubsub_subscribe(topic.to_string()).await?;
    pin_mut!(stream);

    tokio::spawn(topic_discovery(ipfs.clone(), topic.clone()));

    tokio::task::yield_now().await;

    let identity = ipfs.identity(None).await?;

    let peer_id = identity.peer_id;

    let (mut rl, mut stdout) = Readline::new(format!("{} >", peer_id))?;

    loop {
        tokio::select! {
            data = stream.next() => {
                if let Some(msg) = data {
                    writeln!(stdout, "{}: {}", msg.source.expect("Message should contain a source peer_id"), String::from_utf8_lossy(&msg.data))?;
                }
            }
            line = rl.readline().fuse() => match line {
                Ok(line) => {
                    if let Err(e) = ipfs.pubsub_publish(topic.clone(), line.as_bytes().to_vec()).await {
                        writeln!(stdout, "Error publishing message: {e}")?;
                        continue;
                    }
                    writeln!(stdout, "{}: {}", peer_id, line)?;
                }
                Err(ReadlineError::Eof) => break,
                Err(ReadlineError::Interrupted) => break,
                Err(e) => {
                    writeln!(stdout, "Error: {}", e)?;
                    writeln!(stdout, "Exiting...")?;
                    break
                },
            }
        }
    }
    // Exit
    ipfs.exit_daemon().await;
    Ok(())
}

//Note: This is temporary as a similar implementation will be used internally in the future
async fn topic_discovery(ipfs: Ipfs<TestTypes>, topic: String) -> anyhow::Result<()> {
    let cid = ipfs.put_dag(ipld!(topic)).await?;
    ipfs.provide(cid).await?;
    loop {
        ipfs.get_providers(cid).await?;
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
