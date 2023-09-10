use clap::Parser;
use futures::{pin_mut, FutureExt};
use libipld::ipld;
use libp2p::futures::StreamExt;
use libp2p::Multiaddr;
use rust_ipfs::p2p::MultiaddrExt;
use rust_ipfs::{Ipfs, PubsubEvent};

use rust_ipfs::UninitializedIpfsNoop as UninitializedIpfs;

use rustyline_async::{Readline, ReadlineError};
use std::time::Duration;
use std::{io::Write, sync::Arc};
use tokio::sync::Notify;

#[derive(Debug, Parser)]
#[clap(name = "pubsub")]
struct Opt {
    #[clap(long)]
    bootstrap: bool,
    #[clap(long)]
    use_mdns: bool,
    #[clap(long)]
    use_relay: bool,
    #[clap(long)]
    relay_addrs: Vec<Multiaddr>,
    #[clap(long)]
    use_upnp: bool,
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
    let mut uninitialized = UninitializedIpfs::new();

    if opt.use_mdns {
        uninitialized = uninitialized.enable_mdns();
    }

    if opt.use_relay {
        uninitialized = uninitialized.enable_relay(true);
    }

    if opt.use_upnp {
        uninitialized = uninitialized.enable_upnp();
    }

    let ipfs: Ipfs = uninitialized.start().await?;

    let identity = ipfs.identity(None).await?;
    let peer_id = identity.peer_id;
    let (mut rl, mut stdout) = Readline::new(format!("{peer_id} >"))?;

    ipfs.default_bootstrap().await?;

    if opt.bootstrap {
        if let Err(_e) = ipfs.bootstrap().await {}
    }

    let cancel = Arc::new(Notify::new());
    if opt.use_relay {
        let bootstrap_nodes = ipfs.get_bootstraps().await.expect("Bootstrap exist");
        let addrs = opt
            .relay_addrs
            .iter()
            .chain(bootstrap_nodes.iter())
            .cloned();

        for mut addr in addrs {
            let peer_id = addr.extract_peer_id().expect("Bootstrap to contain peer id");
            ipfs.add_relay(peer_id, addr).await?;
        }

        if let Err(e) = ipfs.enable_relay(None).await {
            writeln!(stdout, "> Error selecting a relay: {e}")?;
        }
    }

    let mut event_stream = ipfs.pubsub_events(&topic).await?;

    let stream = ipfs.pubsub_subscribe(topic.to_string()).await?;

    pin_mut!(stream);

    tokio::spawn(topic_discovery(ipfs.clone(), topic.clone()));

    tokio::task::yield_now().await;

    loop {
        tokio::select! {
            data = stream.next() => {
                if let Some(msg) = data {
                    writeln!(stdout, "{}: {}", msg.source.expect("Message should contain a source peer_id"), String::from_utf8_lossy(&msg.data))?;
                }
            }
            Some(event) = event_stream.next() => {
                match event {
                    PubsubEvent::Subscribe { peer_id } => writeln!(stdout, "{} subscribed", peer_id)?,
                    PubsubEvent::Unsubscribe { peer_id } => writeln!(stdout, "{} unsubscribed", peer_id)?,
                }
            }
            line = rl.readline().fuse() => match line {
                Ok(line) => {
                    if let Err(e) = ipfs.pubsub_publish(topic.clone(), line.as_bytes().to_vec()).await {
                        writeln!(stdout, "Error publishing message: {e}")?;
                        continue;
                    }
                    writeln!(stdout, "{peer_id}: {line}")?;
                }
                Err(ReadlineError::Eof) => {
                    cancel.notify_one();
                    break
                },
                Err(ReadlineError::Interrupted) => {
                    cancel.notify_one();
                    break
                },
                Err(e) => {
                    writeln!(stdout, "Error: {e}")?;
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
async fn topic_discovery(ipfs: Ipfs, topic: String) -> anyhow::Result<()> {
    let cid = ipfs.put_dag(ipld!(topic)).await?;
    ipfs.provide(cid).await?;
    loop {
        let mut stream = ipfs.get_providers(cid).await?.boxed();
        while let Some(_providers) = stream.next().await {}
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
