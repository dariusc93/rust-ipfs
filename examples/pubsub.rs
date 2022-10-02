use std::{time::Duration};

use futures::pin_mut;
use ipfs::{Ipfs, IpfsOptions, Protocol, TestTypes, UninitializedIpfs};
use libipld::ipld;
use libp2p::futures::StreamExt;
use tokio::io::AsyncBufReadExt;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let topic = "ipfs-chat";

    // Initialize the repo and start a daemon
    let mut opts = IpfsOptions::inmemory_with_generated_keys();
    // Used to discover peers locally
    // opts.mdns = true;
    // Used, along with relay [client] for hole punching
    opts.dcutr = true;
    // Used to connect to relays
    opts.relay = true;
    opts.kad_protocol = None;

    let (ipfs, fut): (Ipfs<TestTypes>, _) = UninitializedIpfs::new(opts).start().await?;
    tokio::spawn(fut);
    ipfs.default_bootstrap().await?;
    //Until autorelay is implemented and/or functions to use relay more directly, we will manually listen to the relays (using libp2p bootstrap, though you can add your own)
    let list = ipfs.get_bootstraps().await?;
    for addr in list {
        let circuit = addr.with(Protocol::P2pCircuit);
        ipfs.swarm_listen_on(circuit).await?;
    }
    if ipfs.direct_bootstrap().await.is_err() {
        //Due to no peers added to kad, we will not be able to bootstrap
    }
    let stream = ipfs.pubsub_subscribe(topic.to_string()).await?;
    pin_mut!(stream);

    {
        let ipfs = ipfs.clone();
        tokio::spawn(async move { if let Err(_) = topic_discovery(ipfs, topic).await {} });
    }

    tokio::task::yield_now().await;

    let mut stdin = tokio::io::BufReader::new(tokio::io::stdin()).lines();

    loop {
        tokio::select! {
            line = stdin.next_line() => {
                let line = line?.expect("stdin closed");
                if line == "/break" { break; }
                if let Err(_e) = ipfs.pubsub_publish(topic.into(), line.as_bytes().to_vec()).await {}
            },
            data = stream.next() => {
                if let Some(msg) = data {
                    println!("{}: {}", msg.source.unwrap(), String::from_utf8_lossy(&msg.data));
                }
            }
        }
    }
    // Exit
    ipfs.exit_daemon().await;
    Ok(())
}

async fn topic_discovery(ipfs: Ipfs<TestTypes>, topic: &str) -> anyhow::Result<()> {
    let cid = ipfs.put_dag(ipld!(topic)).await?;
    ipfs.provide(cid).await?;
    loop {
        if let Err(_) = ipfs.get_providers(cid).await {}
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
