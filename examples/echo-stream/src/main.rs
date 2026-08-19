// echo example based on libp2p-stream example
use rust_ipfs::{Multiaddr, PeerId, StreamProtocol};
use std::time::Duration;

use clap::Parser;
use futures::{AsyncReadExt, AsyncWriteExt, StreamExt};
use rand::RngCore;
use rust_ipfs::{Ipfs, Keypair, builder::DefaultIpfsBuilder as IpfsBuilder, p2p::MultiaddrExt};

#[derive(Debug, Parser)]
#[clap(name = "stream")]
struct Opt {
    address: Option<Multiaddr>,
}

const ECHO_PROTOCOL: StreamProtocol = StreamProtocol::new("/ipfs/echo/0.0.0");

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opt = Opt::parse();
    tracing_subscriber::fmt::init();

    let keypair = Keypair::generate_ed25519();

    println!("peer id: {}", keypair.public().to_peer_id());
    // Initialize the repo and start a daemon
    let ipfs = IpfsBuilder::with_keypair(&keypair)?
        .enable_tcp()
        .add_listening_addr("/ip4/127.0.0.1/tcp/0".parse()?)
        .with_streams()
        .start()
        .await?;

    tokio::time::sleep(Duration::from_secs(2)).await;

    println!("{:?}", ipfs.listening_addresses().await?);

    let mut incoming_streams = ipfs.new_stream(ECHO_PROTOCOL).await?;

    tokio::spawn(async move {
        while let Some((peer, stream)) = incoming_streams.next().await {
            match echo(stream).await {
                Ok(n) => {
                    tracing::info!(%peer, "Echoed {n} bytes!");
                }
                Err(e) => {
                    tracing::warn!(%peer, "Echo failed: {e}");
                    continue;
                }
            };
        }
    });

    if let Some(address) = opt.address {
        let Some(peer_id) = address.peer_id() else {
            anyhow::bail!("Provided address does not end in `/p2p`");
        };

        ipfs.connect(address).await?;
        let ipfs = ipfs.clone();
        tokio::spawn(connection_handler(peer_id, ipfs));
    }

    // Used to wait until the process is terminated instead of creating a loop
    tokio::signal::ctrl_c().await?;

    ipfs.exit_daemon().await;
    Ok(())
}

async fn connection_handler(peer: PeerId, ipfs: Ipfs) {
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let stream = match ipfs.open_stream(peer, ECHO_PROTOCOL).await {
            Ok(stream) => stream,
            Err(error) => {
                tracing::error!(%peer, %error);
                continue;
            }
        };

        if let Err(e) = send(stream).await {
            tracing::warn!(%peer, "Echo protocol failed: {e}");
            continue;
        }

        tracing::info!(%peer, "Echo complete!")
    }
}

async fn echo(mut stream: rust_ipfs::prelude::Stream) -> std::io::Result<usize> {
    let mut total = 0;

    let mut buf = [0u8; 100];

    loop {
        let read = stream.read(&mut buf).await?;
        if read == 0 {
            return Ok(total);
        }

        total += read;
        stream.write_all(&buf[..read]).await?;
    }
}

async fn send(mut stream: rust_ipfs::prelude::Stream) -> std::io::Result<()> {
    let num_bytes: usize = rand::random_range(0..1000);

    let mut bytes = vec![0; num_bytes];
    rand::rng().fill_bytes(&mut bytes);

    stream.write_all(&bytes).await?;

    let mut buf = vec![0; num_bytes];
    stream.read_exact(&mut buf).await?;

    if bytes != buf {
        return Err(std::io::Error::other("incorrect echo"));
    }

    stream.close().await?;

    Ok(())
}
