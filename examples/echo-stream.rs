// echo example based on libp2p-stream example
#[cfg(feature = "experimental_stream")]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    use std::time::Duration;

    use clap::Parser;
    use futures::{AsyncReadExt, AsyncWriteExt, StreamExt};
    use libp2p::{Multiaddr, PeerId, StreamProtocol};
    use rand::RngCore;
    use rust_ipfs::{p2p::MultiaddrExt, Ipfs, Keypair, UninitializedIpfsNoop as UninitializedIpfs};

    fn generate_ed25519(secret_key_seed: u8) -> Keypair {
        let mut bytes = [0u8; 32];
        bytes[0] = secret_key_seed;
        Keypair::ed25519_from_bytes(bytes).expect("Keypair is valid")
    }

    #[derive(Debug, Parser)]
    #[clap(name = "stream")]
    struct Opt {
        address: Option<Multiaddr>,
        #[clap(long)]
        seed: u8,
        #[clap(long)]
        listen_addr: Vec<Multiaddr>,
        #[clap(long)]
        max_size: usize,
    }

    const ECHO_PROTOCOL: StreamProtocol = StreamProtocol::new("/ipfs/echo/0.0.0");

    let opt = Opt::parse();
    tracing_subscriber::fmt::init();

    let keypair = generate_ed25519(opt.seed);

    println!("peer id: {}", keypair.public().to_peer_id());
    // Initialize the repo and start a daemon
    let ipfs = UninitializedIpfs::new()
        .set_keypair(&keypair)
        .set_listening_addrs(opt.listen_addr)
        .with_streams()
        .start()
        .await?;

    tokio::time::sleep(Duration::from_secs(2)).await;

    println!("{:?}", ipfs.listening_addresses().await?);

    let mut incoming_streams = ipfs.new_stream(ECHO_PROTOCOL).await?;
    let max_bytes: usize = opt.max_size;
    tokio::spawn(async move {
        while let Some((peer, stream)) = incoming_streams.next().await {
            match echo(stream, max_bytes).await {
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
        tokio::spawn(connection_handler(peer_id, ipfs, max_bytes));
    }

    async fn connection_handler(peer: PeerId, ipfs: Ipfs, max_bytes: usize) {
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let stream = match ipfs.open_stream(peer, ECHO_PROTOCOL).await {
                Ok(stream) => stream,
                Err(error) => {
                    tracing::error!(%peer, %error);
                    continue;
                }
            };

            if let Err(e) = send(stream, max_bytes).await {
                tracing::warn!(%peer, "Echo protocol failed: {e}");
                continue;
            }

            tracing::info!(%peer, "Echo complete!")
        }
    }

    async fn echo(
        mut stream: rust_ipfs::libp2p::Stream,
        max_bytes: usize,
    ) -> std::io::Result<usize> {
        let mut total = 0;

        let mut buf = vec![0u8; max_bytes];

        // let (mut reader, mut writer) = stream.split();

        loop {
            let read = stream.read(&mut buf).await?;
            if read == 0 {
                return Ok(total);
            }

            total += read;
            stream.write_all(&buf[..read]).await?;
        }
    }

    async fn send(mut stream: rust_ipfs::libp2p::Stream, max_bytes: usize) -> std::io::Result<()> {
        // let (mut reader, mut writer) = stream.split();
        let num_bytes = max_bytes;

        let mut bytes = vec![0; num_bytes];
        rand::thread_rng().fill_bytes(&mut bytes);

        stream.write_all(&bytes).await?;

        let mut buf = vec![0; num_bytes];
        stream.read_exact(&mut buf).await?;

        if bytes != buf {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "incorrect echo",
            ));
        }

        stream.close().await?;

        Ok(())
    }

    // Used to wait until the process is terminated instead of creating a loop
    tokio::signal::ctrl_c().await?;

    ipfs.exit_daemon().await;
    Ok(())
}

#[cfg(not(feature = "experimental_stream"))]
fn main() {
    unimplemented!("\"experimental_stream\" not enabled")
}
