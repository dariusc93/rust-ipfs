use clap::Parser;
use futures::{pin_mut, FutureExt};
use libipld::ipld;
use libp2p::futures::StreamExt;
use libp2p::Multiaddr;
use rust_ipfs::p2p::MultiaddrExt;
use rust_ipfs::{Ipfs, Keypair, PubsubEvent, UninitializedIpfs};

use rustyline_async::Readline;
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
    #[clap(long)]
    connect: Vec<Multiaddr>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opt = Opt::parse();
    if opt.stdout_log {
        tracing_subscriber::fmt::init();
    }

    let topic = opt.topic.unwrap_or_else(|| String::from("ipfs-chat"));

    let keypair = Keypair::generate_ed25519();

    let peer_id = keypair.public().to_peer_id();
    let (mut rl, mut stdout) = Readline::new(format!("{peer_id} >"))?;

    // Initialize the repo and start a daemon
    let mut uninitialized = UninitializedIpfs::new()
        .with_custom_behaviour(ext_behaviour::Behaviour::new(peer_id, stdout.clone()))
        .set_keypair(&keypair)
        .with_default()
        .add_listening_addr("/ip4/0.0.0.0/tcp/0".parse()?);

    if opt.use_mdns {
        uninitialized = uninitialized.with_mdns();
    }

    if opt.use_relay {
        uninitialized = uninitialized.with_relay(true);
    }

    if opt.use_upnp {
        uninitialized = uninitialized.with_upnp();
    }

    let ipfs: Ipfs = uninitialized.start().await?;

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
            let peer_id = addr
                .extract_peer_id()
                .expect("Bootstrap to contain peer id");
            ipfs.add_relay(peer_id, addr).await?;
        }

        if let Err(e) = ipfs.enable_relay(None).await {
            writeln!(stdout, "> Error selecting a relay: {e}")?;
        }
    }

    for addr in opt.connect {
        let Some(peer_id) = addr.peer_id() else {
            writeln!(stdout, ">{addr} does not contain a p2p protocol. skipping")?;
            continue;
        };

        if let Err(e) = ipfs.connect(addr.clone()).await {
            writeln!(stdout, "> Error connecting to {addr}: {e}")?;
            continue;
        }

        writeln!(stdout, "Connected to {}", peer_id)?;
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
                Ok(rustyline_async::ReadlineEvent::Line(line)) => {
                    if let Err(e) = ipfs.pubsub_publish(topic.clone(), line.as_bytes().to_vec()).await {
                        writeln!(stdout, "Error publishing message: {e}")?;
                        continue;
                    }
                    writeln!(stdout, "{peer_id}: {line}")?;
                }
                Ok(rustyline_async::ReadlineEvent::Eof) => {
                    cancel.notify_one();
                    break
                },
                Ok(rustyline_async::ReadlineEvent::Interrupted) => {
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

mod ext_behaviour {
    use std::{
        collections::HashSet,
        io::Write,
        task::{Context, Poll},
    };

    use libp2p::{
        core::Endpoint,
        swarm::{
            ConnectionDenied, ConnectionId, FromSwarm, NewListenAddr, THandler, THandlerInEvent,
            THandlerOutEvent, ToSwarm,
        },
        Multiaddr, PeerId,
    };
    use rust_ipfs::{NetworkBehaviour, Protocol};
    use rustyline_async::SharedWriter;

    pub struct Behaviour {
        addrs: HashSet<Multiaddr>,
        stdout: SharedWriter,
        peer_id: PeerId,
    }

    impl Behaviour {
        pub fn new(local_peer_id: PeerId, mut stdout: SharedWriter) -> Self {
            writeln!(stdout, "PeerID: {}", local_peer_id).expect("");
            Self {
                peer_id: local_peer_id,
                addrs: Default::default(),
                stdout,
            }
        }
    }

    impl NetworkBehaviour for Behaviour {
        type ConnectionHandler = rust_ipfs::libp2p::swarm::dummy::ConnectionHandler;
        type ToSwarm = void::Void;

        fn handle_pending_inbound_connection(
            &mut self,
            _: ConnectionId,
            _: &Multiaddr,
            _: &Multiaddr,
        ) -> Result<(), ConnectionDenied> {
            Ok(())
        }

        fn handle_pending_outbound_connection(
            &mut self,
            _: ConnectionId,
            _: Option<PeerId>,
            _: &[Multiaddr],
            _: Endpoint,
        ) -> Result<Vec<Multiaddr>, ConnectionDenied> {
            Ok(vec![])
        }

        fn handle_established_inbound_connection(
            &mut self,
            _: ConnectionId,
            _: PeerId,
            _: &Multiaddr,
            _: &Multiaddr,
        ) -> Result<THandler<Self>, ConnectionDenied> {
            Ok(rust_ipfs::libp2p::swarm::dummy::ConnectionHandler)
        }

        fn handle_established_outbound_connection(
            &mut self,
            _: ConnectionId,
            _: PeerId,
            _: &Multiaddr,
            _: Endpoint,
        ) -> Result<THandler<Self>, ConnectionDenied> {
            Ok(rust_ipfs::libp2p::swarm::dummy::ConnectionHandler)
        }

        fn on_connection_handler_event(
            &mut self,
            _: PeerId,
            _: ConnectionId,
            _: THandlerOutEvent<Self>,
        ) {
        }

        fn on_swarm_event(&mut self, event: FromSwarm) {
            match event {
                FromSwarm::NewListenAddr(NewListenAddr { addr, .. }) => {
                    if self.addrs.insert(addr.clone()) {
                        writeln!(
                            self.stdout,
                            "Listening on {}",
                            addr.clone().with(Protocol::P2p(self.peer_id))
                        )
                        .expect("");
                    }
                }
                FromSwarm::ExternalAddrConfirmed(ev) => {
                    if self.addrs.insert(ev.addr.clone()) {
                        writeln!(self.stdout, "Listening on {}", ev.addr).expect("");
                    }
                }
                _ => {}
            }
        }

        fn poll(&mut self, _: &mut Context) -> Poll<ToSwarm<Self::ToSwarm, THandlerInEvent<Self>>> {
            Poll::Pending
        }
    }
}
