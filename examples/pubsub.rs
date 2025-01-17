use clap::Parser;
use futures::FutureExt;
use libp2p::futures::StreamExt;
use libp2p::Multiaddr;
use rust_ipfs::p2p::MultiaddrExt;
use rust_ipfs::{ConnectionEvents, Ipfs, Keypair, PubsubEvent, UninitializedIpfs};

use pollable_map::stream::StreamMap;
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

    let main_topic = Arc::new(tokio::sync::Mutex::new(topic.clone()));

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

    if opt.bootstrap {
        ipfs.default_bootstrap().await?;
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

    let mut st = ipfs.connection_events().await?;

    let mut main_events = StreamMap::new();

    let mut listener_st = StreamMap::new();

    let mut main_event_st = ipfs.pubsub_events(None).await?;

    let stream = ipfs.pubsub_subscribe(topic.clone()).await?;

    listener_st.insert(topic.clone(), stream);

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

    let owned_topic = topic.to_string();
    tokio::spawn(topic_discovery(ipfs.clone(), owned_topic));

    tokio::task::yield_now().await;

    loop {
        tokio::select! {
            Some((topic, msg)) = listener_st.next() => {
                writeln!(stdout, "> {topic}: {}: {}", msg.source.expect("Message should contain a source peer_id"), String::from_utf8_lossy(&msg.data))?;
            }
            Some(conn_ev) = st.next() => {
                match conn_ev {
                    ConnectionEvents::IncomingConnection{ peer_id, .. } => {
                        writeln!(stdout, "> {peer_id} connected")?;
                    }
                    ConnectionEvents::OutgoingConnection{ peer_id, .. } => {
                        writeln!(stdout, "> {peer_id} connected")?;
                    }
                    ConnectionEvents::ClosedConnection{ peer_id, .. } => {
                        writeln!(stdout, "> {peer_id} disconnected")?;
                    }
                }
            }
            Some(event) = main_event_st.next() => {
                match event {
                    PubsubEvent::Subscribe { peer_id, topic: Some(topic) } => writeln!(stdout, "{} subscribed to {}", peer_id, topic)?,
                    PubsubEvent::Unsubscribe { peer_id, topic: Some(topic) } => writeln!(stdout, "{} unsubscribed from {}", peer_id, topic)?,
                    _ => unreachable!(),
                }
            }
            Some((topic, event)) = main_events.next() => {
                match event {
                    PubsubEvent::Subscribe { peer_id, topic: None } => writeln!(stdout, "{} subscribed to {}", peer_id, topic)?,
                    PubsubEvent::Unsubscribe { peer_id, topic: None } => writeln!(stdout, "{} unsubscribed from {}", peer_id, topic)?,
                    _ => unreachable!()
                }
            }
            line = rl.readline().fuse() => match line {
                Ok(rustyline_async::ReadlineEvent::Line(line)) => {
                    let line = line.trim();
                    if !line.starts_with('/') {
                        if !line.is_empty() {
                            let topic_to_publish = &*main_topic.lock().await;
                            if let Err(e) = ipfs.pubsub_publish(topic_to_publish.clone(), line.as_bytes().to_vec()).await {
                                writeln!(stdout, "> error publishing message: {e}")?;
                                continue;
                            }
                            writeln!(stdout, "{peer_id}: {line}")?;
                        }
                        continue;
                    }

                    let mut command = line.split(' ');

                    match command.next() {
                        Some("/subscribe") => {
                            let topic = match command.next() {
                                Some(topic) => topic.to_string(),
                                None => {
                                    writeln!(stdout, "> topic must be provided")?;
                                    continue;
                                }
                            };
                            let event_st = ipfs.pubsub_events(topic.clone()).await?;
                            let Ok(st) = ipfs.pubsub_subscribe(topic.clone()).await else {
                                writeln!(stdout, "> already subscribed to topic")?;
                                continue;
                            };

                            listener_st.insert(topic.clone(), st);
                            main_events.insert(topic.clone(), event_st);
                            writeln!(stdout, "> subscribed to {}", topic)?;
                            *main_topic.lock().await = topic;
                            continue;
                        }
                        Some("/unsubscribe") => {
                            let topic = match command.next() {
                                Some(topic) => topic.to_string(),
                                None => main_topic.lock().await.clone()
                            };

                            listener_st.remove(&topic);
                            main_events.remove(&topic);

                            if !ipfs.pubsub_unsubscribe(&topic).await.unwrap_or_default() {
                                writeln!(stdout, "> unable to unsubscribe from {}", topic)?;
                                continue;
                            }

                            writeln!(stdout, "> unsubscribe from {}", topic)?;
                            if let Some(some_topic) = main_events.keys().next() {
                                *main_topic.lock().await = some_topic.clone();
                                writeln!(stdout, "> setting current topic to {}", some_topic)?;
                            }
                            continue;
                        }
                        Some("/list-topics") => {
                            let topics = ipfs.pubsub_subscribed().await.unwrap_or_default();
                            if topics.is_empty() {
                                writeln!(stdout, "> not subscribed to any topics")?;
                                continue;
                            }

                            let current_topic = main_topic.lock().await.clone();

                            writeln!(stdout, "> list of topics")?;
                            for topic in topics {
                                writeln!(stdout, "\t{topic} {}", if current_topic == topic { "- current" } else { "" } )?;
                            }
                        }
                        Some("/set-current-topic") => {
                            let topic = match command.next() {
                                Some(topic) if !topic.is_empty() => topic.to_string(),
                                _ => {
                                    writeln!(stdout, "> topic must be provided")?;
                                    continue;
                                }
                            };

                            let topics = ipfs.pubsub_subscribed().await.unwrap_or_default();
                            if topics.is_empty() || !topics.contains(&topic) {
                                writeln!(stdout, "> not subscribed to topic \"{topic}\"")?;
                                continue;
                            }

                            *main_topic.lock().await = topic.clone();

                            writeln!(stdout, "> topic set to {topic}")?;
                        }
                        _ => continue
                    }

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
    let topic_bytes = topic.as_bytes().to_vec();
    ipfs.dht_provide(topic_bytes.clone()).await?;
    loop {
        let mut stream = ipfs.dht_get_providers(topic_bytes.clone()).await?.boxed();
        while let Some(_providers) = stream.next().await {}
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

mod ext_behaviour {
    use libp2p::swarm::derive_prelude::PortUse;
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
    use std::convert::Infallible;
    use std::{
        collections::HashSet,
        io::Write,
        task::{Context, Poll},
    };

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
        type ToSwarm = Infallible;

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
            _: PortUse,
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
