use std::str::FromStr;

use clap::Parser;
use libp2p::Multiaddr;
use rust_ipfs::p2p::MultiaddrExt;
use rust_ipfs::Ipfs;

use rust_ipfs::Keypair;
use rust_ipfs::UninitializedIpfs;

#[derive(Debug, Parser)]
#[clap(name = "relay-client")]
struct Opt {
    relay: Vec<Multiaddr>,
    #[clap(long)]
    connect: Option<Multiaddr>,
    #[clap(long)]
    relay_select: Option<RelaySelect>,
}

#[derive(Default, Debug, Clone)]
enum RelaySelect {
    Random,
    First,
    Last,
    #[default]
    All,
}

impl FromStr for RelaySelect {
    type Err = std::io::Error;
    fn from_str(opt: &str) -> Result<Self, Self::Err> {
        let opt = match opt.to_lowercase().as_str() {
            "random" => Self::Random,
            "first" => Self::First,
            "last" => Self::Last,
            "all" => Self::All,
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "selection option invalid",
                ))
            }
        };

        Ok(opt)
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opt = Opt::parse();

    tracing_subscriber::fmt::init();

    let keypair = Keypair::generate_ed25519();
    let local_peer_id = keypair.public().to_peer_id();

    // Initialize the repo and start a daemon
    let ipfs: Ipfs = UninitializedIpfs::new()
        .with_identify(Default::default())
        .with_ping(Default::default())
        .set_keypair(&keypair)
        .set_default_listener()
        .with_relay(true)
        .with_custom_behaviour(ext_behaviour::Behaviour::new(local_peer_id))
        .fd_limit(rust_ipfs::FDLimit::Max)
        .start()
        .await?;

    let mut relays_peer_id = vec![];

    for mut addr in opt.relay.clone() {
        let peer_id = addr
            .extract_peer_id()
            .expect("peerid required on multiaddr");
        relays_peer_id.push(peer_id);

        if let Err(e) = ipfs.add_relay(peer_id, addr.clone()).await {
            println!("error adding relay {addr}: {e}");
        }
    }

    let selection = opt.relay_select.unwrap_or_default();

    if !relays_peer_id.is_empty() {
        let list = match selection {
            RelaySelect::Random => vec![None],
            RelaySelect::First => vec![relays_peer_id.first().copied()],
            RelaySelect::Last => vec![relays_peer_id.last().copied()],
            RelaySelect::All => relays_peer_id.iter().copied().map(Some).collect(),
        };

        for peer_id in list {
            if let Err(e) = ipfs.enable_relay(peer_id).await {
                println!("error enabling relay: {e}");
            }
        }
    }

    if let Some(addr) = opt.connect {
        ipfs.connect(addr).await?;
    }

    tokio::signal::ctrl_c().await?;

    ipfs.exit_daemon().await;
    Ok(())
}

mod ext_behaviour {
    use std::{
        collections::{HashMap, HashSet},
        task::{Context, Poll},
    };

    use libp2p::{
        core::Endpoint,
        swarm::{
            ConnectionDenied, ConnectionId, ExternalAddrExpired, FromSwarm, ListenerClosed,
            NewListenAddr, THandler, THandlerInEvent, THandlerOutEvent, ToSwarm,
        },
        Multiaddr, PeerId,
    };
    use rust_ipfs::{ListenerId, NetworkBehaviour};

    #[derive(Debug)]
    pub struct Behaviour {
        peer_id: PeerId,
        addrs: HashSet<Multiaddr>,
        listened: HashMap<ListenerId, HashSet<Multiaddr>>,
    }

    impl Behaviour {
        pub fn new(local_peer_id: PeerId) -> Self {
            println!("PeerID: {}", local_peer_id);
            Self {
                peer_id: local_peer_id,
                addrs: Default::default(),
                listened: Default::default(),
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
                FromSwarm::NewListenAddr(NewListenAddr {
                    listener_id, addr, ..
                }) => {
                    let addr = addr.clone();

                    let addr = addr.with_p2p(self.peer_id).unwrap();

                    if !self.addrs.insert(addr.clone()) {
                        return;
                    }

                    self.listened
                        .entry(listener_id)
                        .or_default()
                        .insert(addr.clone());

                    println!("Listening on {addr}");
                }

                FromSwarm::ExternalAddrConfirmed(ev) => {
                    let addr = ev.addr.clone();
                    let addr = addr.with_p2p(self.peer_id).unwrap();

                    if !self.addrs.insert(addr.clone()) {
                        return;
                    }

                    println!("Listening on {}", addr);
                }
                FromSwarm::ExternalAddrExpired(ExternalAddrExpired { addr }) => {
                    let addr = addr.clone();
                    let addr = addr.with_p2p(self.peer_id).unwrap();

                    if self.addrs.remove(&addr) {
                        println!("No longer listening on {addr}");
                    }
                }
                FromSwarm::ListenerClosed(ListenerClosed { listener_id, .. }) => {
                    if let Some(addrs) = self.listened.remove(&listener_id) {
                        for addr in addrs {
                            let addr = addr.with_p2p(self.peer_id).unwrap();
                            self.addrs.remove(&addr);
                            println!("No longer listening on {addr}");
                        }
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
