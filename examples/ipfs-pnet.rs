use clap::Parser;
use libp2p::core::upgrade::Version;
use libp2p::noise;
use libp2p::pnet::PnetConfig;
use libp2p::pnet::PreSharedKey;
use libp2p::tcp;
use libp2p::yamux;
use libp2p::Transport;
use rand::Rng;
use rust_ipfs::Ipfs;
use rust_ipfs::Keypair;
use rust_ipfs::UninitializedIpfs;
use std::str::FromStr;

#[derive(Debug, Parser)]
#[clap(name = "ipfs-pnet")]
struct Opt {
    #[clap(required = false)]
    psk: Option<String>,
}
fn generate_psk() -> PreSharedKey {
    let mut key_bytes = [0u8; 32];
    rand::thread_rng().fill(&mut key_bytes);
    PreSharedKey::new(key_bytes)
}
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let keypair = Keypair::generate_ed25519();
    let local_peer_id = keypair.public().to_peer_id();

    // you can provide a PSK here or create a random one
    // example: cargo run --example 8ab6e6aeb73353791b88c3c73e3d9a5111273e6d89edcbfb8be783f1e595617b
    let opt = Opt::parse();
    let psk = {
        match opt.psk {
            Some(psk) => {
                let formatted_psk = format!("/key/swarm/psk/1.0.0/\n/base16/\n{}", psk);
                match PreSharedKey::from_str(&formatted_psk) {
                    Ok(psk) => psk,
                    Err(_) => {
                        println!("Invalid PSK , generating a random PSK");
                        generate_psk()
                    }
                }
            }
            None => generate_psk(),
        }
    };
    println!("PSK: {:?}", psk);

    // Initialize the repo and start a daemon
    let ipfs: Ipfs = UninitializedIpfs::new()
        .with_default()
        .set_keypair(&keypair)
        .add_listening_addr("/ip4/0.0.0.0/tcp/0".parse()?)
        .with_mdns()
        .with_custom_transport(Box::new(move |key: &Keypair, _| {
            let noise_config = noise::Config::new(key).unwrap();
            let yamux_config = yamux::Config::default();

            let base_transport = tcp::tokio::Transport::new(tcp::Config::default().nodelay(true));
            let pnet_transport =
                base_transport.and_then(move |socket, _| PnetConfig::new(psk).handshake(socket));

            Ok(pnet_transport
                .upgrade(Version::V1Lazy)
                .authenticate(noise_config)
                .multiplex(yamux_config)
                .boxed())
        }))
        .with_custom_behaviour(ext_behaviour::Behaviour::new(local_peer_id))
        .start()
        .await?;

    ipfs.default_bootstrap().await?;
    ipfs.bootstrap().await?;

    // Used to wait until the process is terminated instead of creating a loop
    tokio::signal::ctrl_c().await?;
    ipfs.exit_daemon().await;
    Ok(())
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
    use rust_ipfs::NetworkBehaviour;
    use std::convert::Infallible;
    use std::{
        collections::HashSet,
        task::{Context, Poll},
    };

    #[derive(Default, Debug)]
    pub struct Behaviour {
        addrs: HashSet<Multiaddr>,
    }

    impl Behaviour {
        pub fn new(local_peer_id: PeerId) -> Self {
            println!("PeerID: {}", local_peer_id);
            Self {
                addrs: Default::default(),
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
            peerid: PeerId,
            _: ConnectionId,
            _: THandlerOutEvent<Self>,
        ) {
            println!("Connection established with {peerid}");
        }

        fn on_swarm_event(&mut self, event: FromSwarm) {
            match event {
                FromSwarm::NewListenAddr(NewListenAddr { addr, .. }) => {
                    if self.addrs.insert(addr.clone()) {
                        println!("Listening on {addr}");
                    }
                }
                FromSwarm::ExternalAddrConfirmed(ev) => {
                    if self.addrs.insert(ev.addr.clone()) {
                        println!("Listening on {}", ev.addr);
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
