use clap::Parser;
use connexa::prelude::transport::pnet::PreSharedKey;
use rand::Rng;
use rust_ipfs::{builder::IpfsBuilder, Ipfs, Keypair};
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

/// you can provide a PSK as an argument
/// example: cargo run --example 8ab6e6aeb73353791b88c3c73e3d9a5111273e6d89edcbfb8be783f1e595617b
///
/// or create a random one without providing any argument
/// example: cargo run --example ipfs-pnet
#[cfg(feature = "pnet")]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let keypair = Keypair::generate_ed25519();
    let local_peer_id = keypair.public().to_peer_id();

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
    let ipfs: Ipfs = IpfsBuilder::with_keypair(&keypair)?
        .with_default()
        .add_listening_addr("/ip4/0.0.0.0/tcp/0".parse()?)
        .with_mdns()
        .enable_pnet(psk)
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
    use connexa::dummy::DummyHandler;
    use connexa::prelude::swarm::derive_prelude::PortUse;
    use connexa::prelude::swarm::{
        ConnectionDenied, FromSwarm, NewListenAddr, THandler, THandlerInEvent, THandlerOutEvent,
        ToSwarm,
    };
    use connexa::prelude::transport::Endpoint;

    use rust_ipfs::{ConnectionId, Multiaddr, NetworkBehaviour, PeerId};
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
        type ConnectionHandler = DummyHandler;
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
            Ok(DummyHandler)
        }

        fn handle_established_outbound_connection(
            &mut self,
            _: ConnectionId,
            _: PeerId,
            _: &Multiaddr,
            _: Endpoint,
            _: PortUse,
        ) -> Result<THandler<Self>, ConnectionDenied> {
            Ok(DummyHandler)
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

#[cfg(not(feature = "pnet"))]
fn main() {
    panic!("This example requires the `pnet` feature");
}
