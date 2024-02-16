use rust_ipfs::Ipfs;

use rust_ipfs::Keypair;
use rust_ipfs::UninitializedIpfs;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let keypair = Keypair::generate_ed25519();
    let local_peer_id = keypair.public().to_peer_id();

    // Initialize the repo and start a daemon
    let ipfs: Ipfs = UninitializedIpfs::new()
        .with_default()
        .set_keypair(&keypair)
        .add_listening_addr("/ip4/0.0.0.0/tcp/0".parse()?)
        .with_mdns()
        .with_relay(true)
        .with_relay_server(Default::default())
        .with_upnp()
        .with_rendezvous_server()
        .with_custom_behaviour(ext_behaviour::Behaviour::new(local_peer_id))
        .listen_as_external_addr()
        .fd_limit(rust_ipfs::FDLimit::Max)
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
    use std::{
        collections::HashSet,
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
    use rust_ipfs::NetworkBehaviour;

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
