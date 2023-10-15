use rust_ipfs::Ipfs;

use rust_ipfs::UninitializedIpfs;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    // Initialize the repo and start a daemon
    let ipfs: Ipfs = UninitializedIpfs::new()
        .with_custom_behaviour(ext_behaviour::Behaviour)
        .start()
        .await?;

    // Used to wait until the process is terminated instead of creating a loop
    tokio::signal::ctrl_c().await?;

    ipfs.exit_daemon().await;
    Ok(())
}

mod ext_behaviour {
    use std::task::{Context, Poll};

    use libp2p::{
        core::Endpoint,
        swarm::{
            ConnectionDenied, ConnectionId, FromSwarm, NewListenAddr, PollParameters, THandler,
            THandlerInEvent, THandlerOutEvent, ToSwarm,
        },
        Multiaddr, PeerId,
    };
    use rust_ipfs::NetworkBehaviour;

    #[derive(Default, Debug)]
    pub struct Behaviour;

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

        fn on_swarm_event(&mut self, event: FromSwarm<Self::ConnectionHandler>) {
            match event {
                FromSwarm::NewListenAddr(NewListenAddr { addr, .. }) => {
                    println!("Listening on {addr}");
                }
                FromSwarm::AddressChange(_)
                | FromSwarm::ConnectionEstablished(_)
                | FromSwarm::ConnectionClosed(_)
                | FromSwarm::DialFailure(_)
                | FromSwarm::ListenFailure(_)
                | FromSwarm::NewListener(_)
                | FromSwarm::ExpiredListenAddr(_)
                | FromSwarm::ListenerError(_)
                | FromSwarm::ListenerClosed(_)
                | FromSwarm::ExternalAddrConfirmed(_)
                | FromSwarm::ExternalAddrExpired(_)
                | FromSwarm::NewExternalAddrCandidate(_) => {}
            }
        }

        fn poll(
            &mut self,
            _: &mut Context,
            _: &mut impl PollParameters,
        ) -> Poll<ToSwarm<Self::ToSwarm, THandlerInEvent<Self>>> {
            Poll::Pending
        }
    }
}
