mod handler;

use std::{
    task::{Context, Poll},
    time::Duration,
};

use libp2p::{
    core::Endpoint,
    swarm::{
        ConnectionDenied, ConnectionId, FromSwarm, NetworkBehaviour, PollParameters, THandler,
        THandlerInEvent, THandlerOutEvent, ToSwarm,
    },
    Multiaddr, PeerId,
};

pub struct Behaviour {
    idle: Duration,
}

impl Behaviour {
    pub fn new(idle: Duration) -> Behaviour {
        Behaviour { idle }
    }
}

impl NetworkBehaviour for Behaviour {
    type ConnectionHandler = handler::Handler;
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
        Ok(handler::Handler::new(self.idle))
    }

    fn handle_established_outbound_connection(
        &mut self,
        _: ConnectionId,
        _: PeerId,
        _: &Multiaddr,
        _: Endpoint,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        Ok(handler::Handler::new(self.idle))
    }

    fn on_connection_handler_event(
        &mut self,
        _: PeerId,
        _: ConnectionId,
        _: THandlerOutEvent<Self>,
    ) {
    }

    fn on_swarm_event(&mut self, _: FromSwarm<Self::ConnectionHandler>) {}

    fn poll(
        &mut self,
        _: &mut Context,
        _: &mut impl PollParameters,
    ) -> Poll<ToSwarm<Self::ToSwarm, THandlerInEvent<Self>>> {
        Poll::Pending
    }
}
