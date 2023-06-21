use std::{
    collections::VecDeque,
    task::{Context, Poll},
};

use either::Either;
use libp2p::{
    core::Endpoint,
    swarm::{
        self, dummy, ConnectionDenied, ConnectionId, FromSwarm, NetworkBehaviour, PollParameters,
        THandler, THandlerInEvent, ToSwarm,
    },
    Multiaddr, PeerId, StreamProtocol,
};

mod handler;

#[derive(Debug)]
pub struct Behaviour {
    events: VecDeque<ToSwarm<<Self as NetworkBehaviour>::ToSwarm, THandlerInEvent<Self>>>,
    protocol: Vec<StreamProtocol>,
    use_deprecated: bool,
}

impl Default for Behaviour {
    fn default() -> Self {
        Self {
            events: Default::default(),
            protocol: Default::default(),
            use_deprecated: true,
        }
    }
}

impl Behaviour {
    pub fn iter(&self) -> impl Iterator<Item = String> + '_ {
        self.protocol.iter().map(|s| s.to_string())
    }
}

impl NetworkBehaviour for Behaviour {
    type ConnectionHandler = Either<handler::Handler, dummy::ConnectionHandler>;
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
        let handler = match self.use_deprecated {
            true => Either::Right(dummy::ConnectionHandler),
            false => Either::Left(handler::Handler::default()),
        };
        Ok(handler)
    }

    fn handle_established_outbound_connection(
        &mut self,
        _: ConnectionId,
        _: PeerId,
        _: &Multiaddr,
        _: Endpoint,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        let handler = match self.use_deprecated {
            true => Either::Right(dummy::ConnectionHandler),
            false => Either::Left(handler::Handler::default()),
        };
        Ok(handler)
    }

    fn on_connection_handler_event(
        &mut self,
        _: PeerId,
        _: ConnectionId,
        event: swarm::THandlerOutEvent<Self>,
    ) {
        match event {
            Either::Left(handler::Out::Protocol(protocol)) => {
                if self.protocol.ne(&protocol) {
                    self.protocol = protocol;
                }
            }
            Either::Right(v) => void::unreachable(v),
        }
    }

    fn on_swarm_event(&mut self, event: FromSwarm<Self::ConnectionHandler>) {
        match event {
            FromSwarm::AddressChange(_)
            | FromSwarm::ConnectionEstablished(_)
            | FromSwarm::ConnectionClosed(_)
            | FromSwarm::DialFailure(_)
            | FromSwarm::ListenFailure(_)
            | FromSwarm::NewListener(_)
            | FromSwarm::NewListenAddr(_)
            | FromSwarm::ExpiredListenAddr(_)
            | FromSwarm::ListenerError(_)
            | FromSwarm::ListenerClosed(_)
            | FromSwarm::ExternalAddrConfirmed(_)
            | FromSwarm::ExternalAddrExpired(_)
            | FromSwarm::NewExternalAddrCandidate(_) => {}
        }
    }

    #[allow(deprecated)]
    fn poll(
        &mut self,
        _: &mut Context,
        params: &mut impl PollParameters,
    ) -> Poll<ToSwarm<Self::ToSwarm, THandlerInEvent<Self>>> {
        if self.use_deprecated {
            let supported_protocols = params
                .supported_protocols()
                .filter_map(|b| {
                    StreamProtocol::try_from_owned(String::from_utf8_lossy(&b).to_string()).ok()
                })
                .collect::<Vec<_>>();

            if supported_protocols.len() != self.protocol.len() {
                self.protocol = supported_protocols;
            }
        }
        if let Some(event) = self.events.pop_front() {
            return Poll::Ready(event);
        }
        Poll::Pending
    }
}
