use std::{
    task::{Context, Poll},
    time::{Duration, Instant},
};

use libp2p::{
    core::upgrade::DeniedUpgrade,
    swarm::{handler::ConnectionEvent, ConnectionHandlerEvent, KeepAlive, SubstreamProtocol},
};
use void::Void;

#[derive(Clone, Debug)]
pub struct Handler {
    keep_alive: KeepAlive,
}

impl Handler {
    pub fn new(idle: Duration) -> Self {
        Self {
            keep_alive: KeepAlive::Until(Instant::now() + idle),
        }
    }
}

impl libp2p::swarm::ConnectionHandler for Handler {
    type FromBehaviour = Void;
    type ToBehaviour = Void;
    type Error = Void;
    type InboundProtocol = DeniedUpgrade;
    type OutboundProtocol = DeniedUpgrade;
    type InboundOpenInfo = ();
    type OutboundOpenInfo = Void;

    fn listen_protocol(&self) -> SubstreamProtocol<Self::InboundProtocol, Self::InboundOpenInfo> {
        SubstreamProtocol::new(DeniedUpgrade, ())
    }

    fn on_behaviour_event(&mut self, v: Self::FromBehaviour) {
        void::unreachable(v)
    }

    fn connection_keep_alive(&self) -> KeepAlive {
        self.keep_alive
    }

    fn poll(
        &mut self,
        _: &mut Context<'_>,
    ) -> Poll<
        ConnectionHandlerEvent<
            Self::OutboundProtocol,
            Self::OutboundOpenInfo,
            Self::ToBehaviour,
            Self::Error,
        >,
    > {
        Poll::Pending
    }

    fn on_connection_event(
        &mut self,
        _: ConnectionEvent<
            Self::InboundProtocol,
            Self::OutboundProtocol,
            Self::InboundOpenInfo,
            Self::OutboundOpenInfo,
        >,
    ) {
    }
}
