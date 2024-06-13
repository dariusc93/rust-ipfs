use std::task::{Context, Poll};

use libp2p::{
    core::upgrade::DeniedUpgrade,
    swarm::{
        handler::ConnectionEvent, ConnectionHandler, ConnectionHandlerEvent, SubstreamProtocol,
    },
};
use void::Void;

#[derive(Default, Debug)]
pub struct Handler {
    keep_alive: bool,
}

impl Handler {
    pub fn new(keep_alive: bool) -> Self {
        Self { keep_alive }
    }
}

#[derive(Debug, Clone)]
pub enum In {
    Protect,
    Unprotect,
}

impl ConnectionHandler for Handler {
    type FromBehaviour = In;
    type ToBehaviour = Void;
    type InboundProtocol = DeniedUpgrade;
    type OutboundProtocol = DeniedUpgrade;
    type InboundOpenInfo = ();
    type OutboundOpenInfo = Void;

    fn listen_protocol(&self) -> SubstreamProtocol<Self::InboundProtocol, Self::InboundOpenInfo> {
        SubstreamProtocol::new(DeniedUpgrade, ())
    }

    fn connection_keep_alive(&self) -> bool {
        self.keep_alive
    }

    fn on_behaviour_event(&mut self, event: Self::FromBehaviour) {
        match event {
            In::Protect => {
                self.keep_alive = true;
            }
            In::Unprotect => {
                self.keep_alive = false;
            }
        }
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

    fn poll(
        &mut self,
        _: &mut Context<'_>,
    ) -> Poll<
        ConnectionHandlerEvent<Self::OutboundProtocol, Self::OutboundOpenInfo, Self::ToBehaviour>,
    > {
        Poll::Pending
    }
}
