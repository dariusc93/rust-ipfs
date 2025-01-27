use std::{
    collections::VecDeque,
    task::{Context, Poll},
};

use libp2p::{
    core::upgrade::DeniedUpgrade,
    swarm::{
        handler::ConnectionEvent, ConnectionHandler, ConnectionHandlerEvent, SubstreamProtocol,
        SupportedProtocols,
    },
};

#[allow(clippy::type_complexity)]
#[allow(deprecated)]
#[derive(Default, Debug)]
pub struct Handler {
    events: VecDeque<
        ConnectionHandlerEvent<
            <Self as ConnectionHandler>::OutboundProtocol,
            <Self as ConnectionHandler>::OutboundOpenInfo,
            <Self as ConnectionHandler>::ToBehaviour,
        >,
    >,

    supported: bool,

    supported_protocol: SupportedProtocols,
}

#[derive(Debug, Copy, Clone)]
pub enum Out {
    Supported,
    Unsupported,
}

#[allow(deprecated)]
impl ConnectionHandler for Handler {
    type FromBehaviour = ();
    type ToBehaviour = Out;
    type InboundProtocol = DeniedUpgrade;
    type OutboundProtocol = DeniedUpgrade;
    type InboundOpenInfo = ();
    type OutboundOpenInfo = ();

    fn listen_protocol(&self) -> SubstreamProtocol<Self::InboundProtocol, Self::InboundOpenInfo> {
        SubstreamProtocol::new(DeniedUpgrade, ())
    }

    fn connection_keep_alive(&self) -> bool {
        false
    }

    fn on_behaviour_event(&mut self, event: Self::FromBehaviour) {
        _ = event;
    }

    #[allow(clippy::wildcard_in_or_patterns)]
    fn on_connection_event(
        &mut self,
        event: ConnectionEvent<
            Self::InboundProtocol,
            Self::OutboundProtocol,
            Self::InboundOpenInfo,
            Self::OutboundOpenInfo,
        >,
    ) {
        if let ConnectionEvent::RemoteProtocolsChange(protocol) = event {
            let change = self.supported_protocol.on_protocols_change(protocol);
            if change {
                let valid = self
                    .supported_protocol
                    .iter()
                    .any(|proto| libp2p::relay::HOP_PROTOCOL_NAME.eq(proto));

                match (valid, self.supported) {
                    (true, false) => {
                        self.supported = true;
                        self.events
                            .push_back(ConnectionHandlerEvent::NotifyBehaviour(Out::Supported));
                    }
                    (false, true) => {
                        self.supported = false;
                        self.events
                            .push_back(ConnectionHandlerEvent::NotifyBehaviour(Out::Unsupported));
                    }
                    (true, true) => {}
                    _ => {}
                }
            }
        }
    }

    fn poll(
        &mut self,
        _: &mut Context<'_>,
    ) -> Poll<
        ConnectionHandlerEvent<Self::OutboundProtocol, Self::OutboundOpenInfo, Self::ToBehaviour>,
    > {
        if let Some(event) = self.events.pop_front() {
            return Poll::Ready(event);
        }
        Poll::Pending
    }
}
