use std::{
    collections::VecDeque,
    task::{Context, Poll},
};

use libp2p::{
    core::upgrade::DeniedUpgrade,
    swarm::{
        handler::ConnectionEvent, ConnectionHandler, ConnectionHandlerEvent, KeepAlive,
        SubstreamProtocol, SupportedProtocols,
    },
    StreamProtocol,
};
use void::Void;

#[allow(clippy::type_complexity)]
#[derive(Default, Debug)]
pub struct Handler {
    events: VecDeque<
        ConnectionHandlerEvent<
            <Self as ConnectionHandler>::OutboundProtocol,
            <Self as ConnectionHandler>::OutboundOpenInfo,
            <Self as ConnectionHandler>::ToBehaviour,
            <Self as ConnectionHandler>::Error,
        >,
    >,
    supported_protocol: SupportedProtocols,
}

#[derive(Debug, Clone)]
pub enum Out {
    Protocol(Vec<StreamProtocol>),
}

impl ConnectionHandler for Handler {
    type FromBehaviour = Void;
    type ToBehaviour = Out;
    type Error = Void;
    type InboundProtocol = DeniedUpgrade;
    type OutboundProtocol = DeniedUpgrade;
    type InboundOpenInfo = ();
    type OutboundOpenInfo = Void;

    fn listen_protocol(&self) -> SubstreamProtocol<Self::InboundProtocol, Self::InboundOpenInfo> {
        SubstreamProtocol::new(DeniedUpgrade, ())
    }

    fn connection_keep_alive(&self) -> KeepAlive {
        KeepAlive::No
    }

    fn on_behaviour_event(&mut self, _: Self::FromBehaviour) {}

    fn on_connection_event(
        &mut self,
        event: ConnectionEvent<
            Self::InboundProtocol,
            Self::OutboundProtocol,
            Self::InboundOpenInfo,
            Self::OutboundOpenInfo,
        >,
    ) {
        match event {
            ConnectionEvent::LocalProtocolsChange(protocol) => {
                let change = self.supported_protocol.on_protocols_change(protocol);
                if change {
                    self.events
                        .push_back(ConnectionHandlerEvent::NotifyBehaviour(Out::Protocol(
                            self.supported_protocol.iter().cloned().collect(),
                        )));
                }
            }
            ConnectionEvent::RemoteProtocolsChange(_)
            | ConnectionEvent::FullyNegotiatedInbound(_)
            | ConnectionEvent::FullyNegotiatedOutbound(_)
            | ConnectionEvent::AddressChange(_)
            | ConnectionEvent::DialUpgradeError(_)
            | ConnectionEvent::ListenUpgradeError(_) => {}
        }
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
        if let Some(event) = self.events.pop_front() {
            return Poll::Ready(event);
        }
        Poll::Pending
    }
}
