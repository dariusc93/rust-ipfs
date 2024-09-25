use crate::p2p::bitswap::bitswap_pb;
use crate::p2p::bitswap::message::BitswapMessage;
use crate::p2p::bitswap::protocol::{BitswapProtocol, PROTOCOL as BITSWAP_PROTOCOL};
use asynchronous_codec::Framed;
use futures::stream::{BoxStream, Once, SelectAll};
use futures::{FutureExt, Stream};
use libp2p::core::muxing::SubstreamBox;
use libp2p::core::upgrade::ReadyUpgrade;
use libp2p::core::Negotiated;
use libp2p::swarm::handler::{ConnectionEvent, DialUpgradeError, ListenUpgradeError};
use libp2p::swarm::{
    ConnectionHandler, ConnectionHandlerEvent, StreamUpgradeError, SubstreamProtocol,
    SupportedProtocols,
};
use std::collections::VecDeque;
use std::io::Error;
use std::pin::Pin;
use std::task::{Context, Poll};

pub struct Handler {
    listen_protocol: SubstreamProtocol<BitswapProtocol, ()>,
    supported_protocol: SupportedProtocols,
    in_substreams: SelectAll<BoxStream<'static, ()>>,
    out_substreams: SelectAll<BoxStream<'static, ()>>,

    queue: VecDeque<BitswapMessage>,
    events: VecDeque<
        ConnectionHandlerEvent<
            <Self as ConnectionHandler>::OutboundProtocol,
            <Self as ConnectionHandler>::OutboundOpenInfo,
            <Self as ConnectionHandler>::ToBehaviour,
        >,
    >,
}

impl std::fmt::Debug for Handler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Handler").finish()
    }
}

#[derive(Debug)]
pub enum In {
    Message(BitswapMessage),
}

#[derive(Debug)]
pub enum Out {
    Supported,
    NotSupported,
    Message(BitswapMessage),
}

impl ConnectionHandler for Handler {
    type FromBehaviour = In;
    type ToBehaviour = Out;
    type InboundProtocol = BitswapProtocol;
    type OutboundProtocol = BitswapMessage;
    type InboundOpenInfo = ();
    type OutboundOpenInfo = ();

    fn listen_protocol(&self) -> SubstreamProtocol<Self::InboundProtocol, Self::InboundOpenInfo> {
        self.listen_protocol.clone()
    }

    fn connection_keep_alive(&self) -> bool {
        !self.queue.is_empty() || !self.in_substreams.is_empty() || !self.out_substreams.is_empty()
    }

    fn on_behaviour_event(&mut self, event: Self::FromBehaviour) {
        match event {
            In::Message(message) => {
                self.queue.push_back(message);
            }
        }
    }

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
            ConnectionEvent::FullyNegotiatedInbound(inbound) => {
                self.in_substreams.push(Box::pin(futures::stream::empty()));
            }
            ConnectionEvent::FullyNegotiatedOutbound(outbound) => {
                self.out_substreams.push(Box::pin(futures::stream::empty()));
            }
            ConnectionEvent::DialUpgradeError(DialUpgradeError { error, .. }) => match error {
                StreamUpgradeError::Timeout => {}
                StreamUpgradeError::Apply(_) => {}
                StreamUpgradeError::NegotiationFailed => {}
                StreamUpgradeError::Io(_) => {}
            },
            ConnectionEvent::ListenUpgradeError(ListenUpgradeError { error, .. }) => {}
            ConnectionEvent::LocalProtocolsChange(protocol)
            | ConnectionEvent::RemoteProtocolsChange(protocol) => {
                let change = self.supported_protocol.on_protocols_change(protocol);
                if change {
                    let valid = self
                        .supported_protocol
                        .iter()
                        .any(|proto| BITSWAP_PROTOCOL.eq(proto));

                    let out = match valid {
                        true => Out::Supported,
                        false => Out::NotSupported,
                    };

                    self.events
                        .push_back(ConnectionHandlerEvent::NotifyBehaviour(out));
                }
            }
            _ => {}
        }
    }

    fn poll(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<
        ConnectionHandlerEvent<Self::OutboundProtocol, Self::OutboundOpenInfo, Self::ToBehaviour>,
    > {
        if let Some(event) = self.events.pop_front() {
            return Poll::Ready(event);
        }

        if let Some(message) = self.queue.pop_front() {
            return Poll::Ready(ConnectionHandlerEvent::OutboundSubstreamRequest {
                protocol: SubstreamProtocol::new(message, ()),
            });
        }

        Poll::Pending
    }
}
