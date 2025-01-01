use crate::p2p::bitswap::bitswap_pb;
use crate::p2p::bitswap::message::BitswapMessage;
use crate::p2p::bitswap::protocol::BitswapProtocol;
use asynchronous_codec::{Framed, FramedRead, FramedWrite};
use either::Either;
use futures::stream::BoxStream;
use futures::stream::SelectAll;
use futures::{FutureExt, Sink, Stream, StreamExt};
use futures_timer::Delay;
use libp2p::core::upgrade::{DeniedUpgrade, ReadyUpgrade};
use libp2p::swarm::handler::{
    ConnectionEvent, DialUpgradeError, FullyNegotiatedInbound, FullyNegotiatedOutbound,
    ListenUpgradeError,
};
use libp2p::swarm::{
    ConnectionHandler, ConnectionHandlerEvent, StreamUpgradeError, SubstreamProtocol,
};
use libp2p::{Stream as Libp2pStream, StreamProtocol};
use std::collections::VecDeque;
use std::io::Error;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;
use void::Void;

pub struct Handler {
    queue: VecDeque<ConnectionHandlerEvent<BitswapProtocol, (), Out>>,
    message_queue: VecDeque<BitswapMessage>,
    protocol: BitswapProtocol,
    inbound: Option<InboundState>,
    outbound: Option<OutboundState>,
    outbound_pending: bool,
}

enum InboundState {
    Idle(
        Framed<Libp2pStream, quick_protobuf_codec::Codec<bitswap_pb::Message, bitswap_pb::Message>>,
    ),
    Closing(
        Framed<Libp2pStream, quick_protobuf_codec::Codec<bitswap_pb::Message, bitswap_pb::Message>>,
    ),
}

enum OutboundState {
    Idle(
        Framed<Libp2pStream, quick_protobuf_codec::Codec<bitswap_pb::Message, bitswap_pb::Message>>,
    ),
    Pending(
        Framed<Libp2pStream, quick_protobuf_codec::Codec<bitswap_pb::Message, bitswap_pb::Message>>,
        BitswapMessage,
    ),
    Flushing(
        Framed<Libp2pStream, quick_protobuf_codec::Codec<bitswap_pb::Message, bitswap_pb::Message>>,
    ),
}

impl Handler {
    pub fn new() -> Self {
        Self {
            queue: VecDeque::new(),
            message_queue: VecDeque::new(),
            protocol: BitswapProtocol,
            inbound: None,
            outbound: None,
            outbound_pending: false,
        }
    }

    pub fn insert_message_into_queue(&mut self, message: BitswapMessage) {
        self.message_queue.push_back(message);
    }
}

#[derive(Debug)]
pub enum In {
    Send { message: BitswapMessage },
}

#[derive(Debug)]
pub enum Out {
    Received { message: BitswapMessage },
    Blacklisted { reason: BlacklistReason },
}

#[derive(Debug)]
pub enum BlacklistReason {
    Unsupported,
    MaxSubstreamReached,
}

impl ConnectionHandler for Handler {
    type FromBehaviour = In;
    type ToBehaviour = Out;
    type InboundProtocol = BitswapProtocol;
    type OutboundProtocol = BitswapProtocol;
    type InboundOpenInfo = ();
    type OutboundOpenInfo = ();

    fn listen_protocol(&self) -> SubstreamProtocol<Self::InboundProtocol, Self::InboundOpenInfo> {
        SubstreamProtocol::new(self.protocol, ())
    }

    fn connection_keep_alive(&self) -> bool {
        // !self.inbound.is_empty() || !self.outbound.is_empty()
        false
    }

    fn on_behaviour_event(&mut self, event: Self::FromBehaviour) {
        tracing::debug!(?event);
        match event {
            In::Send { message } => {
                self.message_queue.push_back(message);
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
            ConnectionEvent::FullyNegotiatedInbound(FullyNegotiatedInbound {
                protocol, ..
            }) => {
                tracing::debug!("fully negotiated inbound");
                self.inbound = Some(InboundState::Idle(protocol));
            }
            ConnectionEvent::FullyNegotiatedOutbound(FullyNegotiatedOutbound {
                protocol, ..
            }) => {
                tracing::debug!("fully negotiated outbound");
                self.outbound = Some(OutboundState::Idle(protocol));
            }
            ConnectionEvent::DialUpgradeError(DialUpgradeError { error, .. }) => {
                tracing::debug!(%error);
                match error {
                    StreamUpgradeError::Timeout => {}
                    StreamUpgradeError::Apply(_) => {}
                    StreamUpgradeError::NegotiationFailed => {
                        self.queue
                            .push_back(ConnectionHandlerEvent::NotifyBehaviour(Out::Blacklisted {
                                reason: BlacklistReason::Unsupported,
                            }));
                    }
                    StreamUpgradeError::Io(_) => {}
                }
            }
            ConnectionEvent::ListenUpgradeError(ListenUpgradeError { error, .. }) => {
                void::unreachable(error);
                // tracing::debug!(%error);
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
        if let Some(event) = self.queue.pop_front() {
            return Poll::Ready(event);
        }

        if self.outbound.is_none() && !self.outbound_pending {
            self.outbound_pending = true;
            return Poll::Ready(ConnectionHandlerEvent::OutboundSubstreamRequest {
                protocol: self.listen_protocol(),
            });
        }

        loop {
            match self.outbound.take() {
                None => break,
                Some(mut state) => match state {
                    OutboundState::Idle(frame) => {
                        if let Some(message) = self.message_queue.pop_front() {
                            self.outbound = Some(OutboundState::Pending(frame, message));
                            continue;
                        }
                        self.outbound = Some(OutboundState::Idle(frame));
                        break;
                    }
                    OutboundState::Pending(mut frame, message) => {
                        if message.is_empty() {
                            self.outbound = Some(OutboundState::Idle(frame));
                            continue;
                        }
                        match Pin::new(&mut frame).poll_ready(cx) {
                            Poll::Ready(Ok(())) => {
                                let proto = match message.into_proto() {
                                    Ok(pb) => pb,
                                    Err(e) => {
                                        tracing::error!(error = %e, "unable to convert message to protobuf");
                                        self.outbound = Some(OutboundState::Idle(frame));
                                        continue;
                                    }
                                };

                                match Pin::new(&mut frame).start_send(proto) {
                                    Ok(()) => {
                                        self.outbound = Some(OutboundState::Flushing(frame));
                                        continue;
                                    }
                                    Err(_) => {
                                        break;
                                    }
                                }
                            }
                            Poll::Ready(Err(e)) => {
                                tracing::error!(error = %e, "err");
                                break;
                            }
                            Poll::Pending => {
                                self.outbound = Some(OutboundState::Pending(frame, message));
                                break;
                            }
                        }
                    }
                    OutboundState::Flushing(mut frame) => {
                        match Pin::new(&mut frame).poll_flush(cx) {
                            Poll::Ready(Ok(())) => {
                                self.outbound = Some(OutboundState::Flushing(frame));
                            }
                            Poll::Ready(Err(e)) => {
                                _ = e;
                                break;
                            }
                            Poll::Pending => {
                                self.outbound = Some(OutboundState::Flushing(frame));
                            }
                        }
                    }
                },
            }
        }

        loop {
            match self.inbound.take() {
                Some(state) => match state {
                    InboundState::Idle(mut stream) => match Pin::new(&mut stream).poll_next(cx) {
                        Poll::Ready(Some(result)) => {
                            self.inbound = Some(InboundState::Idle(stream));
                            let message = match result {
                                Ok(pb) => match BitswapMessage::from_proto(pb) {
                                    Ok(msg) => msg,
                                    Err(e) => {
                                        tracing::error!(error = %e, "unable to convert protobuf to message");
                                        continue;
                                    }
                                },
                                Err(e) => {
                                    tracing::error!(error = %e, "unable to convert protobuf to message");
                                    continue;
                                }
                            };
                            return Poll::Ready(ConnectionHandlerEvent::NotifyBehaviour(
                                Out::Received { message },
                            ));
                        }
                        Poll::Ready(None) => {
                            self.inbound = Some(InboundState::Closing(stream));
                            break;
                        }
                        Poll::Pending => {
                            self.inbound = Some(InboundState::Idle(stream));
                            break;
                        }
                    },
                    InboundState::Closing(mut stream) => {
                        match Pin::new(&mut stream).poll_close(cx) {
                            Poll::Ready(Ok(())) => {
                                self.inbound = None;
                                break;
                            }
                            Poll::Ready(Err(e)) => {
                                tracing::error!(error = %e, "err");
                                break;
                            }
                            Poll::Pending => {
                                self.inbound = Some(InboundState::Closing(stream));
                                break;
                            }
                        }
                    }
                },
                None => break,
            }
        }

        Poll::Pending
    }
}
