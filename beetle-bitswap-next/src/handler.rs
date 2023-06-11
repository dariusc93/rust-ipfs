use std::{
    collections::VecDeque,
    fmt::Debug,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use asynchronous_codec::Framed;
use futures::StreamExt;
use futures::{
    prelude::*,
    stream::{BoxStream, SelectAll},
};
use libp2p::swarm::{
    ConnectionHandler, ConnectionHandlerEvent, KeepAlive, StreamUpgradeError, SubstreamProtocol,
};
use libp2p::{
    core::{muxing::SubstreamBox, upgrade::NegotiationError, Negotiated},
    swarm::handler::{
        ConnectionEvent, DialUpgradeError, FullyNegotiatedInbound, FullyNegotiatedOutbound,
    },
};
use smallvec::SmallVec;
use tokio::sync::oneshot;
use tracing::{error, trace, warn};

use crate::{
    error::Error,
    message::BitswapMessage,
    network,
    protocol::{BitswapCodec, ProtocolConfig, ProtocolId},
};

/// The initial time (in seconds) we set the keep alive for protocol negotiations to occur.
// TODO: configurable
const INITIAL_KEEP_ALIVE: u64 = 30;

#[derive(thiserror::Error, Debug)]
pub enum BitswapHandlerError {
    /// The message exceeds the maximum transmission size.
    #[error("max transmission size")]
    MaxTransmissionSize,
    /// Protocol negotiation timeout.
    #[error("negotiation timeout")]
    NegotiationTimeout,
    /// Protocol negotiation failed.
    #[error("negotatiation protocol error {0}")]
    NegotiationProtocolError(#[from] NegotiationError),
    /// IO error.
    #[error("io {0}")]
    Io(#[from] std::io::Error),
    #[error("bitswap {0}")]
    Bitswap(#[from] Error),
}

/// The event emitted by the Handler. This informs the behaviour of various events created
/// by the handler.
#[derive(Debug)]
pub enum HandlerEvent {
    /// A Bitswap message has been received.
    Message {
        /// The Bitswap message.
        message: BitswapMessage,
        protocol: ProtocolId,
    },
    Connected {
        protocol: ProtocolId,
    },
    ProtocolNotSuppported,
    FailedToSendMessage {
        error: BitswapHandlerError,
    },
}

type BitswapMessageResponse = oneshot::Sender<Result<(), network::SendError>>;

/// A message sent from the behaviour to the handler.
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum BitswapHandlerIn {
    /// A bitswap message to send.
    Message(BitswapMessage, BitswapMessageResponse),
    // TODO: do we need a close?
    Protect,
    Unprotect,
}

type BitswapConnectionHandlerEvent = ConnectionHandlerEvent<
    ProtocolConfig,
    (BitswapMessage, BitswapMessageResponse),
    HandlerEvent,
    BitswapHandlerError,
>;

/// Protocol Handler that manages a single long-lived substream with a peer.
pub struct BitswapHandler {
    /// Upgrade configuration for the bitswap protocol.
    listen_protocol: SubstreamProtocol<ProtocolConfig, ()>,

    /// Outbound substreams.
    outbound_substreams: SelectAll<BoxStream<'static, BitswapConnectionHandlerEvent>>,
    /// Inbound substreams.
    inbound_substreams: SelectAll<BoxStream<'static, BitswapConnectionHandlerEvent>>,

    /// Pending events to yield.
    events: SmallVec<[BitswapConnectionHandlerEvent; 4]>,

    /// Queue of values that we want to send to the remote.
    send_queue: VecDeque<(BitswapMessage, BitswapMessageResponse)>,

    protocol: Option<ProtocolId>,

    /// The amount of time we allow idle connections before disconnecting.
    idle_timeout: Duration,

    /// Collection of errors from attempting an upgrade.
    upgrade_errors: VecDeque<StreamUpgradeError<BitswapHandlerError>>,

    /// Flag determining whether to maintain the connection to the peer.
    keep_alive: KeepAlive,
}

impl Debug for BitswapHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BitswapHandler")
            .field("listen_protocol", &self.listen_protocol)
            .field(
                "outbound_substreams",
                &format!("SelectAll<{} streams>", self.outbound_substreams.len()),
            )
            .field(
                "inbound_substreams",
                &format!("SelectAll<{} streams>", self.inbound_substreams.len()),
            )
            .field("events", &self.events)
            .field("send_queue", &self.send_queue)
            .field("protocol", &self.protocol)
            .field("idle_timeout", &self.idle_timeout)
            .field("upgrade_errors", &self.upgrade_errors)
            .field("keep_alive", &self.keep_alive)
            .finish()
    }
}

impl BitswapHandler {
    /// Builds a new [`BitswapHandler`].
    pub fn new(protocol_config: ProtocolConfig, idle_timeout: Duration) -> Self {
        Self {
            listen_protocol: SubstreamProtocol::new(protocol_config, ()),
            inbound_substreams: Default::default(),
            outbound_substreams: Default::default(),
            send_queue: Default::default(),
            protocol: None,
            idle_timeout,
            upgrade_errors: VecDeque::new(),
            keep_alive: KeepAlive::Until(Instant::now() + Duration::from_secs(INITIAL_KEEP_ALIVE)),
            events: Default::default(),
        }
    }
}

impl ConnectionHandler for BitswapHandler {
    type FromBehaviour = BitswapHandlerIn;
    type ToBehaviour = HandlerEvent;
    type Error = BitswapHandlerError;
    type InboundOpenInfo = ();
    type InboundProtocol = ProtocolConfig;
    type OutboundOpenInfo = (BitswapMessage, BitswapMessageResponse);
    type OutboundProtocol = ProtocolConfig;

    fn listen_protocol(&self) -> SubstreamProtocol<Self::InboundProtocol, Self::InboundOpenInfo> {
        self.listen_protocol.clone()
    }

    fn on_behaviour_event(&mut self, message: BitswapHandlerIn) {
        match message {
            BitswapHandlerIn::Message(m, response) => {
                self.send_queue.push_back((m, response));

                // sending a message, reset keepalive
                self.keep_alive = KeepAlive::Until(Instant::now() + self.idle_timeout);
            }
            BitswapHandlerIn::Protect => {
                self.keep_alive = KeepAlive::Yes;
            }
            BitswapHandlerIn::Unprotect => {
                self.keep_alive =
                    KeepAlive::Until(Instant::now() + Duration::from_secs(INITIAL_KEEP_ALIVE));
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
                let protocol_id = protocol.codec().protocol;
                if self.protocol.is_none() {
                    self.protocol = Some(protocol_id);
                }

                trace!("New inbound substream request: {:?}", protocol_id);
                self.inbound_substreams
                    .push(Box::pin(inbound_substream(protocol)));
            }
            ConnectionEvent::FullyNegotiatedOutbound(FullyNegotiatedOutbound {
                protocol,
                info,
            }) => {
                let protocol_id = protocol.codec().protocol;
                if self.protocol.is_none() {
                    self.protocol = Some(protocol_id);
                }

                trace!("New outbound substream: {:?}", protocol_id);
                self.outbound_substreams
                    .push(Box::pin(outbound_substream(protocol, info)));
            }
            ConnectionEvent::AddressChange(_) => todo!(),
            ConnectionEvent::DialUpgradeError(DialUpgradeError { error, .. }) => {
                warn!("Dial upgrade error {:?}", error);
                self.upgrade_errors.push_back(error);
            }
            ConnectionEvent::ListenUpgradeError(_) => {}
            //TODO: Cover protocol change
            _ => {}
        }
    }

    fn connection_keep_alive(&self) -> KeepAlive {
        self.keep_alive
    }

    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<BitswapConnectionHandlerEvent> {
        if !self.events.is_empty() {
            return Poll::Ready(self.events.remove(0));
        }

        // Handle any upgrade errors
        if let Some(error) = self.upgrade_errors.pop_front() {
            let reported_error = match error {
                StreamUpgradeError::Timeout | StreamUpgradeError::NegotiationFailed => {
                    BitswapHandlerError::NegotiationTimeout
                }
                StreamUpgradeError::Io(e) => BitswapHandlerError::Io(e),
                StreamUpgradeError::Apply(negotiation_error) => negotiation_error,
            };

            // Close the connection
            return Poll::Ready(ConnectionHandlerEvent::Close(reported_error));
        }

        // determine if we need to create the stream
        if let Some(message) = self.send_queue.pop_front() {
            return Poll::Ready(ConnectionHandlerEvent::OutboundSubstreamRequest {
                protocol: self.listen_protocol.clone().map_info(|()| message),
            });
        }

        // Poll substreams

        if let Poll::Ready(Some(event)) = self.outbound_substreams.poll_next_unpin(cx) {
            return Poll::Ready(event);
        }

        if let Poll::Ready(Some(event)) = self.inbound_substreams.poll_next_unpin(cx) {
            if let ConnectionHandlerEvent::NotifyBehaviour(HandlerEvent::Message { .. }) = event {
                // Update keep alive as we have received a message
                self.keep_alive = KeepAlive::Until(Instant::now() + self.idle_timeout);
            }

            return Poll::Ready(event);
        }

        Poll::Pending
    }
}

fn inbound_substream(
    mut substream: Framed<libp2p::Stream, BitswapCodec>,
) -> impl Stream<Item = BitswapConnectionHandlerEvent> {
    async_stream::stream! {
        while let Some(message) = substream.next().await {
            match message {
                Ok((message, protocol)) => {
                    // reset keep alive idle timeout
                    yield ConnectionHandlerEvent::NotifyBehaviour(HandlerEvent::Message { message, protocol });
                }
                Err(error) => match error {
                    BitswapHandlerError::MaxTransmissionSize => {
                        warn!("Message exceeded the maximum transmission size");
                    }
                    _ => {
                        warn!("Inbound stream error: {}", error);
                        // More serious errors, close this side of the stream. If the
                        // peer is still around, they will re-establish their connection

                        yield ConnectionHandlerEvent::Close(error);
                        break;
                    }
                }
            }
        }

        // All responses received, close the stream.
        if let Err(err) = substream.flush().await {
            warn!("failed to flush stream: {:?}", err);
        }
        if let Err(err) = substream.close().await {
            warn!("failed to close stream: {:?}", err);
        }
    }
}

fn outbound_substream(
    mut substream: Framed<libp2p::Stream, BitswapCodec>,
    (message, response): (BitswapMessage, BitswapMessageResponse),
) -> impl Stream<Item = BitswapConnectionHandlerEvent> {
    async_stream::stream! {
        if let Err(error) = substream.feed(message).await {
            warn!("failed to write item: {:?}", error);
            response.send(Err(network::SendError::Other(error.to_string()))).ok();
            yield ConnectionHandlerEvent::NotifyBehaviour(
                HandlerEvent::FailedToSendMessage { error }
            );
        } else {
            // Message sent
            response.send(Ok(())).ok();
        }

        if let Err(err) = substream.flush().await {
            warn!("failed to flush stream: {:?}", err);
        }

        if let Err(err) = substream.close().await {
            warn!("failed to close stream: {:?}", err);
        }
    }
}
