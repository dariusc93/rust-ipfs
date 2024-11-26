use futures::{FutureExt, StreamExt};
mod codec;

use crate::p2p::request_response::codec::Codec;
use crate::p2p::RequestResponseConfig;
use crate::Multiaddr;
use bytes::Bytes;
use futures::channel::mpsc::Sender as MpscSender;
use futures::channel::oneshot::{Receiver as OneshotReceiver, Sender as OneshotSender};
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::{pin_mut, TryFutureExt};
use libp2p::core::transport::PortUse;
use libp2p::core::Endpoint;
use libp2p::request_response::{
    InboundFailure, InboundRequestId, OutboundFailure, OutboundRequestId, ProtocolSupport,
    ResponseChannel,
};
use libp2p::swarm::{
    ConnectionDenied, ConnectionId, FromSwarm, NetworkBehaviour, THandler, THandlerInEvent,
    THandlerOutEvent, ToSwarm,
};
use libp2p::{request_response, PeerId, StreamProtocol};
use pollable_map::futures::FutureMap;
use std::collections::HashMap;
use std::task::{Context, Poll};
use std::time::Duration;

pub struct Behaviour {
    pending_request: HashMap<PeerId, HashMap<InboundRequestId, ResponseChannel<Bytes>>>,
    awaiting_response: FutureMap<(PeerId, InboundRequestId), OneshotReceiver<Bytes>>,

    pending_response:
        HashMap<PeerId, HashMap<OutboundRequestId, OneshotSender<std::io::Result<Bytes>>>>,
    broadcast_request: Vec<MpscSender<(PeerId, Bytes, OneshotSender<Bytes>)>>,
    rr_behaviour: request_response::Behaviour<Codec>,

    channel_buffer: usize,
}

impl Behaviour {
    pub fn new(config: RequestResponseConfig) -> Self {
        let mut cfg = request_response::Config::default()
            .with_request_timeout(config.timeout.unwrap_or(Duration::from_secs(120)));
        if let Some(size) = config.concurrent_streams {
            cfg = cfg.with_max_concurrent_streams(size);
        }

        let protocol = config.protocol;

        let protocol = vec![(
            StreamProtocol::try_from_owned(protocol).expect("valid protocol"),
            ProtocolSupport::Full,
        )];

        let codec = Codec::new(config.max_request_size, config.max_response_size);

        let rr_behaviour = request_response::Behaviour::with_codec(codec, protocol, cfg);

        Self {
            pending_response: HashMap::new(),
            awaiting_response: FutureMap::new(),
            pending_request: HashMap::new(),
            broadcast_request: Vec::new(),
            rr_behaviour,
            channel_buffer: config.channel_buffer,
        }
    }

    pub fn subscribe(
        &mut self,
    ) -> futures::channel::mpsc::Receiver<(PeerId, Bytes, futures::channel::oneshot::Sender<Bytes>)>
    {
        let (tx, rx) = futures::channel::mpsc::channel(self.channel_buffer);
        self.broadcast_request.push(tx);
        rx
    }

    pub fn send_request(
        &mut self,
        peer_id: PeerId,
        request: Bytes,
    ) -> BoxFuture<'static, std::io::Result<Bytes>> {
        // Since we are only requesting from a single peer, we will only accept one response, if any, from the stream
        let st = self.send_requests([peer_id], request);
        Box::pin(async move {
            pin_mut!(st);
            match st.next().await {
                // Since we are accepting from a single peer, thus would be tracking the peer,
                // we can exclude the peer id from the result.
                Some((_, result)) => result,
                None => Err(std::io::Error::from(std::io::ErrorKind::BrokenPipe)),
            }
        })
    }

    pub fn send_requests(
        &mut self,
        peers: impl IntoIterator<Item = PeerId>,
        request: Bytes,
    ) -> BoxStream<'static, (PeerId, std::io::Result<Bytes>)> {
        let mut oneshots = FutureMap::new();
        for peer_id in peers {
            let id = self.rr_behaviour.send_request(&peer_id, request.clone());
            let (tx, rx) = futures::channel::oneshot::channel();
            self.pending_response
                .entry(peer_id)
                .or_default()
                .insert(id, tx);
            oneshots.insert(
                peer_id,
                rx.map_err(|e| std::io::Error::new(std::io::ErrorKind::BrokenPipe, e))
                    .map(|r| match r {
                        Ok(Ok(bytes)) => Ok(bytes),
                        Ok(Err(e)) => Err(e),
                        Err(e) => Err(e),
                    }),
            );
        }
        oneshots.boxed()
    }

    fn process_request(
        &mut self,
        id: InboundRequestId,
        peer_id: PeerId,
        request: Bytes,
        response_channel: ResponseChannel<Bytes>,
    ) {
        self.broadcast_request.retain(|tx| !tx.is_closed());

        if self.broadcast_request.is_empty() {
            // If the node is not listening in to requests then we should drop the response so it would timeout
            _ = request;
            return;
        }

        self.pending_request
            .entry(peer_id)
            .or_default()
            .insert(id, response_channel);

        for tx in self.broadcast_request.iter_mut() {
            let (rtx, rrx) = futures::channel::oneshot::channel();
            if let Err(_e) = tx.try_send((peer_id, request.clone(), rtx)) {
                // TODO: channel is full or closed
                continue;
            }
            self.awaiting_response.insert((peer_id, id), rrx);
        }
    }

    fn process_response(&mut self, id: OutboundRequestId, peer_id: PeerId, response: Bytes) {
        let Some(list) = self.pending_response.get_mut(&peer_id) else {
            return;
        };

        let ch = list.remove(&id);

        if let Some(ch) = ch {
            let _ = ch.send(Ok(response));
        }
    }

    fn process_outbound_failure(
        &mut self,
        id: OutboundRequestId,
        peer_id: PeerId,
        error: OutboundFailure,
    ) {
        if let Some(list) = self.pending_response.get_mut(&peer_id) {
            if let Some(tx) = list.remove(&id) {
                _ = tx.send(Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    error,
                )));
            }
        }
    }
    fn process_inbound_failure(
        &mut self,
        id: InboundRequestId,
        peer_id: PeerId,
        _: InboundFailure,
    ) {
        if let Some(list) = self.pending_request.get_mut(&peer_id) {
            list.remove(&id);
        }
    }
}

impl NetworkBehaviour for Behaviour {
    type ConnectionHandler =
        <request_response::Behaviour<Codec> as NetworkBehaviour>::ConnectionHandler;
    type ToSwarm = void::Void;

    fn handle_pending_inbound_connection(
        &mut self,
        connection_id: ConnectionId,
        local_addr: &Multiaddr,
        remote_addr: &Multiaddr,
    ) -> Result<(), ConnectionDenied> {
        self.rr_behaviour
            .handle_pending_inbound_connection(connection_id, local_addr, remote_addr)
    }

    fn handle_pending_outbound_connection(
        &mut self,
        connection_id: ConnectionId,
        maybe_peer: Option<PeerId>,
        addresses: &[Multiaddr],
        effective_role: Endpoint,
    ) -> Result<Vec<Multiaddr>, ConnectionDenied> {
        self.rr_behaviour.handle_pending_outbound_connection(
            connection_id,
            maybe_peer,
            addresses,
            effective_role,
        )
    }

    fn handle_established_inbound_connection(
        &mut self,
        connection_id: ConnectionId,
        peer: PeerId,
        local_addr: &Multiaddr,
        remote_addr: &Multiaddr,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        self.rr_behaviour.handle_established_inbound_connection(
            connection_id,
            peer,
            local_addr,
            remote_addr,
        )
    }

    fn handle_established_outbound_connection(
        &mut self,
        connection_id: ConnectionId,
        peer: PeerId,
        addr: &Multiaddr,
        role_override: Endpoint,
        reuse: PortUse,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        self.rr_behaviour.handle_established_outbound_connection(
            connection_id,
            peer,
            addr,
            role_override,
            reuse,
        )
    }

    fn on_connection_handler_event(
        &mut self,
        peer_id: PeerId,
        connection_id: ConnectionId,
        event: THandlerOutEvent<Self>,
    ) {
        self.rr_behaviour
            .on_connection_handler_event(peer_id, connection_id, event)
    }

    fn on_swarm_event(&mut self, event: FromSwarm) {
        self.rr_behaviour.on_swarm_event(event);
    }

    fn poll(&mut self, cx: &mut Context) -> Poll<ToSwarm<Self::ToSwarm, THandlerInEvent<Self>>> {
        while let Poll::Ready(Some(((peer_id, id), res))) =
            self.awaiting_response.poll_next_unpin(cx)
        {
            let response = match res {
                Ok(data) => data,
                Err(_) => {
                    tracing::warn!(%id, %peer_id, "response channel has been dropped. Ignoring");
                    continue;
                }
            };

            if let Some(responses) = self.pending_request.get_mut(&peer_id) {
                if let Some(ch) = responses.remove(&id) {
                    if self.rr_behaviour.send_response(ch, response).is_err() {
                        tracing::warn!(%id, %peer_id, "error sending a response");
                    }
                }
            }
        }

        while let Poll::Ready(event) = self.rr_behaviour.poll(cx) {
            match event {
                ToSwarm::GenerateEvent(request_response::Event::Message {
                    peer: peer_id,
                    message,
                }) => match message {
                    request_response::Message::Response {
                        request_id,
                        response,
                    } => self.process_response(request_id, peer_id, response),
                    request_response::Message::Request {
                        request_id,
                        request,
                        channel,
                    } => {
                        self.process_request(request_id, peer_id, request, channel);
                    }
                },
                ToSwarm::GenerateEvent(request_response::Event::ResponseSent {
                    peer: peer_id,
                    request_id,
                }) => {
                    tracing::trace!(%peer_id, %request_id, "response sent");
                }
                ToSwarm::GenerateEvent(request_response::Event::OutboundFailure {
                    peer,
                    request_id,
                    error,
                }) => {
                    tracing::error!(peer_id = %peer, %request_id, ?error, direction="outbound");
                    self.process_outbound_failure(request_id, peer, error);
                }
                ToSwarm::GenerateEvent(request_response::Event::InboundFailure {
                    peer,
                    request_id,
                    error,
                }) => {
                    tracing::error!(peer_id = %peer, %request_id, ?error, direction="inbound");
                    self.process_inbound_failure(request_id, peer, error);
                }
                other => {
                    let new_to_swarm =
                        other.map_out(|_| unreachable!("we manually map `GenerateEvent` variants"));
                    return Poll::Ready(new_to_swarm);
                }
            };
        }

        Poll::Pending
    }
}
