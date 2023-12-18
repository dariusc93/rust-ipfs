//! Handles the `/ipfs/bitswap/1.0.0` and `/ipfs/bitswap/1.1.0` protocols. This
//! allows exchanging IPFS blocks.
//!
//! # Usage
//!
//! The `Bitswap` struct implements the `NetworkBehaviour` trait. When used, it
//! will allow providing and reciving IPFS blocks.
#[cfg(feature = "compat")]
use crate::compat::{CompatMessage, CompatProtocol, InboundMessage};
use crate::protocol::{
    BitswapCodec, BitswapRequest, BitswapResponse, RequestType, LIBP2P_BITSWAP_PROTOCOL,
};
use crate::query::{QueryEvent, QueryId, QueryManager, Request, Response};
use crate::stats::*;
use fnv::FnvHashMap;
#[cfg(feature = "compat")]
use fnv::FnvHashSet;
use futures::future::BoxFuture;
use futures::{
    channel::mpsc,
    stream::{Stream, StreamExt},
    task::{Context, Poll},
};
use libipld::{error::BlockNotFound, store::StoreParams, Block, Cid, Result};
use libp2p::core::{Endpoint, Multiaddr};
use libp2p::identity::PeerId;

use libp2p::swarm::behaviour::ConnectionEstablished;
#[cfg(feature = "compat")]
use libp2p::swarm::derive_prelude::Either;
use libp2p::swarm::{
    derive_prelude::{ConnectionClosed, FromSwarm},
    ConnectionId, THandlerInEvent,
};
use libp2p::swarm::{ConnectionDenied, THandler};
#[cfg(feature = "compat")]
use libp2p::swarm::{ConnectionHandlerSelect, NotifyHandler, OneShotHandler};
use libp2p::{
    request_response::{
        Behaviour as RequestResponse, Config as RequestResponseConfig,
        Event as RequestResponseEvent, InboundFailure, InboundRequestId,
        Message as RequestResponseMessage, OutboundFailure, OutboundRequestId, ProtocolSupport,
        ResponseChannel,
    },
    swarm::{ConnectionHandler, NetworkBehaviour, ToSwarm},
};
use prometheus::Registry;
use std::{pin::Pin, time::Duration};

/// Bitswap response channel.
pub type Channel = ResponseChannel<BitswapResponse>;

/// Event emitted by the bitswap behaviour.
#[derive(Debug)]
pub enum BitswapEvent {
    /// Received a block from a peer. Includes the number of known missing blocks for a
    /// sync query. When a block is received and missing blocks is not empty the counter
    /// is increased. If missing blocks is empty the counter is decremented.
    Progress(QueryId, usize),
    /// A get or sync query completed.
    Complete(QueryId, Result<()>),
}

/// Trait implemented by a block store.
#[async_trait::async_trait]
pub trait BitswapStore: Send + Sync + 'static {
    /// The store params.
    type Params: StoreParams;
    /// A have query needs to know if the block store contains the block.
    async fn contains(&mut self, cid: &Cid) -> Result<bool>;
    /// A block query needs to retrieve the block from the store.
    async fn get(&mut self, cid: &Cid) -> Result<Option<Vec<u8>>>;
    /// A block response needs to insert the block into the store.
    async fn insert(&mut self, block: &Block<Self::Params>) -> Result<()>;
    /// A sync query needs a list of missing blocks to make progress.
    async fn missing_blocks(&mut self, cid: &Cid) -> Result<Vec<Cid>>;
}

/// Bitswap configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BitswapConfig {
    /// Timeout of a request.
    pub request_timeout: Duration,
}

impl BitswapConfig {
    /// Creates a new `BitswapConfig`.
    pub fn new() -> Self {
        Self {
            request_timeout: Duration::from_secs(10),
        }
    }
}

impl Default for BitswapConfig {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum BitswapId {
    Bitswap(OutboundRequestId),
    #[cfg(feature = "compat")]
    Compat(Cid),
}

enum BitswapChannel {
    Bitswap(Channel),
    #[cfg(feature = "compat")]
    Compat(PeerId, Cid),
}

/// Network behaviour that handles sending and receiving blocks.
pub struct Bitswap<P: StoreParams> {
    /// Inner behaviour.
    inner: RequestResponse<BitswapCodec<P>>,
    /// Query manager.
    query_manager: QueryManager,
    /// Requests.
    requests: FnvHashMap<BitswapId, QueryId>,
    /// Db request channel.
    db_tx: mpsc::UnboundedSender<DbRequest<P>>,
    /// Db response channel.
    db_rx: mpsc::UnboundedReceiver<DbResponse>,
    /// Compat peers.
    #[cfg(feature = "compat")]
    compat: FnvHashSet<PeerId>,
}

impl<P: StoreParams> Bitswap<P> {
    /// Creates a new `Bitswap` behaviour.
    pub fn new<S: BitswapStore<Params = P>>(
        config: BitswapConfig,
        store: S,
        executor: Box<dyn FnOnce(BoxFuture<'static, ()>)>,
    ) -> Self {
        let rr_config =
            RequestResponseConfig::default().with_request_timeout(config.request_timeout);
        let protocols = std::iter::once((LIBP2P_BITSWAP_PROTOCOL, ProtocolSupport::Full));
        let inner = RequestResponse::new(protocols, rr_config);
        let (db_tx, db_rx) = start_db_thread(store, executor);
        Self {
            inner,
            query_manager: Default::default(),
            requests: Default::default(),
            db_tx,
            db_rx,
            #[cfg(feature = "compat")]
            compat: Default::default(),
        }
    }

    /// Adds an address for a peer.
    pub fn add_address(&mut self, peer_id: &PeerId, addr: Multiaddr) {
        self.query_manager.add_peer(peer_id);
        self.inner.add_address(peer_id, addr);
    }

    /// Removes an address for a peer.
    pub fn remove_address(&mut self, peer_id: &PeerId, addr: &Multiaddr) {
        self.query_manager.remove_peer(peer_id);
        self.inner.remove_address(peer_id, addr);
    }

    /// Starts a get query with an initial guess of providers.
    pub fn get(&mut self, cid: Cid, peers: impl Iterator<Item = PeerId>) -> QueryId {
        self.query_manager.get(None, cid, peers)
    }

    /// Starts a sync query with an the initial set of missing blocks.
    pub fn sync(
        &mut self,
        cid: Cid,
        peers: Vec<PeerId>,
        missing: impl Iterator<Item = Cid>,
    ) -> QueryId {
        self.query_manager.sync(cid, peers, missing)
    }

    /// Cancels an in progress query. Returns true if a query was cancelled.
    pub fn cancel(&mut self, id: QueryId) -> bool {
        let res = self.query_manager.cancel(id);
        if res {
            REQUESTS_CANCELED.inc();
        }
        res
    }

    /// Registers prometheus metrics.
    pub fn register_metrics(&self, registry: &Registry) -> Result<()> {
        registry.register(Box::new(REQUESTS_TOTAL.clone()))?;
        registry.register(Box::new(REQUEST_DURATION_SECONDS.clone()))?;
        registry.register(Box::new(REQUESTS_CANCELED.clone()))?;
        registry.register(Box::new(BLOCK_NOT_FOUND.clone()))?;
        registry.register(Box::new(PROVIDERS_TOTAL.clone()))?;
        registry.register(Box::new(MISSING_BLOCKS_TOTAL.clone()))?;
        registry.register(Box::new(RECEIVED_BLOCK_BYTES.clone()))?;
        registry.register(Box::new(RECEIVED_INVALID_BLOCK_BYTES.clone()))?;
        registry.register(Box::new(SENT_BLOCK_BYTES.clone()))?;
        registry.register(Box::new(RESPONSES_TOTAL.clone()))?;
        registry.register(Box::new(THROTTLED_INBOUND.clone()))?;
        registry.register(Box::new(THROTTLED_OUTBOUND.clone()))?;
        registry.register(Box::new(OUTBOUND_FAILURE.clone()))?;
        registry.register(Box::new(INBOUND_FAILURE.clone()))?;
        Ok(())
    }
}

enum DbRequest<P: StoreParams> {
    Bitswap(BitswapChannel, BitswapRequest),
    Insert(Block<P>),
    MissingBlocks(QueryId, Cid),
}

enum DbResponse {
    Bitswap(BitswapChannel, BitswapResponse),
    MissingBlocks(QueryId, Result<Vec<Cid>>),
}

fn start_db_thread<S: BitswapStore>(
    mut store: S,
    executor: Box<dyn FnOnce(BoxFuture<'static, ()>)>,
) -> (
    mpsc::UnboundedSender<DbRequest<S::Params>>,
    mpsc::UnboundedReceiver<DbResponse>,
) {
    let (tx, requests) = mpsc::unbounded();
    let (responses, rx) = mpsc::unbounded();
    executor(Box::pin(async move {
        let mut requests: mpsc::UnboundedReceiver<DbRequest<S::Params>> = requests;
        while let Some(request) = requests.next().await {
            match request {
                DbRequest::Bitswap(channel, request) => {
                    let response = match request.ty {
                        RequestType::Have => {
                            let have = store.contains(&request.cid).await.ok().unwrap_or_default();
                            if have {
                                RESPONSES_TOTAL.with_label_values(&["have"]).inc();
                            } else {
                                RESPONSES_TOTAL.with_label_values(&["dont_have"]).inc();
                            }
                            tracing::trace!("have {}", have);
                            BitswapResponse::Have(have)
                        }
                        RequestType::Block => {
                            let block = store.get(&request.cid).await.ok().unwrap_or_default();
                            if let Some(data) = block {
                                RESPONSES_TOTAL.with_label_values(&["block"]).inc();
                                SENT_BLOCK_BYTES.inc_by(data.len() as u64);
                                tracing::trace!("block {}", data.len());
                                BitswapResponse::Block(data)
                            } else {
                                RESPONSES_TOTAL.with_label_values(&["dont_have"]).inc();
                                tracing::trace!("have false");
                                BitswapResponse::Have(false)
                            }
                        }
                    };
                    responses
                        .unbounded_send(DbResponse::Bitswap(channel, response))
                        .ok();
                }
                DbRequest::Insert(block) => {
                    if let Err(err) = store.insert(&block).await {
                        tracing::error!("error inserting blocks {}", err);
                    }
                }
                DbRequest::MissingBlocks(id, cid) => {
                    let res = store.missing_blocks(&cid).await;
                    responses
                        .unbounded_send(DbResponse::MissingBlocks(id, res))
                        .ok();
                }
            }
        }
    }));
    (tx, rx)
}

impl<P: StoreParams> Bitswap<P> {
    /// Processes an incoming bitswap request.
    fn inject_request(&mut self, channel: BitswapChannel, request: BitswapRequest) {
        self.db_tx
            .unbounded_send(DbRequest::Bitswap(channel, request))
            .ok();
    }

    /// Processes an incoming bitswap response.
    fn inject_response(&mut self, id: BitswapId, peer: PeerId, response: BitswapResponse) {
        if let Some(id) = self.requests.remove(&id) {
            match response {
                BitswapResponse::Have(have) => {
                    self.query_manager
                        .inject_response(id, Response::Have(peer, have));
                }
                BitswapResponse::Block(data) => {
                    if let Some(info) = self.query_manager.query_info(id) {
                        let len = data.len();
                        if let Ok(block) = Block::new(info.cid, data) {
                            RECEIVED_BLOCK_BYTES.inc_by(len as u64);
                            self.db_tx.unbounded_send(DbRequest::Insert(block)).ok();
                            self.query_manager
                                .inject_response(id, Response::Block(peer, true));
                        } else {
                            tracing::error!("received invalid block");
                            RECEIVED_INVALID_BLOCK_BYTES.inc_by(len as u64);
                            self.query_manager
                                .inject_response(id, Response::Block(peer, false));
                        }
                    }
                }
            }
        }
    }

    fn inject_outbound_failure(
        &mut self,
        peer: &PeerId,
        request_id: OutboundRequestId,
        error: &OutboundFailure,
    ) {
        tracing::debug!(
            "bitswap outbound failure {} {} {:?}",
            peer,
            request_id,
            error
        );
        match error {
            OutboundFailure::DialFailure => {
                OUTBOUND_FAILURE.with_label_values(&["dial_failure"]).inc();
            }
            OutboundFailure::Timeout => {
                OUTBOUND_FAILURE.with_label_values(&["timeout"]).inc();
            }
            OutboundFailure::ConnectionClosed => {
                OUTBOUND_FAILURE
                    .with_label_values(&["connection_closed"])
                    .inc();
            }
            OutboundFailure::UnsupportedProtocols => {
                OUTBOUND_FAILURE
                    .with_label_values(&["unsupported_protocols"])
                    .inc();
            }
            OutboundFailure::Io(_) => {
                OUTBOUND_FAILURE.with_label_values(&["io"]).inc();
            }
        }
    }

    fn inject_inbound_failure(
        &mut self,
        peer: &PeerId,
        request_id: InboundRequestId,
        error: &InboundFailure,
    ) {
        tracing::error!(
            "bitswap inbound failure {} {} {:?}",
            peer,
            request_id,
            error
        );
        match error {
            InboundFailure::Timeout => {
                INBOUND_FAILURE.with_label_values(&["timeout"]).inc();
            }
            InboundFailure::ConnectionClosed => {
                INBOUND_FAILURE
                    .with_label_values(&["connection_closed"])
                    .inc();
            }
            InboundFailure::UnsupportedProtocols => {
                INBOUND_FAILURE
                    .with_label_values(&["unsupported_protocols"])
                    .inc();
            }
            InboundFailure::ResponseOmission => {
                INBOUND_FAILURE
                    .with_label_values(&["response_omission"])
                    .inc();
            }
            InboundFailure::Io(_) => {
                INBOUND_FAILURE.with_label_values(&["io"]).inc();
            }
        }
    }
}

impl<P: StoreParams> NetworkBehaviour for Bitswap<P> {
    #[cfg(not(feature = "compat"))]
    type ConnectionHandler =
        <RequestResponse<BitswapCodec<P>> as NetworkBehaviour>::ConnectionHandler;

    #[cfg(feature = "compat")]
    #[allow(clippy::type_complexity)]
    type ConnectionHandler = ConnectionHandlerSelect<
        <RequestResponse<BitswapCodec<P>> as NetworkBehaviour>::ConnectionHandler,
        OneShotHandler<CompatProtocol, CompatMessage, InboundMessage>,
    >;
    type ToSwarm = BitswapEvent;

    fn handle_established_inbound_connection(
        &mut self,
        connection_id: ConnectionId,
        peer: PeerId,
        local_addr: &Multiaddr,
        remote_addr: &Multiaddr,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        #[cfg(not(feature = "compat"))]
        return self.inner.handle_established_inbound_connection(
            connection_id,
            peer,
            local_addr,
            remote_addr,
        );
        #[cfg(feature = "compat")]
        return self
            .inner
            .handle_established_inbound_connection(connection_id, peer, local_addr, remote_addr)
            .map(|handle| ConnectionHandler::select(handle, OneShotHandler::default()));
    }

    fn handle_established_outbound_connection(
        &mut self,
        connection_id: ConnectionId,
        peer: PeerId,
        addr: &Multiaddr,
        role_override: Endpoint,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        #[cfg(not(feature = "compat"))]
        return self.inner.handle_established_outbound_connection(
            connection_id,
            peer,
            addr,
            role_override,
        );
        #[cfg(feature = "compat")]
        return self
            .inner
            .handle_established_outbound_connection(connection_id, peer, addr, role_override)
            .map(|handle| ConnectionHandler::select(handle, OneShotHandler::default()));
    }

    fn handle_pending_outbound_connection(
        &mut self,
        connection_id: ConnectionId,
        maybe_peer: Option<PeerId>,
        addresses: &[Multiaddr],
        effective_role: Endpoint,
    ) -> Result<Vec<Multiaddr>, ConnectionDenied> {
        self.inner.handle_pending_outbound_connection(
            connection_id,
            maybe_peer,
            addresses,
            effective_role,
        )
    }

    fn on_swarm_event(&mut self, event: FromSwarm) {
        match event {
            FromSwarm::ConnectionEstablished(ConnectionEstablished {
                peer_id,
                other_established,
                ..
            }) => {
                if other_established == 0 {
                    self.query_manager.add_peer(&peer_id);
                }
            }
            FromSwarm::ConnectionClosed(ConnectionClosed {
                peer_id,
                connection_id,
                endpoint,
                remaining_established,
            }) => {
                #[cfg(feature = "compat")]
                if remaining_established == 0 {
                    self.compat.remove(&peer_id);
                }
                if remaining_established == 0 {
                    self.query_manager.remove_peer(&peer_id);
                }
                self.inner
                    .on_swarm_event(FromSwarm::ConnectionClosed(ConnectionClosed {
                        peer_id,
                        connection_id,
                        endpoint,
                        remaining_established,
                    }));
            }
            ev => self.inner.on_swarm_event(ev),
        }
    }

    fn on_connection_handler_event(
        &mut self,
        peer_id: PeerId,
        conn: ConnectionId,
        event: <Self::ConnectionHandler as ConnectionHandler>::ToBehaviour,
    ) {
        tracing::trace!(?event, "on_connection_handler_event");
        #[cfg(not(feature = "compat"))]
        return self.inner.on_connection_handler_event(peer_id, conn, event);
        #[cfg(feature = "compat")]
        match event {
            Either::Left(event) => self.inner.on_connection_handler_event(peer_id, conn, event),
            Either::Right(msg) => {
                for msg in msg.unwrap().0 {
                    match msg {
                        CompatMessage::Request(req) => {
                            tracing::trace!("received compat request");
                            self.inject_request(BitswapChannel::Compat(peer_id, req.cid), req);
                        }
                        CompatMessage::Response(cid, res) => {
                            tracing::trace!("received compat response");
                            self.inject_response(BitswapId::Compat(cid), peer_id, res);
                        }
                    }
                }
            }
        }
    }

    fn poll(&mut self, cx: &mut Context) -> Poll<ToSwarm<Self::ToSwarm, THandlerInEvent<Self>>> {
        let mut exit = false;
        while !exit {
            exit = true;
            while let Poll::Ready(Some(response)) = Pin::new(&mut self.db_rx).poll_next(cx) {
                exit = false;
                match response {
                    DbResponse::Bitswap(channel, response) => match channel {
                        BitswapChannel::Bitswap(channel) => {
                            self.inner.send_response(channel, response).ok();
                        }
                        #[cfg(feature = "compat")]
                        BitswapChannel::Compat(peer_id, cid) => {
                            let compat = CompatMessage::Response(cid, response);
                            return Poll::Ready(ToSwarm::NotifyHandler {
                                peer_id,
                                handler: NotifyHandler::Any,
                                event: Either::Right(compat),
                            });
                        }
                    },
                    DbResponse::MissingBlocks(id, res) => match res {
                        Ok(missing) => {
                            MISSING_BLOCKS_TOTAL.inc_by(missing.len() as u64);
                            self.query_manager
                                .inject_response(id, Response::MissingBlocks(missing));
                        }
                        Err(err) => {
                            self.query_manager.cancel(id);
                            let event = BitswapEvent::Complete(id, Err(err));
                            return Poll::Ready(ToSwarm::GenerateEvent(event));
                        }
                    },
                }
            }
            while let Some(query) = self.query_manager.next() {
                exit = false;
                match query {
                    QueryEvent::Request(id, req) => match req {
                        Request::Have(peer_id, cid) => {
                            let req = BitswapRequest {
                                ty: RequestType::Have,
                                cid,
                            };
                            let rid = self.inner.send_request(&peer_id, req);
                            self.requests.insert(BitswapId::Bitswap(rid), id);
                        }
                        Request::Block(peer_id, cid) => {
                            let req = BitswapRequest {
                                ty: RequestType::Block,
                                cid,
                            };
                            let rid = self.inner.send_request(&peer_id, req);
                            self.requests.insert(BitswapId::Bitswap(rid), id);
                        }
                        Request::MissingBlocks(cid) => {
                            self.db_tx
                                .unbounded_send(DbRequest::MissingBlocks(id, cid))
                                .ok();
                        }
                    },
                    QueryEvent::Progress(id, missing) => {
                        let event = BitswapEvent::Progress(id, missing);
                        return Poll::Ready(ToSwarm::GenerateEvent(event));
                    }
                    QueryEvent::Complete(id, res) => {
                        if res.is_err() {
                            BLOCK_NOT_FOUND.inc();
                        }
                        let event = BitswapEvent::Complete(
                            id,
                            res.map_err(|cid| BlockNotFound(cid).into()),
                        );
                        return Poll::Ready(ToSwarm::GenerateEvent(event));
                    }
                }
            }
            while let Poll::Ready(event) = self.inner.poll(cx) {
                exit = false;
                let event = match event {
                    ToSwarm::GenerateEvent(event) => event,
                    ToSwarm::Dial { opts } => {
                        return Poll::Ready(ToSwarm::Dial { opts });
                    }
                    ToSwarm::NotifyHandler {
                        peer_id,
                        handler,
                        event,
                    } => {
                        return Poll::Ready(ToSwarm::NotifyHandler {
                            peer_id,
                            handler,
                            #[cfg(not(feature = "compat"))]
                            event,
                            #[cfg(feature = "compat")]
                            event: Either::Left(event),
                        });
                    }
                    ToSwarm::ListenOn { opts } => {
                        return Poll::Ready(ToSwarm::ListenOn { opts });
                    }
                    ToSwarm::RemoveListener { id } => {
                        return Poll::Ready(ToSwarm::RemoveListener { id });
                    }
                    ToSwarm::NewExternalAddrCandidate(address) => {
                        return Poll::Ready(ToSwarm::NewExternalAddrCandidate(address));
                    }
                    ToSwarm::ExternalAddrConfirmed(address) => {
                        return Poll::Ready(ToSwarm::ExternalAddrConfirmed(address));
                    }
                    ToSwarm::ExternalAddrExpired(address) => {
                        return Poll::Ready(ToSwarm::ExternalAddrExpired(address));
                    }
                    ToSwarm::CloseConnection {
                        peer_id,
                        connection,
                    } => {
                        return Poll::Ready(ToSwarm::CloseConnection {
                            peer_id,
                            connection,
                        });
                    }
                    _ => todo!(),
                };
                match event {
                    RequestResponseEvent::Message { peer, message } => match message {
                        RequestResponseMessage::Request {
                            request_id: _,
                            request,
                            channel,
                        } => self.inject_request(BitswapChannel::Bitswap(channel), request),
                        RequestResponseMessage::Response {
                            request_id,
                            response,
                        } => self.inject_response(BitswapId::Bitswap(request_id), peer, response),
                    },
                    RequestResponseEvent::ResponseSent { .. } => {}
                    RequestResponseEvent::OutboundFailure {
                        peer,
                        request_id,
                        error,
                    } => {
                        self.inject_outbound_failure(&peer, request_id, &error);
                        #[cfg(feature = "compat")]
                        if let OutboundFailure::UnsupportedProtocols = error {
                            if let Some(id) = self.requests.remove(&BitswapId::Bitswap(request_id))
                            {
                                if let Some(info) = self.query_manager.query_info(id) {
                                    let ty = match info.label {
                                        "have" => RequestType::Have,
                                        "block" => RequestType::Block,
                                        _ => unreachable!(),
                                    };
                                    let request = BitswapRequest { ty, cid: info.cid };
                                    self.requests.insert(BitswapId::Compat(info.cid), id);
                                    tracing::trace!("adding compat peer {}", peer);
                                    self.compat.insert(peer);
                                    return Poll::Ready(ToSwarm::NotifyHandler {
                                        peer_id: peer,
                                        handler: NotifyHandler::Any,
                                        event: Either::Right(CompatMessage::Request(request)),
                                    });
                                }
                            }
                        }
                        if let Some(id) = self.requests.remove(&BitswapId::Bitswap(request_id)) {
                            self.query_manager
                                .inject_response(id, Response::Have(peer, false));
                        }
                    }
                    RequestResponseEvent::InboundFailure {
                        peer,
                        request_id,
                        error,
                    } => {
                        self.inject_inbound_failure(&peer, request_id, &error);
                    }
                }
            }
        }
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_std::task;
    use futures::prelude::*;
    use libipld::block::Block;
    use libipld::cbor::DagCborCodec;
    use libipld::ipld;
    use libipld::ipld::Ipld;
    use libipld::multihash::Code;
    use libipld::store::DefaultParams;
    use libp2p::swarm::SwarmEvent;
    use libp2p::SwarmBuilder;
    use libp2p::{PeerId, Swarm};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tracing_subscriber::fmt::TestWriter;

    fn tracing_try_init() {
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_writer(TestWriter::new())
            .try_init()
            .ok();
    }

    fn create_block(ipld: Ipld) -> Block<DefaultParams> {
        Block::encode(DagCborCodec, Code::Blake3_256, &ipld).unwrap()
    }

    #[derive(Clone, Default)]
    struct Store(Arc<Mutex<FnvHashMap<Cid, Vec<u8>>>>);

    #[async_trait::async_trait]
    impl BitswapStore for Store {
        type Params = DefaultParams;
        async fn contains(&mut self, cid: &Cid) -> Result<bool> {
            Ok(self.0.lock().unwrap().contains_key(cid))
        }
        async fn get(&mut self, cid: &Cid) -> Result<Option<Vec<u8>>> {
            Ok(self.0.lock().unwrap().get(cid).cloned())
        }
        async fn insert(&mut self, block: &Block<Self::Params>) -> Result<()> {
            self.0
                .lock()
                .unwrap()
                .insert(*block.cid(), block.data().to_vec());
            Ok(())
        }
        async fn missing_blocks(&mut self, cid: &Cid) -> Result<Vec<Cid>> {
            let mut stack = vec![*cid];
            let mut missing = vec![];
            while let Some(cid) = stack.pop() {
                if let Some(data) = self.get(&cid).await? {
                    let block = Block::<Self::Params>::new_unchecked(cid, data);
                    block.references(&mut stack)?;
                } else {
                    missing.push(cid);
                }
            }
            Ok(missing)
        }
    }

    struct Peer {
        peer_id: PeerId,
        addr: Multiaddr,
        store: Store,
        swarm: Swarm<Bitswap<DefaultParams>>,
    }

    impl Peer {
        fn new() -> Self {
            let store = Store::default();
            let mut swarm = SwarmBuilder::with_new_identity()
                .with_async_std()
                .with_tcp(
                    Default::default(),
                    libp2p::noise::Config::new,
                    libp2p::yamux::Config::default,
                )
                .expect("valid")
                .with_behaviour(|_| {
                    Bitswap::new(
                        BitswapConfig::new(),
                        store.clone(),
                        Box::new(|t| {
                            async_std::task::spawn(t);
                        }),
                    )
                })
                .expect("Valid")
                .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(60)))
                .build();
            let peer_id = *swarm.local_peer_id();
            Swarm::listen_on(&mut swarm, "/ip4/127.0.0.1/tcp/0".parse().unwrap()).unwrap();
            while swarm.next().now_or_never().is_some() {}
            let addr = Swarm::listeners(&swarm).next().unwrap().clone();
            Self {
                peer_id,
                addr,
                store,
                swarm,
            }
        }

        fn add_address(&mut self, peer: &Peer) {
            self.swarm
                .behaviour_mut()
                .add_address(&peer.peer_id, peer.addr.clone());
        }

        fn store(&mut self) -> impl std::ops::DerefMut<Target = FnvHashMap<Cid, Vec<u8>>> + '_ {
            self.store.0.lock().unwrap()
        }

        fn swarm(&mut self) -> &mut Swarm<Bitswap<DefaultParams>> {
            &mut self.swarm
        }

        fn spawn(mut self, name: &'static str) -> PeerId {
            let peer_id = self.peer_id;
            task::spawn(async move {
                loop {
                    let event = self.swarm.next().await;
                    tracing::debug!("{}: {:?}", name, event);
                }
            });
            peer_id
        }

        async fn next(&mut self) -> Option<BitswapEvent> {
            loop {
                let ev = self.swarm.next().await?;
                if let SwarmEvent::Behaviour(event) = ev {
                    return Some(event);
                }
            }
        }
    }

    fn assert_progress(event: Option<BitswapEvent>, id: QueryId, missing: usize) {
        if let Some(BitswapEvent::Progress(id2, missing2)) = event {
            assert_eq!(id2, id);
            assert_eq!(missing2, missing);
        } else {
            panic!("{:?} is not a progress event", event);
        }
    }

    fn assert_complete_ok(event: Option<BitswapEvent>, id: QueryId) {
        if let Some(BitswapEvent::Complete(id2, Ok(()))) = event {
            assert_eq!(id2, id);
        } else {
            panic!("{:?} is not a complete event", event);
        }
    }

    #[async_std::test]
    async fn test_bitswap_get() {
        tracing_try_init();
        let mut peer1 = Peer::new();
        let mut peer2 = Peer::new();
        peer2.add_address(&peer1);

        let block = create_block(ipld!(&b"hello world"[..]));
        peer1.store().insert(*block.cid(), block.data().to_vec());
        let peer1 = peer1.spawn("peer1");

        let id = peer2
            .swarm()
            .behaviour_mut()
            .get(*block.cid(), std::iter::once(peer1));

        assert_complete_ok(peer2.next().await, id);
    }

    #[async_std::test]
    async fn test_bitswap_cancel_get() {
        tracing_try_init();
        let mut peer1 = Peer::new();
        let mut peer2 = Peer::new();
        peer2.add_address(&peer1);

        let block = create_block(ipld!(&b"hello world"[..]));
        peer1.store().insert(*block.cid(), block.data().to_vec());
        let peer1 = peer1.spawn("peer1");

        let id = peer2
            .swarm()
            .behaviour_mut()
            .get(*block.cid(), std::iter::once(peer1));
        peer2.swarm().behaviour_mut().cancel(id);
        let res = peer2.next().now_or_never();
        println!("{:?}", res);
        assert!(res.is_none());
    }

    #[async_std::test]
    async fn test_bitswap_sync() {
        tracing_try_init();
        let mut peer1 = Peer::new();
        let mut peer2 = Peer::new();
        peer2.add_address(&peer1);

        let b0 = create_block(ipld!({
            "n": 0,
        }));
        let b1 = create_block(ipld!({
            "prev": b0.cid(),
            "n": 1,
        }));
        let b2 = create_block(ipld!({
            "prev": b1.cid(),
            "n": 2,
        }));
        peer1.store().insert(*b0.cid(), b0.data().to_vec());
        peer1.store().insert(*b1.cid(), b1.data().to_vec());
        peer1.store().insert(*b2.cid(), b2.data().to_vec());
        let peer1 = peer1.spawn("peer1");

        let id =
            peer2
                .swarm()
                .behaviour_mut()
                .sync(*b2.cid(), vec![peer1], std::iter::once(*b2.cid()));

        assert_progress(peer2.next().await, id, 1);
        assert_progress(peer2.next().await, id, 1);

        assert_complete_ok(peer2.next().await, id);
    }

    #[async_std::test]
    async fn test_bitswap_cancel_sync() {
        tracing_try_init();
        let mut peer1 = Peer::new();
        let mut peer2 = Peer::new();
        peer2.add_address(&peer1);

        let block = create_block(ipld!(&b"hello world"[..]));
        peer1.store().insert(*block.cid(), block.data().to_vec());
        let peer1 = peer1.spawn("peer1");

        let id = peer2.swarm().behaviour_mut().sync(
            *block.cid(),
            vec![peer1],
            std::iter::once(*block.cid()),
        );
        peer2.swarm().behaviour_mut().cancel(id);
        let res = peer2.next().now_or_never();
        println!("{:?}", res);
        assert!(res.is_none());
    }

    #[cfg(feature = "compat")]
    #[async_std::test]
    async fn compat_test() {
        tracing_try_init();
        let cid: Cid = "QmP8njGuyiw9cjkhwHD9nZhyBTHufXFanAvZgcy9xYoWiB"
            .parse()
            .unwrap();
        let peer_id: PeerId = "QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ"
            .parse()
            .unwrap();
        let multiaddr: Multiaddr = "/ip4/104.131.131.82/tcp/4001".parse().unwrap();

        let mut peer = Peer::new();
        peer.swarm()
            .behaviour_mut()
            .add_address(&peer_id, multiaddr);
        let id = peer
            .swarm()
            .behaviour_mut()
            .get(cid, std::iter::once(peer_id));
        assert_complete_ok(peer.next().await, id);
    }
}
