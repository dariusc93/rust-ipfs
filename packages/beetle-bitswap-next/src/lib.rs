//! Implements handling of the [bitswap protocol]((https://github.com/ipfs/specs/blob/master/BITSWAP.md)). Based on go-ipfs.
//!
//! Supports the versions `1.0.0`, `1.1.0` and `1.2.0`.

use std::collections::HashSet;
use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use ahash::AHashMap;
use anyhow::Result;
use async_trait::async_trait;
use cid::Cid;
use handler::{BitswapHandler, HandlerEvent};

use libp2p::swarm::derive_prelude::ConnectionEstablished;
use libp2p::swarm::dial_opts::DialOpts;
use libp2p::swarm::{ConnectionClosed, ConnectionId, DialFailure, FromSwarm};
use libp2p::swarm::{
    ConnectionDenied, DialError, NetworkBehaviour, NotifyHandler, PollParameters, THandler,
    THandlerInEvent, ToSwarm,
};
use libp2p::{Multiaddr, PeerId, StreamProtocol};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tracing::{debug, trace, warn};

use self::client::{Client, Config as ClientConfig};
use self::message::BitswapMessage;
use self::network::Network;
use self::network::OutEvent;
pub use self::protocol::ProtocolConfig;
pub use self::server::{Config as ServerConfig, Server};

mod block;
mod client;
mod error;
mod handler;
mod network;
mod pb;
mod prefix;
mod protocol;
mod server;

pub mod message;
pub mod peer_task_queue;

pub use self::block::{tests::*, Block};
pub use self::protocol::ProtocolId;

const DIAL_BACK_OFF: Duration = Duration::from_secs(10 * 60);

type DialMap = AHashMap<
    PeerId,
    Vec<(
        usize,
        oneshot::Sender<std::result::Result<(ConnectionId, Option<ProtocolId>), String>>,
    )>,
>;

#[derive(Debug)]
pub struct Bitswap<S: Store> {
    network: Network,
    protocol_config: ProtocolConfig,
    peers: AHashMap<PeerId, PeerState>,
    dials: DialMap,
    /// Set to true when dialing should be disabled because we have reached the conn limit.
    pause_dialing: bool,
    client: Client<S>,
    server: Option<Server<S>>,
    incoming_messages: mpsc::Sender<(PeerId, BitswapMessage)>,
    peers_connected: mpsc::Sender<PeerId>,
    peers_disconnected: mpsc::Sender<PeerId>,
    _workers: Arc<Vec<JoinHandle<()>>>,
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
enum PeerState {
    Connected(ConnectionId),
    Responsive(ConnectionId, ProtocolId),
    Unresponsive,
    #[default]
    Disconnected,
    DialFailure(Instant),
}

impl PeerState {
    fn is_connected(self) -> bool {
        matches!(self, PeerState::Connected(_) | PeerState::Responsive(_, _))
    }
}

#[derive(Debug)]
pub struct Config {
    pub client: ClientConfig,
    /// If no server config is set, the server is disabled.
    pub server: Option<ServerConfig>,
    pub protocol: ProtocolConfig,
    pub idle_timeout: Duration,
}

impl Config {
    pub fn default_client_mode() -> Self {
        Config {
            server: None,
            ..Default::default()
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Config {
            client: ClientConfig::default(),
            server: Some(ServerConfig::default()),
            protocol: ProtocolConfig::default(),
            idle_timeout: Duration::from_secs(30),
        }
    }
}

#[async_trait]
pub trait Store: Debug + Clone + Send + Sync + 'static {
    async fn get_size(&self, cid: &Cid) -> Result<usize>;
    async fn get(&self, cid: &Cid) -> Result<Block>;
    async fn has(&self, cid: &Cid) -> Result<bool>;
}

impl<S: Store> Bitswap<S> {
    pub async fn new(self_id: PeerId, store: S, config: Config) -> Self {
        let network = Network::new(self_id);
        let (server, cb) = if let Some(config) = config.server {
            let server = Server::new(network.clone(), store.clone(), config).await;
            let cb = server.received_blocks_cb();
            (Some(server), Some(cb))
        } else {
            (None, None)
        };
        let client = Client::new(network.clone(), store, cb, config.client).await;

        let (sender_msg, mut receiver_msg) = mpsc::channel::<(PeerId, BitswapMessage)>(2048);
        let (sender_con, mut receiver_con) = mpsc::channel(2048);
        let (sender_dis, mut receiver_dis) = mpsc::channel(2048);

        let mut workers = Vec::new();
        workers.push(tokio::task::spawn({
            let server = server.clone();
            let client = client.clone();

            async move {
                // process messages serially but without blocking the p2p loop
                while let Some((peer, mut message)) = receiver_msg.recv().await {
                    let message = tokio::task::spawn_blocking(move || {
                        message.verify_blocks();
                        message
                    })
                    .await
                    .expect("cannot spawn blocking thread");
                    if let Some(ref server) = server {
                        futures::future::join(
                            client.receive_message(&peer, &message),
                            server.receive_message(&peer, &message),
                        )
                        .await;
                    } else {
                        client.receive_message(&peer, &message).await;
                    }
                }
            }
        }));

        workers.push(tokio::task::spawn({
            let server = server.clone();
            let client = client.clone();

            async move {
                // process messages serially but without blocking the p2p loop
                while let Some(peer) = receiver_con.recv().await {
                    if let Some(ref server) = server {
                        futures::future::join(
                            client.peer_connected(&peer),
                            server.peer_connected(&peer),
                        )
                        .await;
                    } else {
                        client.peer_connected(&peer).await;
                    }
                }
            }
        }));

        workers.push(tokio::task::spawn({
            let server = server.clone();
            let client = client.clone();

            async move {
                // process messages serially but without blocking the p2p loop
                while let Some(peer) = receiver_dis.recv().await {
                    if let Some(ref server) = server {
                        futures::future::join(
                            client.peer_disconnected(&peer),
                            server.peer_disconnected(&peer),
                        )
                        .await;
                    } else {
                        client.peer_disconnected(&peer).await;
                    }
                }
            }
        }));

        Bitswap {
            network,
            protocol_config: config.protocol,
            peers: Default::default(),
            dials: Default::default(),
            pause_dialing: false,
            server,
            client,
            incoming_messages: sender_msg,
            peers_connected: sender_con,
            peers_disconnected: sender_dis,
            _workers: Arc::new(workers),
        }
    }

    pub fn server(&self) -> Option<&Server<S>> {
        self.server.as_ref()
    }

    pub fn client(&self) -> &Client<S> {
        &self.client
    }

    pub async fn stop(self) -> Result<()> {
        self.network.stop();
        if let Some(server) = self.server {
            futures::future::try_join(self.client.stop(), server.stop()).await?;
        } else {
            self.client.stop().await?;
        }

        Ok(())
    }

    pub async fn notify_new_blocks(&self, blocks: &[Block]) -> Result<()> {
        self.client.notify_new_blocks(blocks).await?;
        if let Some(ref server) = self.server {
            server.notify_new_blocks(blocks).await?;
        }

        Ok(())
    }

    /// Called on identify events from swarm, informing us about available protocols of this peer.
    /// TODO: Check within handler after updating to libp2p 0.52 instead of checking identify
    ///       If peer does not contain the protocol, emit an event from the handler to notify
    ///       the behaviour and make sure to set `keep_alive` to `KeepAlive::No`
    pub fn on_identify(&mut self, peer: &PeerId, protocols: &[StreamProtocol]) {
        if let Some(PeerState::Connected(conn_id)) = self.get_peer_state(peer) {
            let mut protocols: Vec<ProtocolId> =
                protocols.iter().filter_map(ProtocolId::try_from).collect();
            protocols.sort();
            if let Some(best_protocol) = protocols.last() {
                self.set_peer_state(peer, PeerState::Responsive(conn_id, *best_protocol));
            }
        }
    }

    pub async fn wantlist_for_peer(&self, peer: &PeerId) -> Vec<Cid> {
        if peer == self.network.self_id() {
            return self.client.get_wantlist().await.into_iter().collect();
        }

        if let Some(ref server) = self.server {
            server.wantlist_for_peer(peer).await
        } else {
            Vec::new()
        }
    }

    fn peer_connected(&self, peer: PeerId) {
        if let Err(err) = self.peers_connected.try_send(peer) {
            warn!(
                "failed to process peer connection from {}: {:?}, dropping",
                peer, err
            );
        }
    }

    fn peer_disconnected(&self, peer: PeerId) {
        if let Err(err) = self.peers_disconnected.try_send(peer) {
            warn!(
                "failed to process peer disconnection from {}: {:?}, dropping",
                peer, err
            );
        }
    }

    fn receive_message(&self, peer: PeerId, message: BitswapMessage) {
        // TODO: Handle backpressure properly
        if let Err(err) = self.incoming_messages.try_send((peer, message)) {
            warn!(
                "failed to receive message from {}: {:?}, dropping",
                peer, err
            );
        }
    }

    fn get_peer_state(&self, peer: &PeerId) -> Option<PeerState> {
        self.peers.get(peer).copied()
    }

    fn set_peer_state(&mut self, peer: &PeerId, new_state: PeerState) {
        let peer = *peer;
        match self.peers.entry(peer) {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                let old_state = *entry.get();
                // skip non state changes
                if old_state == new_state {
                    return;
                }
                if let PeerState::Connected(old_id) = old_state {
                    if let PeerState::Connected(new_id) = new_state {
                        // TODO: better understand what this means and how to handle it.
                        debug!(
                            "Peer {}: detected connection id change: {:?} => {:?}",
                            peer, old_id, new_id
                        );
                        return;
                    }
                }

                if new_state == PeerState::Disconnected {
                    entry.remove();
                } else {
                    *entry.get_mut() = new_state;
                }
                match new_state {
                    PeerState::DialFailure(_)
                    | PeerState::Disconnected
                    | PeerState::Unresponsive => {
                        if old_state.is_connected() {
                            self.peer_disconnected(peer);
                        }
                    }
                    PeerState::Connected(_) => {
                        // nothing, just recorded until we receive protocol confirmation
                    }
                    PeerState::Responsive(_, _) => {
                        self.peer_connected(peer);
                    }
                }
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                if new_state != PeerState::Disconnected {
                    entry.insert(new_state);
                }
                match new_state {
                    PeerState::DialFailure(_)
                    | PeerState::Disconnected
                    | PeerState::Unresponsive => {
                        self.peer_disconnected(peer);
                    }
                    PeerState::Connected(_) => {}
                    PeerState::Responsive(_, _) => {
                        self.peer_connected(peer);
                    }
                }
            }
        }
    }
}

#[derive(Debug)]
pub enum BitswapEvent {
    /// We have this content, and want it to be provided.
    Provide { key: Cid },
    FindProviders {
        key: Cid,
        response: tokio::sync::mpsc::Sender<std::result::Result<HashSet<PeerId>, String>>,
        limit: usize,
    },
    Ping {
        peer: PeerId,
        response: oneshot::Sender<Option<Duration>>,
    },
}

impl<S: Store> NetworkBehaviour for Bitswap<S> {
    type ConnectionHandler = BitswapHandler;
    type ToSwarm = BitswapEvent;

    fn handle_established_inbound_connection(
        &mut self,
        _connection_id: ConnectionId,
        _: PeerId,
        _: &Multiaddr,
        _: &Multiaddr,
    ) -> std::result::Result<THandler<Self>, ConnectionDenied> {
        let protocol_config = self.protocol_config.clone();
        Ok(BitswapHandler::new(protocol_config))
    }

    fn handle_established_outbound_connection(
        &mut self,
        _connection_id: ConnectionId,
        _: PeerId,
        _: &Multiaddr,
        _: libp2p::core::Endpoint,
    ) -> std::result::Result<THandler<Self>, ConnectionDenied> {
        let protocol_config = self.protocol_config.clone();
        Ok(BitswapHandler::new(protocol_config))
    }

    #[allow(clippy::collapsible_match)]
    fn on_swarm_event(&mut self, event: FromSwarm<Self::ConnectionHandler>) {
        match event {
            FromSwarm::ConnectionEstablished(ConnectionEstablished {
                peer_id,
                connection_id,
                other_established,
                ..
            }) => {
                trace!("connection established {} ({})", peer_id, other_established);
                self.set_peer_state(&peer_id, PeerState::Connected(connection_id));
                self.pause_dialing = false;
            }
            FromSwarm::ConnectionClosed(ConnectionClosed {
                peer_id,
                remaining_established,
                ..
            }) => {
                self.pause_dialing = false;
                if remaining_established == 0 {
                    // Last connection, close it
                    self.set_peer_state(&peer_id, PeerState::Disconnected)
                }
            }
            FromSwarm::DialFailure(DialFailure { peer_id, error, .. }) => {
                if let Some(peer_id) = peer_id {
                    if let DialError::Denied { .. } = error {
                        self.pause_dialing = true;
                        self.set_peer_state(&peer_id, PeerState::Disconnected);
                    } else {
                        self.set_peer_state(&peer_id, PeerState::DialFailure(Instant::now()));
                    }

                    trace!("inject_dial_failure {}, {:?}", peer_id, error);
                    let dials = &mut self.dials;
                    if let Some(mut dials) = dials.remove(&peer_id) {
                        while let Some((_id, sender)) = dials.pop() {
                            let _ = sender.send(Err(error.to_string()));
                        }
                    }
                }
            }
            _ => {}
        }
    }

    fn on_connection_handler_event(
        &mut self,
        peer_id: PeerId,
        connection: ConnectionId,
        event: HandlerEvent,
    ) {
        // trace!("inject_event from {}, event: {:?}", peer_id, event);
        match event {
            HandlerEvent::Connected { protocol } => {
                self.set_peer_state(&peer_id, PeerState::Responsive(connection, protocol));
                {
                    let dials = &mut self.dials;
                    if let Some(mut dials) = dials.remove(&peer_id) {
                        while let Some((id, sender)) = dials.pop() {
                            if let Err(err) = sender.send(Ok((connection, Some(protocol)))) {
                                warn!("dial:{}: failed to send dial response {:?}", id, err)
                            }
                        }
                    }
                }
            }
            HandlerEvent::ProtocolNotSuppported => {
                self.set_peer_state(&peer_id, PeerState::Unresponsive);

                let dials = &mut self.dials;
                if let Some(mut dials) = dials.remove(&peer_id) {
                    while let Some((id, sender)) = dials.pop() {
                        if let Err(err) = sender.send(Err("protocol not supported".into())) {
                            warn!("dial:{} failed to send dial response {:?}", id, err)
                        }
                    }
                }
            }
            HandlerEvent::Message { message, protocol } => {
                // mark peer as responsive
                self.set_peer_state(&peer_id, PeerState::Responsive(connection, protocol));
                self.receive_message(peer_id, message);
            }
            HandlerEvent::FailedToSendMessage { .. } => {
                // Handle
            }
        }
    }

    #[allow(clippy::type_complexity)]
    fn poll(
        &mut self,
        cx: &mut Context,
        _: &mut impl PollParameters,
    ) -> Poll<ToSwarm<Self::ToSwarm, THandlerInEvent<Self>>> {
        // limit work
        for _ in 0..50 {
            match Pin::new(&mut self.network).poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(ev) => match ev {
                    OutEvent::Dial { peer, response, id } => {
                        match self.get_peer_state(&peer) {
                            Some(PeerState::Responsive(conn, protocol_id)) => {
                                // already connected
                                if let Err(err) = response.send(Ok((conn, Some(protocol_id)))) {
                                    debug!("dial:{}: failed to send dial response {:?}", id, err)
                                }
                                continue;
                            }
                            Some(PeerState::Connected(conn)) => {
                                // already connected
                                if let Err(err) = response.send(Ok((conn, None))) {
                                    debug!("dial:{}: failed to send dial response {:?}", id, err)
                                }
                                continue;
                            }
                            Some(PeerState::DialFailure(dialed))
                                if dialed.elapsed() < DIAL_BACK_OFF =>
                            {
                                // Do not bother trying to dial these for now.
                                if let Err(err) =
                                    response.send(Err(format!("dial:{id}: undialable peer")))
                                {
                                    debug!("dial:{id}: failed to send dial response {err:?}")
                                }
                                continue;
                            }
                            _ => {
                                if self.pause_dialing {
                                    // already connected
                                    if let Err(err) =
                                        response.send(Err(format!("dial:{id}: dialing paused")))
                                    {
                                        debug!("dial:{id}: failed to send dial response {err:?}",)
                                    }
                                    continue;
                                }

                                self.dials.entry(peer).or_default().push((id, response));

                                return Poll::Ready(ToSwarm::Dial {
                                    opts: DialOpts::peer_id(peer).build(),
                                });
                            }
                        }
                    }
                    OutEvent::GenerateEvent(ev) => return Poll::Ready(ToSwarm::GenerateEvent(ev)),
                    OutEvent::SendMessage {
                        peer,
                        message,
                        response,
                        connection_id,
                    } => {
                        tracing::debug!("send message to {}", peer);
                        return Poll::Ready(ToSwarm::NotifyHandler {
                            peer_id: peer,
                            handler: NotifyHandler::One(connection_id),
                            event: handler::BitswapHandlerIn::Message(message, response),
                        });
                    }
                    OutEvent::ProtectPeer { peer } => {
                        if let Some(PeerState::Responsive(conn_id, _)) = self.get_peer_state(&peer)
                        {
                            return Poll::Ready(ToSwarm::NotifyHandler {
                                peer_id: peer,
                                handler: NotifyHandler::One(conn_id),
                                event: handler::BitswapHandlerIn::Protect,
                            });
                        }
                    }
                    OutEvent::UnprotectPeer { peer, response } => {
                        if let Some(PeerState::Responsive(conn_id, _)) = self.get_peer_state(&peer)
                        {
                            let _ = response.send(true);
                            return Poll::Ready(ToSwarm::NotifyHandler {
                                peer_id: peer,
                                handler: NotifyHandler::One(conn_id),
                                event: handler::BitswapHandlerIn::Unprotect,
                            });
                        }
                        let _ = response.send(false);
                    }
                },
            }
        }

        Poll::Pending
    }
}

pub fn verify_hash(cid: &Cid, bytes: &[u8]) -> Option<bool> {
    use cid::multihash::{Code, MultihashDigest};
    Code::try_from(cid.hash().code()).ok().map(|code| {
        let calculated_hash = code.digest(bytes);
        &calculated_hash == cid.hash()
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use anyhow::anyhow;
    use futures::prelude::*;
    use libp2p::identity::Keypair;
    use libp2p::swarm::SwarmEvent;
    use libp2p::Swarm;
    use libp2p::SwarmBuilder;
    use tokio::sync::{mpsc, RwLock};
    use tracing::{info, trace};
    use tracing_subscriber::{fmt, prelude::*, EnvFilter};

    use super::*;
    use crate::Block;

    fn assert_send<T: Send + Sync>() {}

    #[derive(Debug, Clone)]
    struct DummyStore;

    #[async_trait]
    impl Store for DummyStore {
        async fn get_size(&self, _: &Cid) -> Result<usize> {
            todo!()
        }
        async fn get(&self, _: &Cid) -> Result<Block> {
            todo!()
        }
        async fn has(&self, _: &Cid) -> Result<bool> {
            todo!()
        }
    }

    #[test]
    fn test_traits() {
        assert_send::<Bitswap<DummyStore>>();
        assert_send::<&Bitswap<DummyStore>>();
    }

    #[derive(Debug, Clone, Default)]
    struct TestStore {
        store: Arc<RwLock<AHashMap<Cid, Block>>>,
    }

    #[async_trait]
    impl Store for TestStore {
        async fn get_size(&self, cid: &Cid) -> Result<usize> {
            self.store
                .read()
                .await
                .get(cid)
                .map(|block| block.data().len())
                .ok_or_else(|| anyhow!("missing"))
        }

        async fn get(&self, cid: &Cid) -> Result<Block> {
            self.store
                .read()
                .await
                .get(cid)
                .cloned()
                .ok_or_else(|| anyhow!("missing"))
        }

        async fn has(&self, cid: &Cid) -> Result<bool> {
            Ok(self.store.read().await.contains_key(cid))
        }
    }

    #[tokio::test]
    async fn test_get_1_block() {
        get_block::<1>().await;
    }

    #[tokio::test]
    async fn test_get_2_block() {
        get_block::<2>().await;
    }

    #[tokio::test]
    async fn test_get_4_block() {
        get_block::<4>().await;
    }

    #[tokio::test]
    async fn test_get_64_block() {
        get_block::<64>().await;
    }

    #[tokio::test]
    async fn test_get_65_block() {
        get_block::<65>().await;
    }

    #[tokio::test]
    async fn test_get_66_block() {
        get_block::<66>().await;
    }

    #[tokio::test]
    async fn test_get_128_block() {
        tracing_subscriber::registry()
            .with(fmt::layer().pretty())
            .with(EnvFilter::from_default_env())
            .init();

        get_block::<128>().await;
    }

    #[tokio::test]
    async fn test_get_1024_block() {
        get_block::<1024>().await;
    }

    async fn get_block<const N: usize>() {
        let kp = Keypair::generate_ed25519();
        let store1 = TestStore::default();
        let bs1 = Bitswap::new(kp.public().to_peer_id(), store1.clone(), Config::default()).await;

        let mut swarm1 = SwarmBuilder::with_existing_identity(kp)
            .with_tokio()
            .with_tcp(
                libp2p::tcp::Config::default(),
                libp2p::noise::Config::new,
                libp2p::yamux::Config::default,
            )
            .unwrap()
            .with_behaviour(|_| bs1)
            .unwrap()
            .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(30)))
            .build();

        let blocks = (0..N).map(|_| create_random_block_v1()).collect::<Vec<_>>();

        for block in &blocks {
            store1
                .store
                .write()
                .await
                .insert(*block.cid(), block.clone());
        }

        let (tx, mut rx) = mpsc::channel::<Multiaddr>(1);

        Swarm::listen_on(&mut swarm1, "/ip4/127.0.0.1/tcp/0".parse().unwrap()).unwrap();

        let peer1 = tokio::task::spawn(async move {
            while swarm1.next().now_or_never().is_some() {}
            let listeners: Vec<_> = Swarm::listeners(&swarm1).collect();
            for l in listeners {
                tx.send(l.clone()).await.unwrap();
            }

            loop {
                let ev = swarm1.next().await;
                trace!("peer1: {:?}", ev);
            }
        });

        info!("peer2: startup");
        let kp = Keypair::generate_ed25519();
        let store2 = TestStore::default();
        let bs2 = Bitswap::new(kp.public().to_peer_id(), store2.clone(), Config::default()).await;

        let mut swarm2 = SwarmBuilder::with_existing_identity(kp)
            .with_tokio()
            .with_tcp(
                libp2p::tcp::Config::default(),
                libp2p::noise::Config::new,
                libp2p::yamux::Config::default,
            )
            .unwrap()
            .with_behaviour(|_| bs2)
            .unwrap()
            .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(30)))
            .build();

        let swarm2_bs_client = swarm2.behaviour().client().clone();
        let peer2 = tokio::task::spawn(async move {
            let addr = rx.recv().await.unwrap();
            info!("peer2: dialing peer1 at {}", addr);
            Swarm::dial(&mut swarm2, addr).unwrap();

            loop {
                match swarm2.next().await {
                    Some(SwarmEvent::ConnectionEstablished { peer_id, .. }) => {
                        trace!("peer2: connected to {}", peer_id);
                        // simulate identify to inform bitswap about the protocols
                        swarm2.behaviour_mut().on_identify(
                            &peer_id,
                            &[
                                StreamProtocol::new("/ipfs/bitswap/1.2.0"),
                                StreamProtocol::new("/ipfs/bitswap/1.1.0"),
                            ],
                        );
                    }
                    ev => trace!("peer2: {:?}", ev),
                }
            }
        });

        {
            info!("peer2: fetching block - ordered");
            let blocks = blocks.clone();
            let mut futs = Vec::new();
            for block in &blocks {
                let client = swarm2_bs_client.clone();
                futs.push(async move {
                    // Should work, because retrieved
                    let received_block = client.get_block(block.cid()).await?;

                    info!("peer2: received block");
                    Ok::<Block, anyhow::Error>(received_block)
                });
            }

            let results = futures::future::join_all(futs).await;
            for (block, result) in blocks.into_iter().zip(results.into_iter()) {
                let received_block = result.unwrap();
                assert_eq!(block, received_block);
            }
        }

        {
            info!("peer2: fetching block - unordered");
            let mut blocks = blocks.clone();
            let futs = futures::stream::FuturesUnordered::new();
            for block in &blocks {
                let client = swarm2_bs_client.clone();
                futs.push(async move {
                    // Should work, because retrieved
                    let received_block = client.get_block(block.cid()).await?;

                    info!("peer2: received block");
                    Ok::<Block, anyhow::Error>(received_block)
                });
            }

            let mut results = futs.try_collect::<Vec<_>>().await.unwrap();
            results.sort();
            blocks.sort();
            for (block, received_block) in blocks.into_iter().zip(results.into_iter()) {
                assert_eq!(block, received_block);
            }
        }

        {
            info!("peer2: fetching block - session");
            let mut blocks = blocks.clone();
            let ids: Vec<_> = blocks.iter().map(|b| *b.cid()).collect();
            let session = swarm2_bs_client.new_session().await;
            let (blocks_receiver, _guard) = session.get_blocks(&ids).await.unwrap().into_parts();
            let mut results: Vec<_> = blocks_receiver.collect().await;

            results.sort();
            blocks.sort();
            for (block, received_block) in blocks.into_iter().zip(results.into_iter()) {
                assert_eq!(block, received_block);
            }
        }

        info!("--shutting down peer1");
        peer1.abort();
        peer1.await.ok();

        info!("--shutting down peer2");
        peer2.abort();
        peer2.await.ok();
    }
}
