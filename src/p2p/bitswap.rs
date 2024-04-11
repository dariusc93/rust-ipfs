mod message;
mod pb;
mod prefix;
mod protocol;

use std::{
    collections::{hash_map::Entry, BTreeSet, HashMap, HashSet, VecDeque},
    fmt::Debug,
    future::IntoFuture,
    pin::Pin,
    task::{Context, Poll, Waker},
    time::Duration,
};

use bytes::Bytes;
use futures::{future::BoxFuture, ready, stream::FusedStream, FutureExt, Stream, StreamExt};
use futures_timer::Delay;
use libipld::Cid;
use libp2p::{
    core::Endpoint,
    swarm::{
        behaviour::ConnectionEstablished, dial_opts::DialOpts, ConnectionClosed, ConnectionDenied,
        ConnectionId, DialFailure, FromSwarm, NetworkBehaviour, NotifyHandler, OneShotHandler,
        OneShotHandlerConfig, SubstreamProtocol, THandler, THandlerInEvent, THandlerOutEvent,
        ToSwarm,
    },
    Multiaddr, PeerId,
};
use tokio_stream::StreamMap;

mod bitswap_pb {
    pub use super::pb::bitswap_pb::Message;
    pub mod message {
        use super::super::pb::bitswap_pb::mod_Message as message;
        pub use message::mod_Wantlist as wantlist;
        pub use message::Wantlist;
        pub use message::{Block, BlockPresence, BlockPresenceType};
    }
}

use crate::{repo::Repo, Block};

use self::{
    message::{BitswapMessage, BitswapRequest, BitswapResponse, RequestType},
    protocol::{BitswapProtocol, Message},
};

#[derive(Default, Debug, Clone, Copy)]
pub struct Config {
    pub max_wanted_blocks: Option<u8>,
    pub timeout: Option<Duration>,
}

#[derive(Debug)]
pub enum Event {
    NeedBlock { cid: Cid },
    BlockRetrieved { cid: Cid },
    CancelBlock { cid: Cid },
}

pub struct Behaviour {
    events: VecDeque<ToSwarm<<Self as NetworkBehaviour>::ToSwarm, THandlerInEvent<Self>>>,
    connections: HashMap<PeerId, HashSet<(ConnectionId, Multiaddr)>>,
    blacklist_connections: HashMap<PeerId, BTreeSet<ConnectionId>>,
    store: Repo,
    want_session: StreamMap<Cid, Session>,
    have_session: StreamMap<Cid, HaveSession>,
    waker: Option<Waker>,
}

impl Behaviour {
    pub fn new(store: &Repo) -> Self {
        Self {
            events: Default::default(),
            connections: Default::default(),
            blacklist_connections: Default::default(),
            store: store.clone(),
            want_session: StreamMap::new(),
            have_session: StreamMap::new(),
            waker: None,
        }
    }

    pub fn get(&mut self, cid: &Cid, providers: &[PeerId]) {
        self.gets(vec![*cid], providers)
    }

    pub fn gets(&mut self, cids: Vec<Cid>, providers: &[PeerId]) {
        let peers = match providers.is_empty() {
            true => {
                //If no providers are provided, we can send requests connected peers
                self.connections
                    .keys()
                    .filter(|peer_id| !self.blacklist_connections.contains_key(peer_id))
                    .copied()
                    .collect::<Vec<_>>()
            }
            false => {
                let mut connected = VecDeque::new();
                for peer_id in providers
                    .iter()
                    .filter(|peer_id| !self.blacklist_connections.contains_key(peer_id))
                {
                    if self.connections.contains_key(peer_id) {
                        connected.push_back(*peer_id);
                        continue;
                    }
                    let opts = DialOpts::peer_id(*peer_id).build();

                    self.events.push_back(ToSwarm::Dial { opts });
                }
                Vec::from_iter(connected)
            }
        };

        for cid in &cids {
            if self.want_session.contains_key(cid) {
                continue;
            }
            let session = Session::new(&self.store, *cid);
            self.want_session.insert(*cid, session);
        }

        if peers.is_empty() {
            // Since no connections, peers or providers are provided, we need to notify swarm to attempt a form of content discovery
            for cid in cids {
                self.events
                    .push_back(ToSwarm::GenerateEvent(Event::NeedBlock { cid }));
            }
            return;
        }

        self.send_wants(peers)
    }

    pub fn local_wantlist(&self) -> Vec<Cid> {
        self.want_session.keys().copied().collect()
    }

    pub fn peer_wantlist(&self, peer_id: PeerId) -> Vec<Cid> {
        let mut blocks = HashSet::new();

        for (cid, session) in self.have_session.iter() {
            if session.has_peer(peer_id) {
                blocks.insert(*cid);
            }
        }

        Vec::from_iter(blocks)
    }

    // Note: This is called specifically to cancel the request and not just emitting a request
    //       after receiving a request.
    pub fn cancel(&mut self, cid: Cid) {
        if self.want_session.remove(&cid).is_none() {
            return;
        }

        self.events
            .push_back(ToSwarm::GenerateEvent(Event::CancelBlock { cid }));

        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }

    // This will notify connected peers who have the bitswap protocol that we have this block
    // if they wanted it
    // TODO: Maybe have a general `Session` where we could collectively notify a peer of new blocks
    //       in a single message
    pub fn notify_new_blocks(&mut self, cid: impl IntoIterator<Item = Cid>) {
        let blocks = cid.into_iter().collect::<Vec<_>>();

        for (cid, session) in self.have_session.iter_mut() {
            if !blocks.contains(cid) {
                continue;
            }

            session.reset();
        }

        if !self.have_session.is_empty() {
            if let Some(waker) = self.waker.take() {
                waker.wake();
            }
        }
    }

    fn on_connection_established(
        &mut self,
        ConnectionEstablished {
            connection_id,
            peer_id,
            endpoint,
            other_established,
            ..
        }: ConnectionEstablished,
    ) {
        let address = endpoint.get_remote_address().clone();
        self.connections
            .entry(peer_id)
            .or_default()
            .insert((connection_id, address));

        if other_established > 0 {
            return;
        }

        self.send_wants(vec![peer_id]);
    }

    fn on_connection_close(
        &mut self,
        ConnectionClosed {
            connection_id,
            peer_id,
            endpoint,
            remaining_established,
            ..
        }: ConnectionClosed,
    ) {
        let address = endpoint.get_remote_address().clone();
        if let Entry::Occupied(mut entry) = self.connections.entry(peer_id) {
            let list = entry.get_mut();
            list.remove(&(connection_id, address));
            if list.is_empty() {
                entry.remove();
            }
        }

        if let Entry::Occupied(mut entry) = self.blacklist_connections.entry(peer_id) {
            let list = entry.get_mut();
            list.remove(&connection_id);
            if list.is_empty() {
                entry.remove();
            }
        }

        if remaining_established == 0 {
            for (_, session) in self.want_session.iter_mut() {
                session.remove_peer(peer_id);
            }
        }
    }

    fn on_dial_failure(
        &mut self,
        DialFailure {
            connection_id: _,
            peer_id,
            error: _,
        }: DialFailure,
    ) {
        let Some(peer_id) = peer_id else {
            return;
        };

        if self.connections.contains_key(&peer_id) {
            // Since there is still an existing connection for the peer
            // we can ignore the dial failure
            return;
        }

        for session in self.want_session.values_mut() {
            session.remove_peer(peer_id);
        }

        for session in self.have_session.values_mut() {
            session.remove_peer(peer_id);
        }
    }

    fn send_wants(&mut self, peers: Vec<PeerId>) {
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }

        for (_, session) in self.want_session.iter_mut() {
            for peer_id in &peers {
                session.send_have_block(*peer_id)
            }
        }
    }
}

impl NetworkBehaviour for Behaviour {
    type ConnectionHandler = OneShotHandler<BitswapProtocol, BitswapMessage, Message>;
    type ToSwarm = Event;

    fn handle_pending_inbound_connection(
        &mut self,
        _: ConnectionId,
        _: &Multiaddr,
        _: &Multiaddr,
    ) -> Result<(), ConnectionDenied> {
        Ok(())
    }

    fn handle_pending_outbound_connection(
        &mut self,
        _: ConnectionId,
        _: Option<PeerId>,
        _: &[Multiaddr],
        _: Endpoint,
    ) -> Result<Vec<Multiaddr>, ConnectionDenied> {
        Ok(vec![])
    }

    fn handle_established_inbound_connection(
        &mut self,
        _: ConnectionId,
        _: PeerId,
        _: &Multiaddr,
        _: &Multiaddr,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        Ok(OneShotHandler::new(
            SubstreamProtocol::new(Default::default(), ()),
            OneShotHandlerConfig {
                max_dial_negotiated: 100,
                ..Default::default()
            },
        ))
    }

    fn handle_established_outbound_connection(
        &mut self,
        _: ConnectionId,
        _: PeerId,
        _: &Multiaddr,
        _: Endpoint,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        Ok(OneShotHandler::new(
            SubstreamProtocol::new(Default::default(), ()),
            OneShotHandlerConfig {
                max_dial_negotiated: 100,
                ..Default::default()
            },
        ))
    }

    fn on_connection_handler_event(
        &mut self,
        peer_id: PeerId,
        connection_id: ConnectionId,
        event: THandlerOutEvent<Self>,
    ) {
        let message = match event {
            Ok(Message::Receive { message }) => {
                tracing::trace!(%peer_id, %connection_id, "message received");
                if let Entry::Occupied(mut e) = self.blacklist_connections.entry(peer_id) {
                    let list = e.get_mut();
                    list.remove(&connection_id);
                    if list.is_empty() {
                        e.remove();
                    }
                }

                message
            }
            Ok(Message::Sent) => {
                tracing::trace!(%peer_id, %connection_id, "message sent");
                return;
            }
            Err(e) => {
                tracing::error!(%peer_id, %connection_id, error = %e, "error sending or receiving message");
                //TODO: Depending on the underlining error, maybe blacklist the peer from further sending/receiving
                //      until a valid response or request is produced?
                self.blacklist_connections
                    .entry(peer_id)
                    .or_default()
                    .insert(connection_id);
                return;
            }
        };

        let message = BitswapMessage::from_proto(message)
            .map_err(|e| {
                tracing::error!(error = %e, %peer_id, "unable to parse message");
                e
            })
            .unwrap_or_default();

        if message.is_empty() {
            tracing::warn!(%peer_id, %connection_id, "received an empty message");
            return;
        }

        let BitswapMessage {
            requests,
            responses,
        } = message;

        for BitswapRequest {
            ty,
            cid,
            send_dont_have,
            cancel,
            priority: _,
        } in requests
        {
            if !self.have_session.contains_key(&cid) && !cancel {
                // Lets build out have new sessions
                let have_session = HaveSession::new(&self.store, cid);
                self.have_session.insert(cid, have_session);
            }

            let session = self
                .have_session
                .iter_mut()
                .find(|(session_cid, _)| *session_cid == cid)
                .map(|(_, session)| session)
                .expect("session exist");

            if cancel {
                session.cancel(peer_id);
                continue;
            }

            match ty {
                RequestType::Have => {
                    session.want_block(peer_id, send_dont_have);
                }
                RequestType::Block => {
                    session.need_block(peer_id);
                }
            }
        }

        for (cid, response) in responses {
            let Some(session) = self
                .want_session
                .iter_mut()
                .find(|(session_cid, _)| *session_cid == cid)
                .map(|(_, session)| session)
            else {
                tracing::warn!(block = %cid, %peer_id, %connection_id, "want session does not exist. Skipping response");
                continue;
            };
            match response {
                BitswapResponse::Have(have) => match have {
                    true => {
                        session.has_block(peer_id);
                    }
                    false => {
                        session.dont_have_block(peer_id);
                    }
                },
                BitswapResponse::Block(bytes) => {
                    let Ok(block) = Block::new(cid, bytes.to_vec()) else {
                        // The block is invalid so we will notify the session that we still dont have the block
                        // from said peer
                        // TODO: In the future, mark the peer as a bad sender
                        tracing::error!(block = %cid, %peer_id, %connection_id, "block is invalid or corrupted");
                        session.dont_have_block(peer_id);
                        continue;
                    };
                    session.put_block(peer_id, block);
                }
            }
        }
    }

    fn on_swarm_event(&mut self, event: FromSwarm) {
        match event {
            FromSwarm::ConnectionEstablished(event) => self.on_connection_established(event),
            FromSwarm::ConnectionClosed(event) => self.on_connection_close(event),
            FromSwarm::DialFailure(event) => self.on_dial_failure(event),
            _ => {}
        }
    }

    fn poll(&mut self, ctx: &mut Context) -> Poll<ToSwarm<Self::ToSwarm, THandlerInEvent<Self>>> {
        if let Some(event) = self.events.pop_front() {
            return Poll::Ready(event);
        }

        while let Poll::Ready(Some((cid, event))) = self.have_session.poll_next_unpin(ctx) {
            match event {
                HaveSessionEvent::Have { peer_id } => {
                    return Poll::Ready(ToSwarm::NotifyHandler {
                        peer_id,
                        handler: NotifyHandler::Any,
                        event: BitswapMessage::default()
                            .add_response(cid, BitswapResponse::Have(true)),
                    })
                }
                HaveSessionEvent::DontHave { peer_id } => {
                    return Poll::Ready(ToSwarm::NotifyHandler {
                        peer_id,
                        handler: NotifyHandler::Any,
                        event: BitswapMessage::default()
                            .add_response(cid, BitswapResponse::Have(false)),
                    })
                }
                HaveSessionEvent::Block { peer_id, bytes } => {
                    return Poll::Ready(ToSwarm::NotifyHandler {
                        peer_id,
                        handler: NotifyHandler::Any,
                        event: BitswapMessage::default()
                            .add_response(cid, BitswapResponse::Block(bytes)),
                    })
                }
                HaveSessionEvent::Cancelled => {
                    //TODO: Maybe notify peers from this session about any cancelled request?
                    self.have_session.remove(&cid);
                }
            };
        }

        while let Poll::Ready(Some((cid, event))) = self.want_session.poll_next_unpin(ctx) {
            match event {
                SessionEvent::SendWant { peer_id } => {
                    return Poll::Ready(ToSwarm::NotifyHandler {
                        peer_id,
                        handler: NotifyHandler::Any,
                        event: BitswapMessage::default()
                            .add_request(BitswapRequest::have(cid).send_dont_have(true)),
                    });
                }
                SessionEvent::SendCancels { peers } => {
                    for peer_id in peers {
                        self.events.push_back(ToSwarm::NotifyHandler {
                            peer_id,
                            handler: NotifyHandler::Any,
                            event: BitswapMessage::default()
                                .add_request(BitswapRequest::cancel(cid)),
                        });
                    }
                }
                SessionEvent::SendWants { peers } => {
                    for peer_id in peers {
                        self.events.push_back(ToSwarm::NotifyHandler {
                            peer_id,
                            handler: NotifyHandler::Any,
                            event: BitswapMessage::default()
                                .add_request(BitswapRequest::have(cid).send_dont_have(true)),
                        });
                    }
                }
                SessionEvent::SendBlock { peer_id } => {
                    return Poll::Ready(ToSwarm::NotifyHandler {
                        peer_id,
                        handler: NotifyHandler::Any,
                        event: BitswapMessage::default()
                            .add_request(BitswapRequest::block(cid).send_dont_have(true)),
                    });
                }
                SessionEvent::NeedBlock => {
                    return Poll::Ready(ToSwarm::GenerateEvent(Event::NeedBlock { cid }));
                }
                SessionEvent::BlockStored => {
                    return Poll::Ready(ToSwarm::GenerateEvent(Event::BlockRetrieved { cid }))
                }
            }
        }

        self.waker = Some(ctx.waker().clone());

        Poll::Pending
    }
}

pub async fn handle_inbound_request(
    repo: &Repo,
    request: &BitswapRequest,
) -> Option<BitswapResponse> {
    if request.cancel {
        return None;
    }

    match request.ty {
        RequestType::Have => {
            let have = repo.contains(&request.cid).await.unwrap_or_default();
            (have || request.send_dont_have).then(|| BitswapResponse::Have(have))
        }
        RequestType::Block => {
            let block = repo.get_block_now(&request.cid).await.unwrap_or_default();
            if let Some(data) = block.map(|b| Bytes::copy_from_slice(b.data())) {
                Some(BitswapResponse::Block(data))
            } else if request.send_dont_have {
                Some(BitswapResponse::Have(false))
            } else {
                None
            }
        }
    }
}

#[allow(dead_code)]
enum SessionEvent {
    SendWant { peer_id: PeerId },
    SendCancels { peers: VecDeque<PeerId> },
    SendWants { peers: VecDeque<PeerId> },
    SendBlock { peer_id: PeerId },
    BlockStored,
    NeedBlock,
}

enum SessionState {
    Idle,
    NextBlock,
    NextBlockPending {
        timer: Delay,
    },
    PutBlock {
        fut: BoxFuture<'static, Result<Cid, anyhow::Error>>,
    },
    Complete,
}

impl Debug for SessionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SessionState")
    }
}

#[derive(Debug)]
struct Session {
    cid: Cid,
    sending_wants: VecDeque<PeerId>,
    sent_wants: VecDeque<PeerId>,
    have_block: VecDeque<PeerId>,
    failed_block: VecDeque<PeerId>,
    sent_have_block: Option<PeerId>,
    requested_block: bool,
    received: bool,
    waker: Option<Waker>,
    repo: Repo,
    state: SessionState,
    timeout: Option<Duration>,
}

impl Session {
    pub fn new(repo: &Repo, cid: Cid) -> Self {
        Self {
            cid,
            sending_wants: Default::default(),
            sent_wants: Default::default(),
            have_block: Default::default(),
            sent_have_block: Default::default(),
            failed_block: Default::default(),
            received: false,
            requested_block: false,
            repo: repo.clone(),
            waker: None,
            state: SessionState::Idle,
            timeout: None,
        }
    }

    pub fn send_have_block(&mut self, peer_id: PeerId) {
        if !self.sent_wants.contains(&peer_id)
            && !self.sending_wants.contains(&peer_id)
            && !self.have_block.contains(&peer_id)
        {
            tracing::trace!(session = %self.cid, %peer_id, name = "want_session", "send have block");
            self.requested_block = false;
            self.sending_wants.push_back(peer_id);
            if let Some(w) = self.waker.take() {
                w.wake();
            }
        }
    }

    pub fn has_block(&mut self, peer_id: PeerId) {
        tracing::debug!(session = %self.cid, %peer_id, name = "want_session", "have block");
        if !self.have_block.contains(&peer_id) {
            self.have_block.push_back(peer_id);
        }

        self.sending_wants.retain(|pid| *pid != peer_id);
        self.sent_wants.retain(|pid| *pid != peer_id);

        if !matches!(self.state, SessionState::NextBlock) {
            tracing::debug!(session = %self.cid, %peer_id, name = "want_session", "change state to next_block");
            self.state = SessionState::NextBlock;
        }

        if let Some(w) = self.waker.take() {
            w.wake();
        }
    }

    pub fn dont_have_block(&mut self, peer_id: PeerId) {
        tracing::trace!(session = %self.cid, %peer_id, name = "want_session", "dont have block");
        self.sending_wants.retain(|pid| *pid != peer_id);
        self.sent_wants.retain(|pid| *pid != peer_id);

        if !self.is_empty()
            && !matches!(
                self.state,
                SessionState::NextBlock | SessionState::NextBlockPending { .. }
            )
        {
            tracing::warn!(session = %self.cid, %peer_id, name = "want_session", "session is empty");
            return;
        }

        // change state to next block so it will perform another request if possible,
        // otherwise notify swarm if no request been sent
        tracing::debug!(session = %self.cid, %peer_id, name = "want_session", "next_block state");
        self.state = SessionState::NextBlock;
        if let Some(w) = self.waker.take() {
            w.wake();
        }
    }

    pub fn put_block(&mut self, peer_id: PeerId, block: Block) {
        if matches!(self.state, SessionState::PutBlock { .. }) {
            tracing::warn!(session = %self.cid, %peer_id, cid = %block.cid(), name = "want_session", "state already putting block into store");
            return;
        }

        tracing::info!(%peer_id, cid = %block.cid(), name = "want_session", "storing block");
        let fut = self.repo.put_block(block).into_future();
        self.state = SessionState::PutBlock { fut };

        if let Some(w) = self.waker.take() {
            w.wake();
        }
    }

    pub fn remove_peer(&mut self, peer_id: PeerId) {
        if self.is_empty() {
            return;
        }
        // tracing::debug!(session = %self.cid, %peer_id, name = "want_session", "removing peer from want_session");
        self.sending_wants.retain(|pid| *pid != peer_id);
        self.sent_wants.retain(|pid| *pid != peer_id);
        self.have_block.retain(|pid| *pid != peer_id);
        if matches!(self.sent_have_block, Some(p) if p == peer_id) {
            self.sent_have_block.take();
            self.state = SessionState::NextBlock;
        }
        if let Some(w) = self.waker.take() {
            w.wake();
        }
    }

    #[allow(dead_code)]
    fn is_empty(&self) -> bool {
        self.sending_wants.is_empty() && self.sent_wants.is_empty() && self.have_block.is_empty()
    }
}

impl Unpin for Session {}

impl Stream for Session {
    type Item = SessionEvent;

    #[tracing::instrument(level = "trace", name = "Session::poll_next", skip(self, cx))]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.received {
            return Poll::Ready(None);
        }

        if let Some(peer_id) = self.sending_wants.pop_front() {
            self.sent_wants.push_back(peer_id);
            tracing::debug!(session = %self.cid, %peer_id, name = "want_session", "sent want block");
            return Poll::Ready(Some(SessionEvent::SendWant { peer_id }));
        }

        loop {
            match &mut self.state {
                SessionState::Idle => {
                    self.waker = Some(cx.waker().clone());
                    return Poll::Pending;
                }
                SessionState::NextBlock => {
                    if let Some(peer_id) = self.sent_have_block.take() {
                        tracing::debug!(session = %self.cid, %peer_id, name = "want_session", "failed block");
                        self.failed_block.push_back(peer_id);
                    }

                    if let Some(next_peer_id) = self.have_block.pop_front() {
                        tracing::info!(session = %self.cid, %next_peer_id, name = "want_session", "sending block request to next peer");
                        self.requested_block = false;
                        self.sent_have_block = Some(next_peer_id);
                        let timeout = match self.timeout {
                            Some(timeout) if !timeout.is_zero() => timeout,
                            _ => Duration::from_secs(15),
                        };
                        let timer = Delay::new(timeout);
                        self.state = SessionState::NextBlockPending { timer };

                        return Poll::Ready(Some(SessionEvent::SendBlock {
                            peer_id: next_peer_id,
                        }));
                    }

                    tracing::debug!(session = %self.cid, name = "want_session", "session is idle");
                    self.state = SessionState::Idle;

                    if self.is_empty() && !self.requested_block {
                        self.requested_block = true;
                        return Poll::Ready(Some(SessionEvent::NeedBlock));
                    }
                }
                SessionState::NextBlockPending { timer } => {
                    ready!(timer.poll_unpin(cx));
                    tracing::warn!(session = %self.cid, name = "want_session", "request timeout attempting to get next block");
                    self.state = SessionState::NextBlock;
                }
                SessionState::PutBlock { fut } => match ready!(fut.poll_unpin(cx)) {
                    Ok(cid) => {
                        tracing::info!(session = %self.cid, block = %cid, name = "want_session", "block stored in block store");
                        self.state = SessionState::Complete;

                        cx.waker().wake_by_ref();
                        return Poll::Ready(Some(SessionEvent::BlockStored));
                    }
                    Err(e) => {
                        tracing::error!(session = %self.cid, error = %e, name = "want_session", "error storing block in store");
                        self.state = SessionState::NextBlock;
                    }
                },
                SessionState::Complete => {
                    self.received = true;
                    let mut peers = HashSet::new();
                    // although this should be empty by the time we reach here, its best to clear it anyway since nothing was ever sent
                    self.sending_wants.clear();

                    peers.extend(std::mem::take(&mut self.sent_wants));
                    peers.extend(std::mem::take(&mut self.have_block));
                    peers.extend(std::mem::take(&mut self.failed_block));

                    if let Some(peer_id) = self.sent_have_block.take() {
                        peers.insert(peer_id);
                    };

                    let peers = VecDeque::from_iter(peers);

                    tracing::info!(session = %self.cid, pending_cancellation = peers.len());

                    return Poll::Ready(Some(SessionEvent::SendCancels { peers }));
                }
            }
        }
    }
}

impl FusedStream for Session {
    fn is_terminated(&self) -> bool {
        self.received
    }
}

pub enum HaveSessionEvent {
    Have { peer_id: PeerId },
    DontHave { peer_id: PeerId },
    Block { peer_id: PeerId, bytes: Bytes },
    Cancelled,
}

enum HaveSessionState {
    Idle,
    ContainBlock {
        fut: BoxFuture<'static, Result<bool, anyhow::Error>>,
    },
    GetBlock {
        fut: BoxFuture<'static, Result<Option<Block>, anyhow::Error>>,
    },
    Block {
        bytes: Bytes,
    },
    Complete,
}

enum HaveWantState {
    Pending,
    Sent,
    Block,
    BlockSent,
}

pub struct HaveSession {
    cid: Cid,
    want: HashMap<PeerId, HaveWantState>,
    send_dont_have: HashSet<PeerId>,
    have: Option<bool>,
    repo: Repo,
    waker: Option<Waker>,
    state: HaveSessionState,
}

impl HaveSession {
    pub fn new(repo: &Repo, cid: Cid) -> Self {
        let mut session = Self {
            cid,
            want: HashMap::new(),
            send_dont_have: HashSet::new(),
            have: None,
            repo: repo.clone(),
            waker: None,
            state: HaveSessionState::Idle,
        };
        let repo = session.repo.clone();
        // We perform a precheck against the block to determine if we have it so when a peer send a request
        // we can respond accordingly
        let fut = async move { repo.contains(&cid).await }.boxed();

        session.state = HaveSessionState::ContainBlock { fut };

        session
    }

    pub fn peers(&self) -> Vec<PeerId> {
        self.want.keys().copied().collect()
    }

    pub fn has_peer(&self, peer_id: PeerId) -> bool {
        self.want.contains_key(&peer_id)
    }

    pub fn want_block(&mut self, peer_id: PeerId, send_dont_have: bool) {
        if self.want.contains_key(&peer_id) {
            tracing::warn!(session = %self.cid, %peer_id, "peer requested block");
            return;
        }

        tracing::info!(session = %self.cid, %peer_id, name = "have_session", "peer want block");

        if send_dont_have {
            self.send_dont_have.insert(peer_id);
        }
        self.want.insert(peer_id, HaveWantState::Pending);

        if let Some(w) = self.waker.take() {
            w.wake();
        }
    }

    pub fn need_block(&mut self, peer_id: PeerId) {
        if self
            .want
            .get(&peer_id)
            .map(|state| matches!(state, HaveWantState::Block))
            .unwrap_or_default()
        {
            tracing::warn!(session = %self.cid, %peer_id, name = "have_session", "already sending block to peer");
            return;
        }

        tracing::info!(session = %self.cid, %peer_id, name = "have_session", "peer requested block");

        self.want
            .entry(peer_id)
            .and_modify(|state| *state = HaveWantState::Block)
            .or_insert(HaveWantState::Block);

        if !matches!(
            self.state,
            HaveSessionState::GetBlock { .. } | HaveSessionState::Block { .. }
        ) {
            let repo = self.repo.clone();
            let cid = self.cid;
            let fut = async move { repo.get_block_now(&cid).await }.boxed();

            tracing::info!(session = %self.cid, %peer_id, name = "have_session", "change state to get_block");
            self.state = HaveSessionState::GetBlock { fut };

            if let Some(w) = self.waker.take() {
                w.wake();
            }
        }
    }

    pub fn remove_peer(&mut self, peer_id: PeerId) {
        tracing::info!(session = %self.cid, %peer_id, name = "have_session", "removing peer from have_session");
        self.want.remove(&peer_id);
        self.send_dont_have.remove(&peer_id);
        if let Some(w) = self.waker.take() {
            w.wake();
        }
    }

    pub fn reset(&mut self) {
        // Only reset if we have not resolve block
        if self.have.is_none() || self.have.unwrap_or_default() {
            return;
        }

        tracing::info!(session = %self.cid, name = "have_session", "resetting session");

        for state in self.want.values_mut() {
            *state = HaveWantState::Pending;
        }
        let repo = self.repo.clone();
        let cid = self.cid;
        let fut = async move { repo.contains(&cid).await }.boxed();

        self.state = HaveSessionState::ContainBlock { fut };
        if let Some(w) = self.waker.take() {
            w.wake();
        }
    }

    pub fn cancel(&mut self, peer_id: PeerId) {
        self.want.remove(&peer_id);
        self.send_dont_have.remove(&peer_id);

        tracing::info!(session = %self.cid, %peer_id, name = "have_session", "cancelling request");
    }
}

impl Unpin for HaveSession {}

impl Stream for HaveSession {
    type Item = HaveSessionEvent;

    #[tracing::instrument(level = "trace", name = "HaveSession::poll_next", skip(self, cx))]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if matches!(self.state, HaveSessionState::Complete) {
            return Poll::Ready(None);
        }

        let this = &mut *self;

        // Since our state contains a block, we can attempt to provide it to peers who requests it then we will
        // reset the state back to idle until the wants are empty
        if let HaveSessionState::Block { bytes } = &this.state {
            if let Some(next_peer_id) = this
                .want
                .iter()
                .filter(|(_, state)| matches!(state, HaveWantState::Block))
                .map(|(peer_id, _)| peer_id)
                .copied()
                .next()
            {
                this.want.remove(&next_peer_id);
                return Poll::Ready(Some(HaveSessionEvent::Block {
                    peer_id: next_peer_id,
                    bytes: bytes.clone(),
                }));
            }

            if this.want.is_empty() {
                // Since we have no more peers who want the block, we will finalize the session
                this.state = HaveSessionState::Complete;
                return Poll::Ready(Some(HaveSessionEvent::Cancelled));
            }

            this.state = HaveSessionState::Idle;
            return Poll::Pending;
        }

        loop {
            match &mut this.state {
                HaveSessionState::Idle => {
                    if let Some(have) = this.have {
                        if let Some((peer_id, state)) = this
                            .want
                            .iter_mut()
                            .find(|(_, state)| matches!(state, HaveWantState::Pending))
                        {
                            let peer_id = *peer_id;
                            *state = HaveWantState::Sent;

                            return match have {
                                true => Poll::Ready(Some(HaveSessionEvent::Have { peer_id })),
                                false => Poll::Ready(Some(HaveSessionEvent::DontHave { peer_id })),
                            };
                        }
                    }
                    return Poll::Pending;
                }
                HaveSessionState::ContainBlock { fut } => {
                    let have = ready!(fut.poll_unpin(cx)).unwrap_or_default();
                    this.have = Some(have);
                    this.state = HaveSessionState::Idle;
                }
                // Maybe we should have a lock on a single lock to prevent GC from cleaning it up or being removed while waiting for it to be
                // exchanged. This could probably be done through a temporary pin
                HaveSessionState::GetBlock { fut } => {
                    let result = ready!(fut.poll_unpin(cx));
                    let block = match result.as_ref() {
                        Ok(Some(block)) => block.data(),
                        _ => {
                            this.state = HaveSessionState::Idle;
                            this.have = Some(false);
                            continue;
                        }
                    };
                    let bytes = Bytes::copy_from_slice(block);
                    // In case we are sent a block request
                    this.have = Some(true);

                    this.state = HaveSessionState::Block {
                        bytes: bytes.clone(),
                    };
                }
                HaveSessionState::Block { bytes } => {
                    // This will kick start the providing process to the peer
                    match this
                        .want
                        .iter_mut()
                        .find(|(_, state)| matches!(state, HaveWantState::Block))
                    {
                        Some((peer_id, state)) => {
                            *state = HaveWantState::BlockSent;
                            return Poll::Ready(Some(HaveSessionEvent::Block {
                                peer_id: *peer_id,
                                bytes: bytes.clone(),
                            }));
                        }
                        None => return Poll::Pending,
                    }
                }
                HaveSessionState::Complete => {
                    // Although this branch is unreachable, we should return `None` in case this is ever reached
                    // though any attempts in continuious polling after this point would be considered as UB
                    return Poll::Ready(None);
                }
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.want.len(), None)
    }
}

impl FusedStream for HaveSession {
    fn is_terminated(&self) -> bool {
        matches!(self.state, HaveSessionState::Complete)
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use futures::StreamExt;
    use libipld::{
        multihash::{Code, MultihashDigest},
        Cid, IpldCodec,
    };
    use libp2p::{
        swarm::{dial_opts::DialOpts, SwarmEvent},
        Multiaddr, PeerId, Swarm, SwarmBuilder,
    };

    use crate::{repo::Repo, Block};

    fn create_block() -> Block {
        let data = b"hello block\n".to_vec();
        let cid = Cid::new_v1(IpldCodec::Raw.into(), Code::Sha2_256.digest(&data));

        Block::new_unchecked(cid, data)
    }

    #[tokio::test]
    async fn exchange_blocks() -> anyhow::Result<()> {
        let (_, _, mut swarm1, repo) = build_swarm().await;
        let (peer2, addr2, mut swarm2, repo2) = build_swarm().await;

        let block = create_block();

        let cid = *block.cid();

        repo.put_block(block.clone()).await?;

        let opt = DialOpts::peer_id(peer2)
            .addresses(vec![addr2.clone()])
            .build();

        swarm1.dial(opt)?;

        loop {
            futures::select! {
                event = swarm1.select_next_some() => {
                    if let SwarmEvent::ConnectionEstablished { peer_id, .. } = event {
                        assert_eq!(peer_id, peer2);
                        break;
                    }
                }
                _ = swarm2.next() => {}
            }
        }

        swarm2.behaviour_mut().get(&cid, &[peer2]);

        loop {
            tokio::select! {
                _ = swarm1.next() => {}
                e = swarm2.select_next_some() => {
                    if let SwarmEvent::Behaviour(super::Event::BlockRetrieved { cid: inner_cid }) = e {
                        assert_eq!(inner_cid, cid);
                    }
                },
                Ok(true) = repo2.contains(&cid) => {
                    break;
                }
            }
        }

        let b = repo2
            .get_block_now(&cid)
            .await
            .unwrap()
            .expect("block exist");

        assert_eq!(b, block);

        Ok(())
    }

    #[tokio::test]
    async fn notify_after_block_exchange() -> anyhow::Result<()> {
        let (peer1, _, mut swarm1, repo) = build_swarm().await;
        let (peer2, addr2, mut swarm2, _) = build_swarm().await;
        let (peer3, addr3, mut swarm3, repo3) = build_swarm().await;

        let block = create_block();

        let cid = *block.cid();

        repo.put_block(block.clone()).await?;

        let opt = DialOpts::peer_id(peer2)
            .addresses(vec![addr2.clone()])
            .build();
        swarm1.dial(opt)?;

        let opt = DialOpts::peer_id(peer3)
            .addresses(vec![addr3.clone()])
            .build();

        swarm2.dial(opt)?;
        let mut peer_1_connected = false;
        let mut peer_2_connected = false;
        let mut peer_3_connected = false;

        loop {
            futures::select! {
                event = swarm1.select_next_some() => {
                    if let SwarmEvent::ConnectionEstablished { .. } = event {
                        peer_1_connected = true;
                    }
                }
                event = swarm2.select_next_some() => {
                    if let SwarmEvent::ConnectionEstablished { peer_id, .. } = event {
                        if peer_id == peer1 {
                            peer_2_connected = true;
                        }
                    }
                }

                event = swarm3.select_next_some() => {
                    if let SwarmEvent::ConnectionEstablished { peer_id, .. } = event {
                        assert_eq!(peer_id, peer2);
                        peer_3_connected = true;
                    }
                }
            }
            if peer_1_connected && peer_2_connected && peer_3_connected {
                break;
            }
        }
        swarm2.behaviour_mut().get(&cid, &[peer1]);
        swarm3.behaviour_mut().get(&cid, &[]);

        loop {
            tokio::select! {
                _ = swarm1.next() => {}
                e = swarm2.select_next_some() => {
                    if let SwarmEvent::Behaviour(super::Event::BlockRetrieved { cid: inner_cid }) = e {
                        assert_eq!(inner_cid, cid);
                        swarm2.behaviour_mut().notify_new_blocks(std::iter::once(cid));
                    }
                },
                e = swarm3.select_next_some() => {
                    if let SwarmEvent::Behaviour(super::Event::BlockRetrieved { cid: inner_cid }) = e {
                        assert_eq!(inner_cid, cid);
                        break;
                    }
                },
            }
        }

        let b = repo3
            .get_block_now(&cid)
            .await
            .unwrap()
            .expect("block exist");

        assert_eq!(b, block);

        Ok(())
    }

    #[tokio::test]
    async fn cancel_block_exchange() -> anyhow::Result<()> {
        let (_, _, mut swarm1, _) = build_swarm().await;

        let block = create_block();

        let cid = *block.cid();

        swarm1.behaviour_mut().get(&cid, &[]);
        swarm1.behaviour_mut().cancel(cid);

        loop {
            tokio::select! {
                e = swarm1.select_next_some() => {
                    if let SwarmEvent::Behaviour(super::Event::CancelBlock { cid: inner_cid }) = e {
                        assert_eq!(inner_cid, cid);
                        break;
                    }
                },
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn local_wantlist() -> anyhow::Result<()> {
        let (_, _, mut swarm1, _) = build_swarm().await;

        let block = create_block();

        let cid = *block.cid();

        swarm1.behaviour_mut().get(&cid, &[]);

        let list = swarm1.behaviour().local_wantlist();

        assert_eq!(list[0], cid);

        Ok(())
    }

    #[tokio::test]
    async fn peer_wantlist() -> anyhow::Result<()> {
        let (peer1, _, mut swarm1, _) = build_swarm().await;
        let (peer2, addr2, mut swarm2, _) = build_swarm().await;

        let block = create_block();

        let cid = *block.cid();

        let opt = DialOpts::peer_id(peer2)
            .addresses(vec![addr2.clone()])
            .build();
        swarm1.dial(opt)?;

        let mut peer_1_connected = false;
        let mut peer_2_connected = false;

        loop {
            futures::select! {
                event = swarm1.select_next_some() => {
                    if let SwarmEvent::ConnectionEstablished { .. } = event {
                        peer_1_connected = true;
                    }
                }
                event = swarm2.select_next_some() => {
                    if let SwarmEvent::ConnectionEstablished { peer_id, .. } = event {
                        if peer_id == peer1 {
                            peer_2_connected = true;
                        }
                    }
                }
            }
            if peer_1_connected && peer_2_connected {
                break;
            }
        }
        swarm2.behaviour_mut().get(&cid, &[peer1]);

        loop {
            tokio::select! {
                _ = swarm1.next() => {}
                e = swarm2.select_next_some() => {
                    if let SwarmEvent::Behaviour(super::Event::NeedBlock { cid: inner_cid }) = e {
                        assert_eq!(inner_cid, cid);
                        break;
                    }
                },
            }
        }

        let list = swarm1.behaviour().peer_wantlist(peer2);
        assert_eq!(list[0], cid);

        Ok(())
    }

    async fn build_swarm() -> (PeerId, Multiaddr, Swarm<super::Behaviour>, Repo) {
        let repo = Repo::new_memory();

        let mut swarm = SwarmBuilder::with_new_identity()
            .with_tokio()
            .with_tcp(
                libp2p::tcp::Config::default(),
                libp2p::noise::Config::new,
                libp2p::yamux::Config::default,
            )
            .expect("")
            .with_behaviour(|_| super::Behaviour::new(&repo))
            .expect("")
            .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(30)))
            .build();

        Swarm::listen_on(&mut swarm, "/ip4/127.0.0.1/tcp/0".parse().unwrap()).unwrap();

        if let Some(SwarmEvent::NewListenAddr { address, .. }) = swarm.next().await {
            let peer_id = swarm.local_peer_id();
            return (*peer_id, address, swarm, repo);
        }

        panic!("no new addrs")
    }
}
