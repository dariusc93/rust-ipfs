mod message;
mod pb;
mod prefix;
mod protocol;

use std::{
    collections::{hash_map::Entry, BTreeSet, HashMap, HashSet, VecDeque},
    task::{Context, Poll, Waker},
    time::Duration,
};

use bytes::Bytes;
use futures::{
    stream::{BoxStream, SelectAll},
    StreamExt,
};
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

type StreamList = SelectAll<BoxStream<'static, TaskHandle>>;

#[derive(Debug)]
enum TaskHandle {
    SendMessage { message: BitswapMessage },
    HaveBlock { cid: Cid },
    DontHaveBlock { cid: Cid },
    BlockStored { cid: Cid },
}

pub struct Behaviour {
    events: VecDeque<ToSwarm<<Self as NetworkBehaviour>::ToSwarm, THandlerInEvent<Self>>>,
    connections: HashMap<PeerId, HashSet<(ConnectionId, Multiaddr)>>,
    blacklist_connections: HashMap<PeerId, BTreeSet<ConnectionId>>,
    store: Repo,
    ledger: Ledger,
    tasks: StreamMap<PeerId, StreamList>,
    waker: Option<Waker>,
}

impl Behaviour {
    pub fn new(store: &Repo) -> Self {
        Self {
            events: Default::default(),
            connections: Default::default(),
            blacklist_connections: Default::default(),
            store: store.clone(),
            ledger: Ledger::default(),
            tasks: StreamMap::new(),
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
            self.ledger.local_want_list.insert(*cid, 1);
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
        let ledger = &self.ledger;
        ledger.local_want_list.keys().copied().collect()
    }

    pub fn peer_wantlist(&self, peer_id: PeerId) -> Vec<Cid> {
        let ledger = &self.ledger;
        ledger
            .peer_wantlist
            .get(&peer_id)
            .map(|list| list.keys().copied().collect())
            .unwrap_or_default()
    }

    // Note: This is called specifically to cancel the request and not just emitting a request
    //       after receiving a request.
    pub fn cancel(&mut self, cid: Cid) {
        let ledger = &mut self.ledger;

        if ledger.local_want_list.remove(&cid).is_none() {
            return;
        }

        let request = BitswapMessage::default().add_request(BitswapRequest::cancel(cid));

        if let Some(peers) = ledger.sent_wants.remove(&cid) {
            for peer_id in peers {
                if !self.connections.contains_key(&peer_id) {
                    continue;
                }

                self.events.push_back(ToSwarm::NotifyHandler {
                    peer_id,
                    handler: NotifyHandler::Any,
                    event: request.clone(),
                });
            }
        }

        if let Some(list) = ledger.have_block.remove(&cid) {
            for peer_id in list {
                self.events.push_back(ToSwarm::NotifyHandler {
                    peer_id,
                    handler: NotifyHandler::Any,
                    event: request.clone(),
                });
            }
        }

        let mut pending = vec![];

        ledger.pending_have_block.retain(|peer_id, list| {
            if list.remove(&cid) {
                pending.push(*peer_id);
            }
            !list.is_empty()
        });

        for peer_id in pending {
            if self.connections.contains_key(&peer_id) {
                self.events.push_back(ToSwarm::NotifyHandler {
                    peer_id,
                    handler: NotifyHandler::Any,
                    event: request.clone(),
                });
            }
        }

        self.events
            .push_back(ToSwarm::GenerateEvent(Event::CancelBlock { cid }));
    }

    // This will notify connected peers who have the bitswap protocol that we have this block
    // if they wanted it
    pub fn notify_new_blocks(&mut self, cid: impl IntoIterator<Item = Cid>) {
        let blocks = cid.into_iter().collect::<Vec<_>>();
        let ledger = &self.ledger;

        for (peer_id, wantlist) in &ledger.peer_wantlist {
            if !self.connections.contains_key(peer_id) {
                tracing::warn!(%peer_id, "peer is unexpectedly not connected");
                continue;
            }
            let mut message = BitswapMessage::default();
            for block in &blocks {
                if !wantlist.contains_key(block) {
                    continue;
                }

                message = message.add_response(*block, BitswapResponse::Have(true));
            }

            if message.responses.is_empty() {
                continue;
            }

            self.events.push_back(ToSwarm::NotifyHandler {
                peer_id: *peer_id,
                handler: NotifyHandler::Any,
                event: message,
            });
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

        let mut futs = SelectAll::new();
        futs.push(futures::stream::pending().boxed());
        self.tasks.insert(peer_id, futs);

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
        let ledger = &mut self.ledger;
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
            self.tasks.remove(&peer_id);

            ledger.sent_wants.retain(|_, list| {
                list.remove(&peer_id);
                !list.is_empty()
            });

            ledger.have_block.retain(|_, list| {
                list.retain(|peer| *peer != peer_id);
                !list.is_empty()
            });

            ledger.peer_wantlist.remove(&peer_id);
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

        let ledger = &mut self.ledger;

        // Remove entry from all wants
        for list in ledger.sent_wants.values_mut() {
            list.remove(&peer_id);
        }
    }

    fn send_wants(&mut self, peers: Vec<PeerId>) {
        let ledger = &mut self.ledger;
        let list = Vec::from_iter(ledger.local_want_list.keys().copied());

        if list.is_empty() {
            return;
        }

        if let Some(waker) = self.waker.take() {
            waker.wake();
        }

        let requests = list
            .iter()
            .map(|cid| BitswapRequest::have(*cid).send_dont_have(true))
            .collect::<Vec<_>>();

        for cid in list {
            let wants = ledger.sent_wants.entry(cid).or_default();
            for peer_id in &peers {
                wants.insert(*peer_id);
            }
        }

        let requests = peers
            .into_iter()
            .map(|peer_id| {
                (
                    peer_id,
                    BitswapMessage::default().set_requests(requests.clone()),
                )
            })
            .collect::<VecDeque<_>>();

        for (peer_id, message) in requests {
            self.events.push_back(ToSwarm::NotifyHandler {
                peer_id,
                handler: NotifyHandler::Any,
                event: message,
            });
        }
    }

    fn process_handle(&mut self, peer_id: PeerId, handle: TaskHandle) {
        let ledger = &mut self.ledger;
        match handle {
            TaskHandle::SendMessage { message } => {
                self.events.push_back(ToSwarm::NotifyHandler {
                    peer_id,
                    handler: NotifyHandler::Any,
                    event: message,
                });
            }
            TaskHandle::HaveBlock { cid } => {
                if let Entry::Occupied(mut e) = ledger.sent_wants.entry(cid) {
                    let list = e.get_mut();

                    list.remove(&peer_id);

                    if list.is_empty() {
                        e.remove();
                    }
                }

                tracing::info!(%peer_id, block = %cid, "has block. Pushing into queue");

                let have_block = ledger.have_block.entry(cid).or_default();

                have_block.push_back(peer_id);

                if !ledger
                    .pending_have_block
                    .iter()
                    .any(|(_, list)| list.contains(&cid))
                {
                    let next_peer_id = have_block.pop_front().expect("Valid entry");

                    tracing::debug!(%peer_id, %cid, ?ledger.pending_have_block, "pending have block");

                    if ledger
                        .pending_have_block
                        .entry(next_peer_id)
                        .or_default()
                        .insert(cid)
                    {
                        tracing::info!(%next_peer_id, block = %cid, "requesting block");
                        // if we dont have a pending request for a block that a peer stated they have
                        // we will request it
                        self.events.push_back(ToSwarm::NotifyHandler {
                            peer_id: next_peer_id,
                            handler: NotifyHandler::Any,
                            event: BitswapMessage::default()
                                .add_request(BitswapRequest::block(cid).send_dont_have(true)),
                        });
                    } else {
                        have_block.push_back(peer_id);
                    }
                }
            }
            TaskHandle::DontHaveBlock { cid } => {
                // Since peer does not have the block, we will remove them from the pending wants

                if let Entry::Occupied(mut e) = ledger.sent_wants.entry(cid) {
                    let list = e.get_mut();

                    if !list.remove(&peer_id) {
                        tracing::warn!(%peer_id, block = %cid, "did not request block from peer.");
                    }

                    if list.is_empty() {
                        e.remove();
                    }
                }

                // If the peer for whatever reason sent this after stating they did have the block, remove their pending status,
                // and move to the next available peer in the have list
                tracing::info!(%peer_id, block = %cid, "doesnt have block");

                if let Entry::Occupied(mut e) = ledger.pending_have_block.entry(peer_id) {
                    let list = e.get_mut();
                    list.remove(&cid);

                    tracing::info!(%peer_id, block = %cid, "canceling request from peer");
                    self.events.push_back(ToSwarm::NotifyHandler {
                        peer_id,
                        handler: NotifyHandler::Any,
                        event: BitswapMessage::default().add_request(BitswapRequest::cancel(cid)),
                    });

                    if list.is_empty() {
                        e.remove();
                    }
                };

                if let Some(next_peer_id) = ledger
                    .have_block
                    .get_mut(&cid)
                    .and_then(|list| list.pop_front())
                {
                    tracing::info!(peer_id=%next_peer_id, block = %cid, "requesting block from next peer");

                    ledger
                        .pending_have_block
                        .entry(next_peer_id)
                        .or_default()
                        .insert(cid);

                    self.events.push_back(ToSwarm::NotifyHandler {
                        peer_id: next_peer_id,
                        handler: NotifyHandler::Any,
                        event: BitswapMessage::default()
                            .add_request(BitswapRequest::block(cid).send_dont_have(true)),
                    });
                } else {
                    tracing::warn!(block = %cid, "no available peers available who have block");
                    self.events
                        .push_back(ToSwarm::GenerateEvent(Event::NeedBlock { cid }));
                }
            }
            TaskHandle::BlockStored { cid } => {
                tracing::info!(%peer_id, block = %cid, "block is received by peer. removing from local wantlist");
                if ledger.local_want_list.remove(&cid).is_none() {
                    return;
                }

                let message = BitswapMessage::default().add_request(BitswapRequest::cancel(cid));

                tracing::debug!(block = %cid, "notifying pending_have_block");
                // First notify the peer that we sent a block request too
                let mut pending = vec![];

                ledger.pending_have_block.retain(|peer_id, list| {
                    if list.remove(&cid) {
                        pending.push(*peer_id);
                    }
                    !list.is_empty()
                });

                for peer_id in pending {
                    tracing::info!(%peer_id, block = %cid, "sending cancel request to pending_have_block");
                    self.events.push_back(ToSwarm::NotifyHandler {
                        peer_id,
                        handler: NotifyHandler::Any,
                        event: message.clone(),
                    });
                }

                // Second notify the peers we who have notified us that they have the block
                let list = ledger.have_block.remove(&cid).unwrap_or_default();

                for peer_id in list {
                    tracing::debug!(%peer_id, block = %cid, "canceling request");

                    self.events.push_back(ToSwarm::NotifyHandler {
                        peer_id,
                        handler: NotifyHandler::Any,
                        event: message.clone(),
                    });
                }

                // Third notify the peers who we asked for have request but never responsed
                let list = ledger.sent_wants.remove(&cid).unwrap_or_default();

                for peer_id in list {
                    tracing::info!(%peer_id, block = %cid, "canceling request");
                    self.events.push_back(ToSwarm::NotifyHandler {
                        peer_id,
                        handler: NotifyHandler::Any,
                        event: message.clone(),
                    });
                }

                // Finally notify the swarm
                self.events
                    .push_back(ToSwarm::GenerateEvent(Event::BlockRetrieved { cid }));
            }
        }

        // wake task to progress
        if let Some(w) = self.waker.take() {
            w.wake();
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

        let repo = self.store.clone();

        let BitswapMessage {
            requests,
            responses,
        } = BitswapMessage::from_proto(message)
            .map_err(|e| {
                tracing::error!(error = %e, %peer_id, "unable to parse message");
                e
            })
            .unwrap_or_default();

        if requests.is_empty() && responses.is_empty() {
            tracing::warn!(%peer_id, %connection_id, "received an empty message");
            return;
        }

        // split the requests from cancel requests so we can process them here
        // TODO: Should we ignore any additional requests in the message if a cancel request
        //       is present?
        let (requests, canceled_requests): (Vec<_>, Vec<_>) =
            requests.into_iter().partition(|req| !req.cancel);

        // Remove cid from peer wantlist if cancelled
        let mut cancelled = HashSet::new();

        // Note: If the cancel request is sent along side a have request, it will cancel both out.
        for request in canceled_requests {
            if !cancelled.insert(request.cid) {
                continue;
            }
            tracing::info!(cid = %request.cid, %peer_id, %connection_id, "receive cancel request");
            if let Entry::Occupied(mut e) = self.ledger.peer_wantlist.entry(peer_id) {
                let list = e.get_mut();
                list.remove(&request.cid);
                tracing::info!(cid= %request.cid, %peer_id, "canceled block");
                if list.is_empty() {
                    e.remove();
                }
            }
        }

        let requests = requests
            .into_iter()
            .filter(|req| !cancelled.contains(&req.cid))
            .collect::<Vec<_>>();

        let responses = responses
            .into_iter()
            .filter(|(cid, _)| !cancelled.contains(cid))
            .collect::<Vec<_>>();

        // add peer wantlist into ledger
        for request in &requests {
            if request.ty == RequestType::Have {
                self.ledger
                    .peer_wantlist
                    .entry(peer_id)
                    .or_default()
                    .insert(request.cid, request.priority);
            }
        }

        // We check again in case the intended requests and responses are actually empty after filtering
        if requests.is_empty() && responses.is_empty() {
            return;
        }

        // split the haves and blocks from the responses
        let (haves, blocks): (HashMap<_, _>, HashMap<_, _>) = responses
            .into_iter()
            .partition(|(_, res)| matches!(res, BitswapResponse::Have(_)));

        for (cid, response) in haves {
            if let BitswapResponse::Have(have) = response {
                match have {
                    true => self.process_handle(peer_id, TaskHandle::HaveBlock { cid }),
                    false => self.process_handle(peer_id, TaskHandle::DontHaveBlock { cid }),
                }
            }
        }

        let (blocks, unwanted_blocks): (HashMap<_, _>, HashMap<_, _>) = blocks
            .into_iter()
            .partition(|(cid, _)| self.ledger.local_want_list.contains_key(cid));

        for (cid, _) in unwanted_blocks {
            tracing::info!(%cid, %peer_id, %connection_id, "did not request block. Ignoring response.");
        }

        let task_handler = self
            .tasks
            .iter_mut()
            .find(|(internal_peer_id, _)| peer_id == *internal_peer_id)
            .map(|(_, futs)| futs)
            .expect("connection exist");

        let stream = async_stream::stream! {
            let mut message = BitswapMessage::default();
            for request in requests {
                tracing::debug!(request_cid = %request.cid, %peer_id, %connection_id, "receive request");
                if request.cancel {
                    tracing::warn!(request_cid = %request.cid, %peer_id, %connection_id, "receive cancel request although it was previous filtered out");
                    continue;
                }

                let Some(response) = handle_inbound_request(&repo, &request).await else {
                    tracing::warn!(request_cid = %request.cid, %peer_id, %connection_id, "unable to handle inbound request or the request has been canceled");
                    continue;
                };
                tracing::trace!(request_cid = %request.cid, %peer_id, %connection_id, ?response, "sending response");
                message = message.add_response(request.cid, response);
            }

            if !message.is_empty() {
                yield TaskHandle::SendMessage { message };
            }

            for (cid, response) in blocks {
                tracing::info!(%cid, %peer_id, %connection_id, "received response");
                if let BitswapResponse::Block(data) = response {
                    tracing::info!(block = %cid, %peer_id, %connection_id, block_size=data.len(), "received block");
                    if repo.contains(&cid).await.unwrap_or_default() {
                        tracing::info!(block = %cid, %peer_id, %connection_id, "block exist locally. skipping");
                        yield TaskHandle::BlockStored { cid };
                        continue;
                    }

                    let Ok(block) = Block::new(cid, data.to_vec()) else {
                        // The block is invalid so we will notify the behaviour that we still dont have the block
                        // from said peer
                        tracing::error!(block = %cid, %peer_id, %connection_id, "block is invalid or corrupted");
                        yield TaskHandle::DontHaveBlock { cid };
                        continue;
                    };

                    match repo.put_block(block).await {
                        Ok(local_cid) => {
                            tracing::info!(block = %local_cid, %peer_id, %connection_id, "block stored in block store.");
                            yield TaskHandle::BlockStored { cid }
                        },
                        Err(e) => {
                            tracing::error!(block = %cid, %peer_id, %connection_id, error = %e, "error inserting block into block store");
                            yield TaskHandle::DontHaveBlock { cid };
                            continue;
                        }
                    }
                }
            }
        };

        task_handler.push(stream.boxed());
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

        while let Poll::Ready(Some((peer_id, handle))) = self.tasks.poll_next_unpin(ctx) {
            self.process_handle(peer_id, handle);
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

#[derive(Default)]
pub struct Ledger {
    pub local_want_list: HashMap<Cid, i32>,
    pub peer_wantlist: HashMap<PeerId, HashMap<Cid, i32>>,
    pub sent_wants: HashMap<Cid, HashSet<PeerId>>,
    pub have_block: HashMap<Cid, VecDeque<PeerId>>,
    pub pending_have_block: HashMap<PeerId, HashSet<Cid>>,
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
