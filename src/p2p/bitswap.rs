mod codec;
mod message;
mod pb;
mod prefix;
mod process_events;

use std::{
    collections::{hash_map::Entry, HashMap, HashSet, VecDeque},
    task::{Context, Poll},
};

use futures::{channel::mpsc, SinkExt, StreamExt};
use libipld::{Block, Cid};
use libp2p::{
    core::Endpoint,
    request_response::{self, OutboundRequestId, ProtocolSupport},
    swarm::{
        behaviour::ConnectionEstablished, ConnectionClosed, ConnectionDenied, ConnectionId,
        FromSwarm, NetworkBehaviour, THandler, THandlerInEvent, THandlerOutEvent, ToSwarm,
    },
    Multiaddr, PeerId,
};

use crate::repo::Repo;

use self::{
    message::{BitswapMessage, BitswapRequest, BitswapResponse},
    process_events::handle_inbound_request,
};

#[derive(Debug)]
pub enum Event {
    NeedBlock { cid: Cid },
}

mod bitswap_pb {
    pub use super::pb::bitswap_pb::Message;
    pub mod message {
        use super::super::pb::bitswap_pb::mod_Message as message;
        pub use message::mod_Wantlist as wantlist;
        pub use message::Wantlist;
        pub use message::{Block, BlockPresence, BlockPresenceType};
    }
}

pub struct Behaviour {
    events: VecDeque<ToSwarm<<Self as NetworkBehaviour>::ToSwarm, THandlerInEvent<Self>>>,
    inner: request_response::Behaviour<codec::Codec>,
    wants_list: HashSet<Cid>,
    repo: Repo,
    sent_wants: HashMap<Cid, HashSet<PeerId>>,
    have_block: HashMap<Cid, VecDeque<PeerId>>,
    pending_have_block: HashMap<Cid, (PeerId, Option<OutboundRequestId>)>,
    connections: HashMap<PeerId, HashSet<(ConnectionId, Multiaddr)>>,
    task_tx: mpsc::Sender<TaskHandle>,
    task_rx: mpsc::Receiver<TaskHandle>,
}

#[derive(Debug)]
enum TaskHandle {
    SendResponse {
        peer_id: PeerId,
        source: (Cid, BitswapResponse),
    },
    HaveBlock {
        peer_id: PeerId,
        cid: Cid,
    },
    DontHaveBlock {
        peer_id: PeerId,
        cid: Cid,
    },
    BlockStored {
        cid: Cid,
    },
}

impl Behaviour {
    pub fn new(repo: &Repo) -> Self {
        let (task_tx, task_rx) = mpsc::channel(256);
        Self {
            events: VecDeque::new(),
            inner: request_response::Behaviour::new(
                [("/ipfs/bitswap/1.2.0", ProtocolSupport::Full)],
                Default::default(),
            ),
            wants_list: HashSet::new(),
            have_block: HashMap::new(),
            pending_have_block: HashMap::new(),
            sent_wants: HashMap::new(),
            connections: HashMap::new(),
            repo: repo.clone(),
            task_rx,
            task_tx,
        }
    }

    pub fn get(&mut self, cid: &Cid, providers: &[PeerId]) {
        self.wants_list.insert(*cid);

        let mut peers = match providers.is_empty() {
            true => {
                //If no providers are provided, we can send requests connected peers
                self.connections.keys().copied().collect::<VecDeque<_>>()
            }
            false => VecDeque::from_iter(providers.to_vec()),
        };

        if peers.is_empty() {
            // Since no peers or providers are provided, we need to notify swarm to attempt a form of content discovery
            self.events
                .push_back(ToSwarm::GenerateEvent(Event::NeedBlock { cid: *cid }));
            return;
        }

        let first_peer_id = peers.pop_front().expect("valid entry");

        let first_request = (
            &first_peer_id,
            BitswapRequest::new_block(*cid).send_dont_have(true),
        );

        let mut requests = peers
            .iter()
            .map(|peer_id| (peer_id, BitswapRequest::new_have(*cid).send_dont_have(true)))
            .collect::<VecDeque<_>>();

        requests.push_front(first_request);

        for (peer_id, request) in requests {
            self.inner
                .send_request(peer_id, vec![BitswapMessage::Request(request)]);

            self.sent_wants.entry(*cid).or_default().insert(*peer_id);
        }
    }

    pub fn gets(&mut self, cid: Vec<Cid>, providers: &[PeerId]) {
        for cid in cid {
            self.get(&cid, providers)
        }
    }

    fn process_message(&mut self, peer_id: PeerId, messages: Vec<BitswapMessage>) {
        let repo = self.repo.clone();
        let mut tx = self.task_tx.clone();
        tokio::spawn(async move {
            for message in messages {
                match message {
                    BitswapMessage::Request(request) => {
                        if let Some(response) = handle_inbound_request(&repo, &request).await {
                            _ = tx
                                .send(TaskHandle::SendResponse {
                                    peer_id,
                                    source: (request.cid, response),
                                })
                                .await;
                        }
                    }
                    BitswapMessage::Response(cid, response) => {
                        match response {
                            BitswapResponse::Have(have) => {
                                if have {
                                    _ = tx.send(TaskHandle::HaveBlock { peer_id, cid }).await;
                                } else {
                                    _ = tx.send(TaskHandle::DontHaveBlock { peer_id, cid }).await;
                                }
                            }
                            BitswapResponse::Block(data) => {
                                let Ok(block) = Block::new(cid, data) else {
                                    // The block is invalid so we will notify the behaviour that we still dont have the block
                                    // from said peer
                                    _ = tx.send(TaskHandle::DontHaveBlock { peer_id, cid }).await;
                                    continue;
                                };

                                _ = repo.put_block(block).await;

                                _ = tx.send(TaskHandle::BlockStored { cid }).await;
                            }
                        };
                    }
                }
            }
        });
    }

    fn process_handle(&mut self, handle: TaskHandle) {
        match handle {
            TaskHandle::SendResponse {
                peer_id,
                source: (cid, response),
            } => {
                self.inner
                    .send_request(&peer_id, vec![BitswapMessage::Response(cid, response)]);
            }
            TaskHandle::HaveBlock { peer_id, cid } => {
                if let Entry::Occupied(mut e) = self.sent_wants.entry(cid) {
                    let list = e.get_mut();
                    list.remove(&peer_id);
                    if list.is_empty() {
                        e.remove();
                    }
                }
                self.have_block.entry(cid).or_default().push_back(peer_id);
                if let Entry::Vacant(e) = self.pending_have_block.entry(cid) {
                    let next_peer_id = self
                        .have_block
                        .entry(cid)
                        .or_default()
                        .pop_front()
                        .expect("Valid entry");
                    //If we dont have a pending request for a block that a peer stated they have
                    //we will request it
                    let id = self.inner.send_request(
                        &next_peer_id,
                        vec![BitswapMessage::Request(
                            BitswapRequest::new_block(cid).send_dont_have(true),
                        )],
                    );

                    // We store the outbound id to mark this as "pending". If the peer
                    // successfully receives the request and responded to the stream
                    // we will assume it has been receive and "take" the id out, pending
                    // a response for the block.
                    e.insert((next_peer_id, Some(id)));
                }
            }
            TaskHandle::DontHaveBlock { peer_id, cid } => {
                // Since peer does not have the block, we will remove them from the pending wants

                if let Entry::Occupied(mut e) = self.sent_wants.entry(cid) {
                    let list = e.get_mut();
                    list.remove(&peer_id);
                    if list.is_empty() {
                        e.remove();
                    }
                }

                // If the peer for whatever reason sent this after stating they did have the block, remove their pending status,
                // and move to the next available peer in the have list
                if self
                    .pending_have_block
                    .get(&cid)
                    .map(|(pid, _)| *pid == peer_id)
                    .unwrap_or_default()
                {
                    self.pending_have_block.remove(&cid);
                    let Some(next_peer_id) = self.have_block.entry(cid).or_default().pop_front()
                    else {
                        return;
                    };
                    let id = self.inner.send_request(
                        &next_peer_id,
                        vec![BitswapMessage::Request(
                            BitswapRequest::new_block(cid).send_dont_have(true),
                        )],
                    );

                    self.pending_have_block.insert(cid, (peer_id, Some(id)));
                }

                // If there are no peers, notify swarm
                if !self.pending_have_block.contains_key(&cid)
                    && !self.have_block.contains_key(&cid)
                {
                    self.events
                        .push_back(ToSwarm::GenerateEvent(Event::NeedBlock { cid }));
                }
            }
            TaskHandle::BlockStored { cid } => {
                self.pending_have_block.remove(&cid);
                let list = self.have_block.remove(&cid).unwrap_or_default();

                for peer_id in list {
                    self.inner.send_request(
                        &peer_id,
                        vec![BitswapMessage::Request(BitswapRequest::new_cancel(cid))],
                    );
                }
            }
        }
    }
}

impl NetworkBehaviour for Behaviour {
    type ConnectionHandler =
        <request_response::Behaviour<codec::Codec> as NetworkBehaviour>::ConnectionHandler;

    type ToSwarm = Event;

    fn handle_established_inbound_connection(
        &mut self,
        connection_id: ConnectionId,
        peer: PeerId,
        local_addr: &Multiaddr,
        remote_addr: &Multiaddr,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        self.inner.handle_established_inbound_connection(
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
    ) -> Result<THandler<Self>, ConnectionDenied> {
        self.inner
            .handle_established_outbound_connection(connection_id, peer, addr, role_override)
    }

    fn handle_pending_inbound_connection(
        &mut self,
        connection_id: ConnectionId,
        local_addr: &Multiaddr,
        remote_addr: &Multiaddr,
    ) -> Result<(), ConnectionDenied> {
        self.inner
            .handle_pending_inbound_connection(connection_id, local_addr, remote_addr)
    }

    fn handle_pending_outbound_connection(
        &mut self,
        connection_id: ConnectionId,
        maybe_peer: Option<PeerId>,
        addresses: &[Multiaddr],
        effective_role: Endpoint,
    ) -> Result<Vec<libp2p::Multiaddr>, ConnectionDenied> {
        self.inner.handle_pending_outbound_connection(
            connection_id,
            maybe_peer,
            addresses,
            effective_role,
        )
    }

    fn on_connection_handler_event(
        &mut self,
        peer_id: PeerId,
        connection_id: ConnectionId,
        event: THandlerOutEvent<Self>,
    ) {
        self.inner
            .on_connection_handler_event(peer_id, connection_id, event)
    }

    fn on_swarm_event(&mut self, event: FromSwarm) {
        match &event {
            FromSwarm::ConnectionEstablished(ConnectionEstablished {
                peer_id,
                connection_id,
                endpoint,
                ..
            }) => {
                let addr = endpoint.get_remote_address().clone();
                self.connections
                    .entry(*peer_id)
                    .or_default()
                    .insert((*connection_id, addr));

                // Send want list, only if we havent already
                for cid in self.wants_list.clone() {
                    if !self
                        .sent_wants
                        .get(&cid)
                        .map(|list| list.contains(peer_id))
                        .unwrap_or_default()
                    {
                        self.get(&cid, &[*peer_id]);
                    }
                }
            }
            FromSwarm::ConnectionClosed(ConnectionClosed {
                peer_id,
                connection_id,
                endpoint,
                ..
            }) => {
                let addr = endpoint.get_remote_address().clone();
                if let Entry::Occupied(mut e) = self.connections.entry(*peer_id) {
                    let addrs = e.get_mut();
                    addrs.remove(&(*connection_id, addr));
                    if addrs.is_empty() {
                        e.remove();
                    }
                }

                self.sent_wants.retain(|_, list| {
                    list.remove(peer_id);
                    !list.is_empty()
                });

                self.have_block.retain(|_, list| {
                    list.retain(|peer| peer != peer_id);
                    !list.is_empty()
                });
            }
            _ => {}
        };

        self.inner.on_swarm_event(event)
    }

    fn poll(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<ToSwarm<Self::ToSwarm, THandlerInEvent<Self>>> {
        if let Some(event) = self.events.pop_front() {
            return Poll::Ready(event);
        }

        while let Poll::Ready(event) = self.inner.poll(cx) {
            match event {
                ToSwarm::GenerateEvent(request_response::Event::Message { peer, message }) => {
                    match message {
                        request_response::Message::Request {
                            request, channel, ..
                        } => {
                            _ = self.inner.send_response(channel, ());
                            self.process_message(peer, request);
                        }

                        request_response::Message::Response { .. } => {}
                    }

                    continue;
                }
                ToSwarm::GenerateEvent(request_response::Event::InboundFailure { .. }) => {
                    continue;
                }
                ToSwarm::GenerateEvent(request_response::Event::ResponseSent { .. }) => {
                    continue;
                }
                other @ (ToSwarm::ExternalAddrConfirmed(_)
                | ToSwarm::ExternalAddrExpired(_)
                | ToSwarm::NewExternalAddrCandidate(_)
                | ToSwarm::NotifyHandler { .. }
                | ToSwarm::Dial { .. }
                | ToSwarm::CloseConnection { .. }
                | ToSwarm::ListenOn { .. }
                | ToSwarm::RemoveListener { .. }) => {
                    let new_to_swarm =
                        other.map_out(|_| unreachable!("we manually map `GenerateEvent` variants"));
                    return Poll::Ready(new_to_swarm);
                }
                _ => {}
            };
        }

        while let Poll::Ready(Some(handle)) = self.task_rx.poll_next_unpin(cx) {
            self.process_handle(handle);
        }

        Poll::Pending
    }
}
