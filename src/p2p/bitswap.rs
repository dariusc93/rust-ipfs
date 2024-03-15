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
        THandler, THandlerInEvent, THandlerOutEvent, ToSwarm,
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
}

type StreamList = SelectAll<BoxStream<'static, TaskHandle>>;

#[derive(Debug)]
enum TaskHandle {
    SendResponse { source: (Cid, BitswapResponse) },
    HaveBlock { cid: Cid },
    DontHaveBlock { cid: Cid },
    BlockStored { cid: Cid },
    Cancel { cid: Cid },
}

pub struct Behaviour {
    events: VecDeque<ToSwarm<<Self as NetworkBehaviour>::ToSwarm, THandlerInEvent<Self>>>,
    connections: HashMap<PeerId, HashSet<(ConnectionId, Multiaddr)>>,
    blacklist_connections: HashMap<PeerId, BTreeSet<ConnectionId>>,
    store: Repo,
    local_wants_list: HashSet<Cid>,
    sent_wants: HashMap<Cid, HashSet<PeerId>>,
    have_block: HashMap<Cid, VecDeque<(PeerId, ConnectionId)>>,
    pending_have_block: HashMap<Cid, PeerId>,
    tasks: StreamMap<(PeerId, ConnectionId), StreamList>,
    waker: Option<Waker>,
}

impl Behaviour {
    pub fn new(store: &Repo) -> Self {
        Self {
            events: Default::default(),
            connections: Default::default(),
            blacklist_connections: Default::default(),
            store: store.clone(),
            tasks: StreamMap::new(),
            local_wants_list: Default::default(),
            sent_wants: Default::default(),
            pending_have_block: Default::default(),
            have_block: HashMap::new(),
            waker: None,
        }
    }

    pub fn get(&mut self, cid: &Cid, providers: &[PeerId]) {
        self.local_wants_list.insert(*cid);

        let wants = self.sent_wants.entry(*cid).or_default();

        let peers = match providers.is_empty() {
            true => {
                //If no providers are provided, we can send requests connected peers
                self.connections
                    .keys()
                    .filter(|peer_id| !self.blacklist_connections.contains_key(peer_id))
                    .copied()
                    .collect::<VecDeque<_>>()
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
                    wants.insert(*peer_id);
                }
                connected
            }
        };

        if peers.is_empty() {
            // Since no connections, peers or providers are provided, we need to notify swarm to attempt a form of content discovery
            self.events
                .push_back(ToSwarm::GenerateEvent(Event::NeedBlock { cid: *cid }));
            return;
        }

        // We take the first peer in the list and ask for the block directly while the remaining peers
        // we ask if they have said block
        // let first_peer_id = peers.pop_front().expect("valid entry");

        // let first_request = (
        //     &first_peer_id,
        //     BitswapMessage::Request(BitswapRequest::block(*cid).send_dont_have(true)),
        // );

        let requests = peers
            .iter()
            .map(|peer_id| {
                (
                    peer_id,
                    BitswapMessage::Request(BitswapRequest::have(*cid).send_dont_have(true)),
                )
            })
            .collect::<VecDeque<_>>();

        // requests.push_front(first_request);

        for (peer_id, message) in requests {
            self.events.push_back(ToSwarm::NotifyHandler {
                peer_id: *peer_id,
                handler: NotifyHandler::Any,
                event: message,
            });
            wants.insert(*peer_id);
        }

        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }

    pub fn gets(&mut self, cid: Vec<Cid>, providers: &[PeerId]) {
        for cid in cid {
            self.get(&cid, providers)
        }
    }

    pub fn notify_new_blocks(&mut self, cid: impl IntoIterator<Item = Cid>) {
        let blocks = cid
            .into_iter()
            .map(|cid| BitswapMessage::Response(cid, BitswapResponse::Have(true)))
            .collect::<Vec<_>>();

        for block in &blocks {
            self.events.extend(
                self.connections
                    .keys()
                    .filter(|peer_id| !self.blacklist_connections.contains_key(peer_id))
                    .map(|peer_id| ToSwarm::NotifyHandler {
                        peer_id: *peer_id,
                        handler: NotifyHandler::Any,
                        event: block.clone(),
                    }),
            )
        }
    }

    fn on_connection_established(
        &mut self,
        ConnectionEstablished {
            connection_id,
            peer_id,
            endpoint,
            ..
        }: ConnectionEstablished,
    ) {
        let address = endpoint.get_remote_address().clone();
        self.connections
            .entry(peer_id)
            .or_default()
            .insert((connection_id, address));

        let mut futs = SelectAll::new();
        futs.push(futures::stream::pending().boxed());
        self.tasks.insert((peer_id, connection_id), futs);
        self.send_wants(peer_id);
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

        self.tasks.remove(&(peer_id, connection_id));

        if remaining_established == 0 {
            self.sent_wants.retain(|_, list| {
                list.remove(&peer_id);
                !list.is_empty()
            });

            self.have_block.retain(|_, list| {
                list.retain(|(peer, id)| *peer != peer_id && *id != connection_id);
                !list.is_empty()
            });
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

        // Remove entry from all wants
        for list in self.sent_wants.values_mut() {
            list.remove(&peer_id);
        }
    }

    fn send_wants(&mut self, peer_id: PeerId) {
        let list = self.local_wants_list.iter().copied().collect::<Vec<_>>();

        for cid in list {
            if !self
                .sent_wants
                .get(&cid)
                .map(|list| list.contains(&peer_id))
                .unwrap_or_default()
            {
                self.get(&cid, &[peer_id]);
            }
        }
    }

    fn process_handle(
        &mut self,
        peer_id: PeerId,
        connection_id: ConnectionId,
        handle: TaskHandle,
    ) -> Option<ToSwarm<<Behaviour as NetworkBehaviour>::ToSwarm, THandlerInEvent<Self>>> {
        match handle {
            TaskHandle::SendResponse {
                source: (cid, response),
            } => {
                return Some(ToSwarm::NotifyHandler {
                    peer_id,
                    handler: NotifyHandler::One(connection_id),
                    event: BitswapMessage::Response(cid, response),
                })
            }
            TaskHandle::HaveBlock { cid } => {
                if let Entry::Occupied(mut e) = self.sent_wants.entry(cid) {
                    let list = e.get_mut();
                    list.remove(&peer_id);
                    if list.is_empty() {
                        e.remove();
                    }
                }
                self.have_block
                    .entry(cid)
                    .or_default()
                    .push_back((peer_id, connection_id));

                if let Entry::Vacant(e) = self.pending_have_block.entry(cid) {
                    let (next_peer_id, connection_id) = self
                        .have_block
                        .get_mut(&cid)
                        .and_then(|list| list.pop_front())
                        .expect("Valid entry");

                    e.insert(next_peer_id);

                    // f we dont have a pending request for a block that a peer stated they have
                    // we will request it
                    return Some(ToSwarm::NotifyHandler {
                        peer_id: next_peer_id,
                        handler: NotifyHandler::One(connection_id),
                        event: BitswapMessage::Request(
                            BitswapRequest::block(cid).send_dont_have(true),
                        ),
                    });
                }
            }
            TaskHandle::DontHaveBlock { cid } => {
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
                    .map(|pid| *pid == peer_id)
                    .unwrap_or_default()
                {
                    self.pending_have_block.remove(&cid);
                    let Some((next_peer_id, next_connection_id)) = self
                        .have_block
                        .get_mut(&cid)
                        .and_then(|list| list.pop_front())
                    else {
                        return None;
                    };

                    self.pending_have_block.insert(cid, peer_id);

                    return Some(ToSwarm::NotifyHandler {
                        peer_id: next_peer_id,
                        handler: NotifyHandler::One(next_connection_id),
                        event: BitswapMessage::Request(
                            BitswapRequest::block(cid).send_dont_have(true),
                        ),
                    });
                }

                // If there are no peers, notify swarm
                if !self.pending_have_block.contains_key(&cid)
                    && !self.have_block.contains_key(&cid)
                {
                    return Some(ToSwarm::GenerateEvent(Event::NeedBlock { cid }));
                }
            }
            TaskHandle::BlockStored { cid } => {
                self.pending_have_block.remove(&cid);
                let list = self.have_block.remove(&cid).unwrap_or_default();

                for (peer_id, connection_id) in list {
                    self.events.push_front(ToSwarm::NotifyHandler {
                        peer_id,
                        handler: NotifyHandler::One(connection_id),
                        event: BitswapMessage::Request(BitswapRequest::cancel(cid)),
                    });
                }

                return Some(ToSwarm::GenerateEvent(Event::BlockRetrieved { cid }));
            }
            TaskHandle::Cancel { cid } => {
                _ = cid;
            }
        }
        None
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
        Ok(OneShotHandler::default())
    }

    fn handle_established_outbound_connection(
        &mut self,
        _: ConnectionId,
        _: PeerId,
        _: &Multiaddr,
        _: Endpoint,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        Ok(OneShotHandler::default())
    }

    fn on_connection_handler_event(
        &mut self,
        peer_id: PeerId,
        connection_id: ConnectionId,
        event: THandlerOutEvent<Self>,
    ) {
        let message = match event {
            Ok(Message::Receive { message }) => {
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
                if let Entry::Occupied(mut e) = self.blacklist_connections.entry(peer_id) {
                    let list = e.get_mut();
                    list.remove(&connection_id);
                    if list.is_empty() {
                        e.remove();
                    }
                }
                return;
            }
            Err(_) => {
                //TODO: Depending on the underlining error, maybe blacklist the peer from further sending/receiving
                //      until a valid response or request is produced?
                self.blacklist_connections
                    .entry(peer_id)
                    .or_default()
                    .insert(connection_id);
                return;
            }
        };

        let messages = BitswapMessage::from_proto(message).unwrap_or_default();

        let task_handler = self
            .tasks
            .iter_mut()
            .find(|((internal_peer_id, internal_connection_id), _)| {
                peer_id == *internal_peer_id && *internal_connection_id == connection_id
            })
            .map(|(_, futs)| futs)
            .expect("connection exist");

        // process each message in its own task
        let repo = self.store.clone();
        let stream = async_stream::stream! {
            for message in messages {
                match message {
                    BitswapMessage::Request(request) => {
                        if request.cancel {
                            yield TaskHandle::Cancel { cid: request.cid };
                            continue;
                        }

                        let Some(response) = handle_inbound_request(&repo, &request).await else {
                            continue;
                        };

                        yield TaskHandle::SendResponse {
                            source: (request.cid, response),
                        }
                    }
                    BitswapMessage::Response(cid, response) => {
                        match response {
                            BitswapResponse::Have(have) => {
                                if have {
                                    yield TaskHandle::HaveBlock { cid }
                                } else {
                                    yield TaskHandle::DontHaveBlock { cid }
                                }
                            }
                            BitswapResponse::Block(data) => {
                                let Ok(block) = Block::new(cid, data.to_vec()) else {
                                    // The block is invalid so we will notify the behaviour that we still dont have the block
                                    // from said peer
                                    yield TaskHandle::DontHaveBlock { cid };
                                    continue;
                                };

                                _ = repo.put_block(block).await;

                                yield TaskHandle::BlockStored { cid }
                            }
                        };
                    }
                };
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

        while let Poll::Ready(Some(((peer_id, connection_id), handle))) =
            self.tasks.poll_next_unpin(ctx)
        {
            if let Some(event) = self.process_handle(peer_id, connection_id, handle) {
                return Poll::Ready(event);
            }
        }

        self.waker = Some(ctx.waker().clone());

        Poll::Pending
    }
}

#[derive(Debug, thiserror::Error)]
pub enum BitswapError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("Supplied wantlist is empty")]
    EmptyWantList,
    #[error("Entries exceeded max")]
    MaxEntryExceeded,
}

pub enum BitswapResult {
    Response {
        peer_id: PeerId,
        connection_id: ConnectionId,
        message: bitswap_pb::Message,
    },
    Cancel {
        message: bitswap_pb::Message,
    },
    EmptyResponse,
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
            if have || request.send_dont_have {
                Some(BitswapResponse::Have(have))
            } else {
                None
            }
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

    async fn build_swarm() -> (PeerId, Multiaddr, Swarm<super::Behaviour>, Repo) {
        let repo = Repo::new_memory(None);

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
