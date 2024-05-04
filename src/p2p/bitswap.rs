mod message;
mod pb;
mod prefix;
mod protocol;
mod sessions;

use std::{
    collections::{hash_map::Entry, BTreeSet, HashMap, HashSet, VecDeque},
    fmt::Debug,
    task::{Context, Poll, Waker},
    time::Duration,
};

use futures::StreamExt;
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
    sessions::{HaveSession, HaveSessionEvent, WantSession, WantSessionEvent},
};

const CAP_THRESHOLD: usize = 100;

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
    want_session: StreamMap<Cid, WantSession>,
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
            let session = WantSession::new(&self.store, *cid);
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

        self.send_wants(peers, cids)
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

        self.send_wants(vec![peer_id], vec![]);
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

    fn send_wants(&mut self, peers: Vec<PeerId>, cids: Vec<Cid>) {
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }

        match cids.is_empty() {
            false => {
                for cid in cids {
                    let Some(session) = self
                        .want_session
                        .iter_mut()
                        .find(|(session_cid, _)| *session_cid == cid)
                        .map(|(_, session)| session)
                    else {
                        continue;
                    };
                    for peer_id in &peers {
                        session.send_have_block(*peer_id)
                    }
                }
            }
            true => {
                for session in self.want_session.values_mut() {
                    for peer_id in &peers {
                        session.send_have_block(*peer_id)
                    }
                }
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
            ..
        } = message;

        for request in requests {
            let BitswapRequest {
                ty,
                cid,
                send_dont_have: _,
                cancel,
                priority: _,
            } = &request;

            if !self.have_session.contains_key(cid) && !cancel {
                // Lets build out have new sessions
                let have_session = HaveSession::new(&self.store, *cid);
                self.have_session.insert(*cid, have_session);
            }

            let Some(session) = self
                .have_session
                .iter_mut()
                .find(|(session_cid, _)| session_cid == cid)
                .map(|(_, session)| session)
            else {
                if !*cancel {
                    tracing::warn!(block = %cid, %peer_id, %connection_id, "have session does not exist. Skipping request");
                }
                continue;
            };

            if *cancel {
                session.cancel(peer_id);
                continue;
            }

            match ty {
                RequestType::Have => {
                    session.want_block(peer_id);
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
        } else if self.events.capacity() > CAP_THRESHOLD {
            self.events.shrink_to_fit();
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
                WantSessionEvent::SendWant { peer_id } => {
                    return Poll::Ready(ToSwarm::NotifyHandler {
                        peer_id,
                        handler: NotifyHandler::Any,
                        event: BitswapMessage::default()
                            .add_request(BitswapRequest::have(cid).send_dont_have(true)),
                    });
                }
                WantSessionEvent::SendCancels { peers } => {
                    for peer_id in peers {
                        self.events.push_back(ToSwarm::NotifyHandler {
                            peer_id,
                            handler: NotifyHandler::Any,
                            event: BitswapMessage::default()
                                .add_request(BitswapRequest::cancel(cid)),
                        });
                    }
                }
                WantSessionEvent::SendBlock { peer_id } => {
                    return Poll::Ready(ToSwarm::NotifyHandler {
                        peer_id,
                        handler: NotifyHandler::Any,
                        event: BitswapMessage::default()
                            .add_request(BitswapRequest::block(cid).send_dont_have(true)),
                    });
                }
                WantSessionEvent::NeedBlock => {
                    return Poll::Ready(ToSwarm::GenerateEvent(Event::NeedBlock { cid }));
                }
                WantSessionEvent::BlockStored => {
                    return Poll::Ready(ToSwarm::GenerateEvent(Event::BlockRetrieved { cid }))
                }
            }
        }

        self.waker = Some(ctx.waker().clone());

        Poll::Pending
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
