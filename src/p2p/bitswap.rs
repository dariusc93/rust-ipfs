mod handler;
mod message;
mod pb;
mod prefix;
mod session;
mod wantlist;

use std::{
    collections::{hash_map::Entry, HashMap, HashSet, VecDeque},
    fmt::Debug,
    task::{Context, Poll, Waker},
    time::Duration,
};

use connexa::prelude::{
    swarm::{
        behaviour::ConnectionEstablished, dial_opts::DialOpts, ConnectionClosed, ConnectionDenied,
        ConnectionId, FromSwarm, NetworkBehaviour, NotifyHandler, THandler, THandlerInEvent,
        THandlerOutEvent, ToSwarm,
    },
    transport::transport::PortUse,
    transport::Endpoint,
    Multiaddr, PeerId,
};
use futures::StreamExt;
use ipld_core::cid::Cid;

use pollable_map::stream::StreamMap;

mod bitswap_pb {
    pub use super::pb::bitswap_pb::Message;
    pub mod message {
        use super::super::pb::bitswap_pb::mod_Message as message;
        pub use message::mod_Wantlist as wantlist;
        pub use message::Wantlist;
        pub use message::{Block, BlockPresence, BlockPresenceType};
    }
}

use self::{
    message::RequestType,
    session::{PeerSession, PeerSessionEvent},
    wantlist::{Wantlist, WantlistDriver, WantlistEvent},
};
use crate::repo::DefaultStorage;
use crate::repo::Repo;

const CAP_THRESHOLD: usize = 100;
const DEFAULT_PRIORITY: i32 = 1;

#[derive(Debug)]
pub enum Event {
    NeedBlock { cid: Cid },
    BlockRetrieved { cid: Cid },
    CancelBlock { cid: Cid },
}

pub struct Behaviour {
    events: VecDeque<ToSwarm<Event, THandlerInEvent<Self>>>,
    connections: HashMap<PeerId, HashSet<ConnectionId>>,
    unsupported: HashSet<PeerId>,
    store: Repo<DefaultStorage>,
    wantlist: Wantlist,
    driver: WantlistDriver,
    sessions: StreamMap<PeerId, PeerSession>,
    waker: Option<Waker>,
}

impl Behaviour {
    pub fn new(store: &Repo<DefaultStorage>) -> Self {
        let wantlist = Wantlist::default();
        Self {
            events: VecDeque::new(),
            connections: HashMap::new(),
            unsupported: HashSet::new(),
            store: store.clone(),
            driver: WantlistDriver::new(wantlist.clone()),
            wantlist,
            sessions: StreamMap::new(),
            waker: None,
        }
    }

    pub fn get(&mut self, cid: &Cid, providers: &[PeerId], timeout: Option<Duration>) {
        self.gets(vec![*cid], providers, timeout)
    }

    pub fn gets(&mut self, cids: Vec<Cid>, providers: &[PeerId], timeout: Option<Duration>) {
        for cid in &cids {
            self.wantlist
                .want(*cid, RequestType::Have, DEFAULT_PRIORITY, timeout);
        }

        // A target is any peer we can broadcast to right now. Explicit providers that are not yet
        // connected are dialed but do not count until the connection is up.
        let mut have_target = false;
        if providers.is_empty() {
            have_target = self
                .connections
                .keys()
                .any(|peer| !self.unsupported.contains(peer));
        } else {
            for peer in providers {
                if self.unsupported.contains(peer) {
                    continue;
                }
                if self.connections.contains_key(peer) {
                    have_target = true;
                } else {
                    self.events.push_back(ToSwarm::Dial {
                        opts: DialOpts::peer_id(*peer).build(),
                    });
                }
            }
        }

        for session in self.sessions.values_mut() {
            session.sync();
        }

        if !have_target {
            for cid in cids {
                self.events
                    .push_back(ToSwarm::GenerateEvent(Event::NeedBlock { cid }));
            }
        }

        self.wake();
    }

    pub fn local_wantlist(&self) -> Vec<Cid> {
        self.wantlist.cids()
    }

    pub fn peer_wantlist(&self, peer_id: PeerId) -> Vec<Cid> {
        self.sessions
            .get(&peer_id)
            .map(|session| session.peer_wantlist())
            .unwrap_or_default()
    }

    // Note: This is called specifically to cancel the request and not just emitting a request
    //       after receiving a request.
    pub fn cancel(&mut self, cid: Cid) {
        if !self.wantlist.cancel(&cid) {
            return;
        }

        for session in self.sessions.values_mut() {
            session.sync();
        }

        self.events
            .push_back(ToSwarm::GenerateEvent(Event::CancelBlock { cid }));
        self.wake();
    }

    // Notify connected peers that asked for these blocks that we now have them.
    pub fn notify_new_blocks(&mut self, cid: impl IntoIterator<Item = Cid>) {
        let blocks = cid.into_iter().collect::<Vec<_>>();
        if blocks.is_empty() {
            return;
        }

        for session in self.sessions.values_mut() {
            session.serve_wanted(&blocks);
        }

        self.wake();
    }

    fn wake(&mut self) {
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }

    fn ensure_session(&mut self, peer_id: PeerId) {
        if self.unsupported.contains(&peer_id) || self.sessions.contains_key(&peer_id) {
            return;
        }
        let session = PeerSession::new(self.wantlist.clone(), self.store.clone());
        self.sessions.insert(peer_id, session);
        self.wake();
    }

    fn on_connection_established(
        &mut self,
        ConnectionEstablished {
            connection_id,
            peer_id,
            ..
        }: ConnectionEstablished,
    ) {
        self.connections
            .entry(peer_id)
            .or_default()
            .insert(connection_id);
    }

    fn on_connection_close(
        &mut self,
        ConnectionClosed {
            connection_id,
            peer_id,
            remaining_established,
            ..
        }: ConnectionClosed,
    ) {
        if let Entry::Occupied(mut entry) = self.connections.entry(peer_id) {
            let list = entry.get_mut();
            list.remove(&connection_id);
            if list.is_empty() {
                entry.remove();
            }
        }

        if remaining_established == 0 {
            self.sessions.remove(&peer_id);
            self.unsupported.remove(&peer_id);
        }
    }
}

impl NetworkBehaviour for Behaviour {
    type ConnectionHandler = handler::Handler;
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
        Ok(handler::Handler::default())
    }

    fn handle_established_outbound_connection(
        &mut self,
        _: ConnectionId,
        _: PeerId,
        _: &Multiaddr,
        _: Endpoint,
        _: PortUse,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        Ok(handler::Handler::default())
    }

    fn on_connection_handler_event(
        &mut self,
        peer_id: PeerId,
        _connection_id: ConnectionId,
        event: THandlerOutEvent<Self>,
    ) {
        match event {
            handler::ToBehaviour::Ready => self.ensure_session(peer_id),
            handler::ToBehaviour::Message(message) => {
                self.ensure_session(peer_id);
                if let Some(session) = self.sessions.get_mut(&peer_id) {
                    session.on_message(message);
                }
                self.wake();
            }
            handler::ToBehaviour::Unsupported => {
                self.unsupported.insert(peer_id);
                self.sessions.remove(&peer_id);
            }
            handler::ToBehaviour::Failed => {
                self.sessions.remove(&peer_id);
            }
        }
    }

    fn on_swarm_event(&mut self, event: FromSwarm) {
        match event {
            FromSwarm::ConnectionEstablished(event) => self.on_connection_established(event),
            FromSwarm::ConnectionClosed(event) => self.on_connection_close(event),
            _ => {}
        }
    }

    fn poll(&mut self, ctx: &mut Context) -> Poll<ToSwarm<Self::ToSwarm, THandlerInEvent<Self>>> {
        if let Some(event) = self.events.pop_front() {
            return Poll::Ready(event);
        } else if self.events.capacity() > CAP_THRESHOLD {
            self.events.shrink_to_fit();
        }

        while let Poll::Ready(Some(event)) = self.driver.poll_next_unpin(ctx) {
            match event {
                WantlistEvent::NeedProviders(cid) => {
                    return Poll::Ready(ToSwarm::GenerateEvent(Event::NeedBlock { cid }));
                }
                WantlistEvent::Expired(cid) => {
                    for session in self.sessions.values_mut() {
                        session.sync();
                    }
                    return Poll::Ready(ToSwarm::GenerateEvent(Event::CancelBlock { cid }));
                }
                WantlistEvent::Rebroadcast => {
                    for session in self.sessions.values_mut() {
                        session.sync();
                    }
                }
            }
        }

        while let Poll::Ready(Some((peer_id, event))) = self.sessions.poll_next_unpin(ctx) {
            match event {
                PeerSessionEvent::Send(message) => {
                    return Poll::Ready(ToSwarm::NotifyHandler {
                        peer_id,
                        handler: NotifyHandler::Any,
                        event: handler::FromBehaviour::Send(message),
                    });
                }
                PeerSessionEvent::Stored(cid) => {
                    self.wantlist.cancel(&cid);
                    for session in self.sessions.values_mut() {
                        session.sync();
                    }
                    return Poll::Ready(ToSwarm::GenerateEvent(Event::BlockRetrieved { cid }));
                }
                PeerSessionEvent::DontHave(_cid) => {}
            }
        }

        self.waker = Some(ctx.waker().clone());

        Poll::Pending
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use crate::{block::BlockCodec, repo::DefaultStorage};
    use connexa::prelude::{
        swarm::{dial_opts::DialOpts, NetworkBehaviour, Swarm, SwarmBuilder, SwarmEvent},
        transport::{
            noise,
            transport::{MemoryTransport, Transport},
            upgrade::Version,
            yamux,
        },
        Multiaddr, PeerId,
    };
    use futures::StreamExt;
    use ipld_core::cid::Cid;
    use multihash_codetable::{Code, MultihashDigest};

    use crate::{repo::Repo, Block};

    fn create_block() -> Block {
        let data = b"hello block\n".to_vec();
        let cid = Cid::new_v1(BlockCodec::Raw.into(), Code::Sha2_256.digest(&data));

        Block::new_unchecked(cid, data)
    }

    async fn wait_on_connection(
        swarm1: &mut Swarm<Behaviour>,
        swarm2: &mut Swarm<Behaviour>,
        peer_id: PeerId,
    ) {
        loop {
            futures::select! {
                event = swarm1.select_next_some() => {
                    if let SwarmEvent::ConnectionEstablished { peer_id: peer, .. } = event {
                        assert_eq!(peer, peer_id);
                        break;
                    }
                }
                _ = swarm2.next() => {}
            }
        }
    }

    #[tokio::test]
    async fn exchange_blocks() -> anyhow::Result<()> {
        let (_, _, mut swarm1, repo) = build_swarm().await;
        let (peer2, addr2, mut swarm2, repo2) = build_swarm().await;

        let block = create_block();

        let cid = *block.cid();

        repo.put_block(&block).await?;

        let opt = DialOpts::peer_id(peer2)
            .addresses(vec![addr2.clone()])
            .build();

        swarm1.dial(opt)?;

        wait_on_connection(&mut swarm1, &mut swarm2, peer2).await;

        swarm2.behaviour_mut().bitswap.get(&cid, &[], None);

        loop {
            tokio::select! {
                _ = swarm1.next() => {}
                e = swarm2.select_next_some() => {
                    match e {
                        SwarmEvent::Behaviour(BehaviourEvent::Bitswap(super::Event::BlockRetrieved { cid: inner_cid })) => {
                            assert_eq!(inner_cid, cid);
                        }
                        SwarmEvent::Behaviour(BehaviourEvent::Bitswap(super::Event::CancelBlock { cid: inner_cid })) => {
                            assert_eq!(inner_cid, cid);
                            unreachable!("exchange should not timeout");
                        }
                        _ => {}
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
    async fn notify_swarm() -> anyhow::Result<()> {
        let (_, _, mut swarm1, _) = build_swarm().await;

        let block = create_block();

        let cid = *block.cid();

        swarm1
            .behaviour_mut()
            .bitswap
            .get(&cid, &[], Some(Duration::from_millis(500)));

        let mut notified_counter = 0;

        loop {
            tokio::select! {
                e = swarm1.select_next_some() => {
                    match e {
                        SwarmEvent::Behaviour(BehaviourEvent::Bitswap(super::Event::NeedBlock { cid: inner_cid })) => {
                            assert_eq!(inner_cid, cid);
                            notified_counter += 1;
                        }
                        SwarmEvent::Behaviour(BehaviourEvent::Bitswap(super::Event::CancelBlock { cid: inner_cid })) => {
                            assert_eq!(inner_cid, cid);
                            unreachable!()
                        }
                        _ => {}
                    }
                },
            }

            if notified_counter == 2 {
                break;
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn bitswap_timeout() -> anyhow::Result<()> {
        let (_, _, mut swarm1, _) = build_swarm().await;
        let (peer2, addr2, mut swarm2, _) = build_swarm().await;

        let block = create_block();

        let cid = *block.cid();

        let opt = DialOpts::peer_id(peer2)
            .addresses(vec![addr2.clone()])
            .build();

        swarm1.dial(opt)?;

        wait_on_connection(&mut swarm1, &mut swarm2, peer2).await;

        swarm2
            .behaviour_mut()
            .bitswap
            .get(&cid, &[], Some(Duration::from_millis(150)));

        loop {
            tokio::select! {
                _ = swarm1.next() => {}
                e = swarm2.select_next_some() => {
                    if let SwarmEvent::Behaviour(BehaviourEvent::Bitswap(super::Event::CancelBlock { cid: inner_cid })) = e {
                        assert_eq!(inner_cid, cid);
                        break;
                    }
                },
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn exchange_blocks_with_explicit_peer() -> anyhow::Result<()> {
        let (peer1, _, mut swarm1, repo) = build_swarm().await;
        let (peer2, addr2, mut swarm2, repo2) = build_swarm().await;

        let block = create_block();

        let cid = *block.cid();

        repo.put_block(&block).await?;

        let opt = DialOpts::peer_id(peer2)
            .addresses(vec![addr2.clone()])
            .build();

        swarm1.dial(opt)?;

        wait_on_connection(&mut swarm1, &mut swarm2, peer2).await;

        swarm2.behaviour_mut().bitswap.get(&cid, &[peer1], None);

        loop {
            tokio::select! {
                _ = swarm1.next() => {}
                e = swarm2.select_next_some() => {
                    if let SwarmEvent::Behaviour(BehaviourEvent::Bitswap(super::Event::BlockRetrieved { cid: inner_cid })) = e {
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

        repo.put_block(&block).await?;

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
        swarm2.behaviour_mut().bitswap.get(&cid, &[peer1], None);
        swarm3.behaviour_mut().bitswap.get(&cid, &[], None);

        loop {
            tokio::select! {
                _ = swarm1.next() => {}
                e = swarm2.select_next_some() => {
                    if let SwarmEvent::Behaviour(BehaviourEvent::Bitswap(super::Event::BlockRetrieved { cid: inner_cid })) = e {
                        assert_eq!(inner_cid, cid);
                        swarm2.behaviour_mut().bitswap.notify_new_blocks(std::iter::once(cid));
                    }
                },
                e = swarm3.select_next_some() => {
                    if let SwarmEvent::Behaviour(BehaviourEvent::Bitswap(super::Event::BlockRetrieved { cid: inner_cid })) = e {
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

        swarm1.behaviour_mut().bitswap.get(&cid, &[], None);
        swarm1.behaviour_mut().bitswap.cancel(cid);

        loop {
            tokio::select! {
                e = swarm1.select_next_some() => {
                    if let SwarmEvent::Behaviour(BehaviourEvent::Bitswap(super::Event::CancelBlock { cid: inner_cid })) = e {
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

        swarm1.behaviour_mut().bitswap.get(&cid, &[], None);

        let list = swarm1.behaviour().bitswap.local_wantlist();

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
        swarm2.behaviour_mut().bitswap.get(&cid, &[peer1], None);

        loop {
            tokio::select! {
                _ = swarm1.next() => {}
                e = swarm2.select_next_some() => {
                    if let SwarmEvent::Behaviour(BehaviourEvent::Bitswap(super::Event::NeedBlock { cid: inner_cid })) = e {
                        assert_eq!(inner_cid, cid);
                        break;
                    }
                },
            }
        }

        let list = swarm1.behaviour().bitswap.peer_wantlist(peer2);
        assert_eq!(list[0], cid);

        Ok(())
    }

    async fn build_swarm() -> (PeerId, Multiaddr, Swarm<Behaviour>, Repo<DefaultStorage>) {
        let repo = Repo::new_memory();

        let mut swarm = SwarmBuilder::with_new_identity()
            .with_tokio()
            .with_other_transport(|kp| {
                MemoryTransport::default()
                    .upgrade(Version::V1)
                    .authenticate(noise::Config::new(kp).expect("valid config"))
                    .multiplex(yamux::Config::default())
                    .timeout(Duration::from_secs(20))
                    .boxed()
            })
            .expect("")
            .with_behaviour(|_| Behaviour {
                bitswap: super::Behaviour::new(&repo),
                address_book: crate::p2p::addressbook::Behaviour::with_config(
                    crate::p2p::addressbook::Config {
                        store_on_connection: true,
                        ..Default::default()
                    },
                ),
            })
            .expect("")
            .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(30)))
            .build();

        Swarm::listen_on(&mut swarm, "/memory/0".parse().unwrap()).unwrap();

        if let Some(SwarmEvent::NewListenAddr { address, .. }) = swarm.next().await {
            let peer_id = swarm.local_peer_id();
            return (*peer_id, address, swarm, repo);
        }

        unreachable!()
    }

    #[derive(NetworkBehaviour)]
    #[behaviour(prelude = "connexa::prelude::swarm::derive_prelude")]
    struct Behaviour {
        bitswap: super::Behaviour,
        address_book: crate::p2p::addressbook::Behaviour,
    }
}
