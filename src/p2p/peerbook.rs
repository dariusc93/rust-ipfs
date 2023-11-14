/// PeerBook with connection limits based on https://github.com/libp2p/rust-libp2p/pull/3386
use core::task::{Context, Poll};
use futures::channel::oneshot;
use futures::StreamExt;
use libp2p::core::{Endpoint, Multiaddr};
use libp2p::identify::Info;
use libp2p::swarm::derive_prelude::ConnectionEstablished;
use libp2p::swarm::dial_opts::DialOpts;
use libp2p::swarm::{
    self, dummy::ConnectionHandler as DummyConnectionHandler, CloseConnection, NetworkBehaviour,
};
#[allow(deprecated)]
use libp2p::swarm::{
    ConnectionClosed, ConnectionDenied, ConnectionId, DialFailure, FromSwarm, THandler,
    THandlerInEvent, ToSwarm,
};
use libp2p::PeerId;
use std::collections::hash_map::Entry;
use std::time::Duration;
use tracing::log;
use wasm_timer::Interval;

use std::collections::{HashMap, VecDeque};

#[derive(Debug)]
#[allow(clippy::type_complexity)]
pub struct Behaviour {
    events: VecDeque<ToSwarm<<Self as NetworkBehaviour>::ToSwarm, THandlerInEvent<Self>>>,
    cleanup_interval: Interval,

    pending_connections: HashMap<ConnectionId, oneshot::Sender<anyhow::Result<()>>>,
    pending_disconnection: HashMap<PeerId, oneshot::Sender<anyhow::Result<()>>>,

    pending_identify_timer: HashMap<PeerId, Interval>,

    pending_identify: HashMap<PeerId, oneshot::Sender<anyhow::Result<()>>>,

    peer_info: HashMap<PeerId, Info>,
    peer_rtt: HashMap<PeerId, [Duration; 3]>,
    peer_connections: HashMap<PeerId, Vec<(ConnectionId, Multiaddr)>>,

    config: Config,
}

impl Default for Behaviour {
    fn default() -> Self {
        Self {
            events: Default::default(),
            cleanup_interval: Interval::new_at(
                std::time::Instant::now() + Duration::from_secs(60),
                Duration::from_secs(60),
            ),
            pending_connections: Default::default(),
            pending_disconnection: Default::default(),
            pending_identify_timer: Default::default(),
            pending_identify: Default::default(),
            peer_info: Default::default(),
            peer_rtt: Default::default(),
            peer_connections: Default::default(),
            config: Config::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    pub wait_on_identify: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            wait_on_identify: true,
        }
    }
}

impl Behaviour {
    pub fn new(config: Config) -> Self {
        Self {
            config,
            ..Default::default()
        }
    }

    pub fn connect(&mut self, opt: impl Into<DialOpts>) -> oneshot::Receiver<anyhow::Result<()>> {
        let opts: DialOpts = opt.into();
        let (tx, rx) = oneshot::channel();
        let id = opts.connection_id();
        self.events.push_back(ToSwarm::Dial { opts });
        self.pending_connections.insert(id, tx);
        rx
    }

    pub fn disconnect(&mut self, peer_id: PeerId) -> oneshot::Receiver<anyhow::Result<()>> {
        let (tx, rx) = oneshot::channel();

        if !self.peer_connections.contains_key(&peer_id) {
            let _ = tx.send(Err(anyhow::anyhow!("Peer is not connected")));
            return rx;
        }

        if self.pending_disconnection.contains_key(&peer_id) {
            let _ = tx.send(Err(anyhow::anyhow!("Disconnection is pending")));
            return rx;
        }
        self.events.push_back(ToSwarm::CloseConnection {
            peer_id,
            connection: CloseConnection::All,
        });

        self.pending_disconnection.insert(peer_id, tx);

        rx
    }

    pub fn inject_peer_info(&mut self, info: Info) {
        let peer_id = info.public_key.to_peer_id();
        self.peer_info.insert(peer_id, info);
        self.pending_identify_timer.remove(&peer_id);
        if self.config.wait_on_identify {
            if let Some(ch) = self.pending_identify.remove(&peer_id) {
                let _ = ch.send(Ok(()));
            }
        }
    }

    pub fn peers(&self) -> impl Iterator<Item = &PeerId> {
        self.peer_connections.keys()
    }

    pub fn connected_peers_addrs(&self) -> impl Iterator<Item = (PeerId, Vec<Multiaddr>)> + '_ {
        self.peer_connections.iter().map(|(peer_id, list)| {
            let list = list
                .iter()
                .map(|(_, addr)| addr)
                .cloned()
                .collect::<Vec<_>>();
            (*peer_id, list)
        })
    }

    pub fn set_peer_rtt(&mut self, peer_id: PeerId, rtt: Duration) {
        self.peer_rtt
            .entry(peer_id)
            .and_modify(|r| {
                r.rotate_left(1);
                r[2] = rtt;
            })
            .or_insert([Duration::from_millis(0), Duration::from_millis(0), rtt]);
    }

    pub fn get_peer_rtt(&self, peer_id: PeerId) -> Option<[Duration; 3]> {
        self.peer_rtt.get(&peer_id).copied()
    }

    pub fn get_peer_latest_rtt(&self, peer_id: PeerId) -> Option<Duration> {
        self.get_peer_rtt(peer_id).map(|rtt| rtt[2])
    }

    pub fn get_peer_info(&self, peer_id: PeerId) -> Option<&Info> {
        self.peer_info.get(&peer_id)
    }

    pub fn remove_peer_info(&mut self, peer_id: PeerId) {
        self.peer_info.remove(&peer_id);
    }

    pub fn peer_connections(&self, peer_id: PeerId) -> Option<Vec<Multiaddr>> {
        self.peer_connections
            .get(&peer_id)
            .map(|list| list.iter().map(|(_, addr)| addr).cloned().collect())
    }
}

impl NetworkBehaviour for Behaviour {
    type ConnectionHandler = DummyConnectionHandler;
    type ToSwarm = void::Void;

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
        Ok(DummyConnectionHandler)
    }

    fn handle_established_outbound_connection(
        &mut self,
        _: ConnectionId,
        _: PeerId,
        _: &Multiaddr,
        _: Endpoint,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        Ok(DummyConnectionHandler)
    }

    fn on_connection_handler_event(
        &mut self,
        _: libp2p::PeerId,
        _: swarm::ConnectionId,
        _: swarm::THandlerOutEvent<Self>,
    ) {
    }

    #[allow(clippy::single_match)]
    fn on_swarm_event(&mut self, event: FromSwarm) {
        match event {
            FromSwarm::ConnectionEstablished(ConnectionEstablished {
                peer_id,
                connection_id,
                endpoint,
                ..
            }) => {
                if let Some(ch) = self.pending_connections.remove(&connection_id) {
                    match (
                        self.get_peer_info(peer_id).is_none(),
                        self.config.wait_on_identify,
                    ) {
                        (true, true) => {
                            self.pending_identify.insert(peer_id, ch);
                            self.pending_identify_timer.insert(
                                peer_id,
                                Interval::new_at(
                                    std::time::Instant::now() + Duration::from_secs(5),
                                    Duration::from_secs(5),
                                ),
                            );
                        }
                        _ => {
                            let _ = ch.send(Ok(()));
                        }
                    }
                }
                let multiaddr = endpoint.get_remote_address().clone();

                self.peer_connections
                    .entry(peer_id)
                    .or_default()
                    .push((connection_id, multiaddr));
            }
            FromSwarm::DialFailure(DialFailure {
                error,
                connection_id,
                ..
            }) => {
                if let Some(ch) = self.pending_connections.remove(&connection_id) {
                    let _ = ch.send(Err(anyhow::anyhow!("{error}")));
                }
            }
            FromSwarm::ConnectionClosed(ConnectionClosed {
                peer_id,
                connection_id,
                ..
            }) => {
                self.peer_rtt.remove(&peer_id);

                if let Entry::Occupied(mut entry) = self.peer_connections.entry(peer_id) {
                    let list = entry.get_mut();
                    if let Some(index) = list.iter().position(|(id, _)| connection_id.eq(id)) {
                        list.swap_remove(index);
                    }
                    if list.is_empty() {
                        entry.remove();
                    }
                }

                //Note: This is in case we receive a connection close before it was ever established
                if let Some(ch) = self.pending_connections.remove(&connection_id) {
                    let _ = ch.send(Ok(()));
                }
                if let Some(ch) = self.pending_disconnection.remove(&peer_id) {
                    let _ = ch.send(Ok(()));
                }
            }

            _ => {}
        }
    }

    fn poll(&mut self, cx: &mut Context) -> Poll<ToSwarm<Self::ToSwarm, THandlerInEvent<Self>>> {
        if let Some(event) = self.events.pop_front() {
            return Poll::Ready(event);
        }

        self.pending_identify_timer
            .retain(|peer_id, timer| match timer.poll_next_unpin(cx) {
                Poll::Ready(Some(_)) => {
                    if let Some(ch) = self.pending_identify.remove(peer_id) {
                        let _ = ch.send(Ok(()));
                    }
                    false
                }
                Poll::Ready(None) => {
                    log::error!("timer for {} was not available", peer_id);
                    false
                }
                Poll::Pending => true,
            });

        Poll::Pending
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use super::Behaviour as PeerBook;
    use futures::StreamExt;
    use libp2p::{
        identify::{self, Config},
        swarm::{behaviour::toggle::Toggle, NetworkBehaviour, SwarmEvent},
        Multiaddr, PeerId, Swarm, SwarmBuilder,
    };

    #[derive(NetworkBehaviour)]
    struct Behaviour {
        peerbook: PeerBook,
        identify: Toggle<identify::Behaviour>,
    }

    #[tokio::test]
    async fn connect_without_identify() {
        let (_, addr1, mut swarm1) = build_swarm(false).await;
        let (peer2, _, mut swarm2) = build_swarm(false).await;

        let mut oneshot = swarm2.behaviour_mut().peerbook.connect(addr1.clone());

        loop {
            tokio::select! {
                biased;
                _ = swarm1.next() => {},
                _ = swarm2.next() => {},
                conn_res = (&mut oneshot) => {
                    conn_res.unwrap().unwrap();
                    break;
                }
            }
        }

        let list = swarm1.connected_peers().copied().collect::<Vec<_>>();

        assert!(list.contains(&peer2));
    }

    #[tokio::test]
    async fn disconnect() {
        let (peer1, addr1, mut swarm1) = build_swarm(false).await;
        let (_, _, mut swarm2) = build_swarm(false).await;

        let mut oneshot = swarm2.behaviour_mut().peerbook.connect(addr1.clone());

        loop {
            tokio::select! {
                biased;
                _ = swarm1.next() => {},
                _ = swarm2.next() => {},
                conn_res = (&mut oneshot) => {
                    conn_res.unwrap().unwrap();
                    break;
                }
            }
        }

        let list = swarm2.connected_peers().copied().collect::<Vec<_>>();
        assert!(list.contains(&peer1));

        let oneshot = swarm2.behaviour_mut().peerbook.disconnect(peer1);

        let mut p1_disconnect = false;
        let mut p2_disconnect = false;

        loop {
            tokio::select! {
                biased;
                e1 = swarm1.select_next_some() => {
                    if matches!(e1, SwarmEvent::ConnectionClosed { .. }) {
                        p2_disconnect = true;
                    }
                },
                e2 = swarm2.select_next_some() => {
                    if matches!(e2, SwarmEvent::ConnectionClosed { .. }) {
                        p1_disconnect = true;
                    }
                },
            }
            if p1_disconnect && p2_disconnect {
                break;
            }
        }

        oneshot.await.unwrap().unwrap();

        let list = swarm2.connected_peers().copied().collect::<Vec<_>>();
        assert!(list.is_empty());
    }

    #[tokio::test]
    async fn cannot_disconnect() {
        let (peer1, _, mut swarm1) = build_swarm(false).await;
        let (_, _, mut swarm2) = build_swarm(false).await;

        let mut oneshot = swarm2.behaviour_mut().peerbook.disconnect(peer1);

        loop {
            tokio::select! {
                biased;
                e1 = swarm1.select_next_some() => {
                    if matches!(e1, SwarmEvent::ConnectionClosed { .. }) {
                        panic!("Cannot disconnect if not connected")
                    }
                },
                e2 = swarm2.select_next_some() => {
                    if matches!(e2, SwarmEvent::ConnectionClosed { .. }) {
                        panic!("Cannot disconnect if not connected")
                    }
                },
                res = &mut oneshot => {
                    let result = res.unwrap();
                    assert!(result.is_err());
                    break;
                }
            }
        }
    }

    #[tokio::test]
    async fn connect_with_identify() {
        let (_, addr1, mut swarm1) = build_swarm(true).await;
        let (peer2, _, mut swarm2) = build_swarm(true).await;

        let mut oneshot = swarm2.behaviour_mut().peerbook.connect(addr1.clone());
        let mut peer_1_identify = false;
        let mut peer_2_identify = false;
        loop {
            tokio::select! {
                biased;
                Some(e) = swarm1.next() => {
                    if let SwarmEvent::Behaviour(BehaviourEvent::Identify(identify::Event::Received { .. })) = e {
                        peer_2_identify = true;
                    }
                },
                Some(e) = swarm2.next() => {
                    if let SwarmEvent::Behaviour(BehaviourEvent::Identify(identify::Event::Received { .. })) = e {
                        peer_1_identify = true;
                    }
                },
                conn_res = (&mut oneshot) => {
                    conn_res.unwrap().unwrap();
                }
            }

            if peer_1_identify && peer_2_identify {
                break;
            }
        }

        let list = swarm1.connected_peers().copied().collect::<Vec<_>>();

        assert!(list.contains(&peer2));
    }

    async fn build_swarm(identify: bool) -> (PeerId, Multiaddr, Swarm<Behaviour>) {
        let mut swarm = SwarmBuilder::with_new_identity()
            .with_tokio()
            .with_tcp(
                libp2p::tcp::Config::default(),
                libp2p::noise::Config::new,
                libp2p::yamux::Config::default,
            )
            .expect("")
            .with_behaviour(|kp| Behaviour {
                peerbook: PeerBook::default(),
                identify: Toggle::from(identify.then_some(identify::Behaviour::new(Config::new(
                    "/peerbook/0.1".into(),
                    kp.public(),
                )))),
            })
            .expect("")
            .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(30)))
            .build();

        Swarm::listen_on(&mut swarm, "/ip4/127.0.0.1/tcp/0".parse().unwrap()).unwrap();

        if let Some(SwarmEvent::NewListenAddr { address, .. }) = swarm.next().await {
            let peer_id = swarm.local_peer_id();
            return (*peer_id, address, swarm);
        }

        panic!("no new addrs")
    }
}
