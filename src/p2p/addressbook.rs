mod handler;

use crate::AddPeerOpt;
use futures::StreamExt;
use futures_timer::Delay;
use libp2p::core::transport::PortUse;
use libp2p::swarm::dial_opts::DialOpts;
use libp2p::swarm::{ConnectionClosed, DialError, DialFailure};
use libp2p::{
    core::{ConnectedPoint, Endpoint},
    multiaddr::Protocol,
    swarm::{
        self, behaviour::ConnectionEstablished, AddressChange, ConnectionDenied, ConnectionId,
        FromSwarm, NetworkBehaviour, THandler, THandlerInEvent, ToSwarm,
    },
    Multiaddr, PeerId,
};
use pollable_map::futures::FutureMap;
use std::fmt::Debug;
use std::time::Duration;
use std::{
    collections::{hash_map::Entry, HashMap, HashSet, VecDeque},
    task::{Context, Poll},
};

#[derive(Default, Debug, Copy, Clone)]
pub struct Config {
    /// Store peer address on an established connection
    pub store_on_connection: bool,
    /// Keep connection alive automatically if peer is added through `Behaviour::add_address`
    pub keep_connection_alive: bool,
}

#[derive(Default)]
pub struct Behaviour {
    events: VecDeque<ToSwarm<<Self as NetworkBehaviour>::ToSwarm, THandlerInEvent<Self>>>,
    connections: HashMap<PeerId, HashSet<ConnectionId>>,
    peer_addresses: HashMap<PeerId, HashSet<Multiaddr>>,
    peer_keepalive: HashSet<PeerId>,
    can_reconnect: HashMap<PeerId, (Duration, u8)>,
    peer_reconnect_attempts: HashMap<PeerId, u8>,
    reconnect_peers: FutureMap<PeerId, Delay>,
    config: Config,
}

impl Debug for Behaviour {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Behaviour")
            .field("connections", &self.connections)
            .field("peer_addresses", &self.peer_addresses)
            .field("peer_keepalive", &self.peer_keepalive)
            .field("can_reconnect", &self.can_reconnect)
            .field("config", &self.config)
            .finish()
    }
}

impl Behaviour {
    pub fn with_config(config: Config) -> Self {
        Self {
            config,
            ..Default::default()
        }
    }
    pub fn add_address<I: Into<AddPeerOpt>>(&mut self, opt: I) -> bool {
        let opt = opt.into();

        let peer_id = opt.peer_id();
        let addresses = opt.addresses();

        if !addresses.is_empty() {
            let addrs = self.peer_addresses.entry(*peer_id).or_default();

            for addr in addresses {
                addrs.insert(addr.clone());
            }

            if let Some(opts) = opt.to_dial_opts() {
                self.events.push_back(ToSwarm::Dial { opts });
            }
        }

        if (opt.can_keep_alive() || self.config.keep_connection_alive)
            && self.peer_addresses.contains_key(peer_id)
        {
            self.keep_peer_alive(peer_id);
        }

        if let Some(opt) = opt.reconnect_opt() {
            self.can_reconnect.insert(*peer_id, opt);
        }

        true
    }

    pub fn remove_address(&mut self, peer_id: &PeerId, addr: &Multiaddr) -> bool {
        if let Entry::Occupied(mut e) = self.peer_addresses.entry(*peer_id) {
            let entry = e.get_mut();

            if !entry.remove(addr) {
                return false;
            }

            if entry.is_empty() {
                e.remove();
                self.dont_keep_peer_alive(peer_id);
            }
        }
        true
    }

    pub fn remove_peer(&mut self, peer_id: &PeerId) -> bool {
        let removed = self.peer_addresses.remove(peer_id).is_some();
        if removed {
            self.dont_keep_peer_alive(peer_id);
        }
        removed
    }

    pub fn contains(&self, peer_id: &PeerId, addr: &Multiaddr) -> bool {
        self.peer_addresses
            .get(peer_id)
            .map(|list| list.contains(addr))
            .unwrap_or_default()
    }

    pub fn get_peer_addresses(&self, peer_id: &PeerId) -> Option<Vec<Multiaddr>> {
        self.peer_addresses
            .get(peer_id)
            .cloned()
            .map(Vec::from_iter)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&PeerId, &HashSet<Multiaddr>)> {
        self.peer_addresses.iter()
    }

    fn keep_peer_alive(&mut self, peer_id: &PeerId) {
        self.peer_keepalive.insert(*peer_id);
        if let Some(conns) = self.connections.get(peer_id) {
            self.events.extend(
                conns
                    .iter()
                    .copied()
                    .map(|connection_id| ToSwarm::NotifyHandler {
                        peer_id: *peer_id,
                        handler: swarm::NotifyHandler::One(connection_id),
                        event: handler::In::Protect,
                    }),
            )
        }
    }

    fn dont_keep_peer_alive(&mut self, peer_id: &PeerId) {
        self.peer_keepalive.remove(peer_id);
        if let Some(conns) = self.connections.get(peer_id) {
            self.events.extend(
                conns
                    .iter()
                    .copied()
                    .map(|connection_id| ToSwarm::NotifyHandler {
                        peer_id: *peer_id,
                        handler: swarm::NotifyHandler::One(connection_id),
                        event: handler::In::Unprotect,
                    }),
            )
        }
    }

    fn on_connection_established(
        &mut self,
        ConnectionEstablished {
            peer_id,
            connection_id,
            endpoint,
            ..
        }: ConnectionEstablished,
    ) {
        self.connections
            .entry(peer_id)
            .or_default()
            .insert(connection_id);

        self.reconnect_peers.remove(&peer_id);

        if !self.config.store_on_connection {
            return;
        }

        let mut addr = address_from_connection_point(endpoint);

        if matches!(addr.iter().last(), Some(Protocol::P2p(_))) {
            addr.pop();
        }

        self.peer_addresses.entry(peer_id).or_default().insert(addr);
    }

    fn on_connection_closed(
        &mut self,
        ConnectionClosed {
            peer_id,
            connection_id,
            remaining_established,
            ..
        }: ConnectionClosed,
    ) {
        if let Entry::Occupied(mut entry) = self.connections.entry(peer_id) {
            let list = entry.get_mut();
            list.remove(&connection_id);
            if list.is_empty() && remaining_established == 0 {
                entry.remove();
                if let Some((duration, attempts)) = self.can_reconnect.remove(&peer_id) {
                    self.reconnect_peers.insert(peer_id, Delay::new(duration));
                    self.peer_reconnect_attempts.insert(peer_id, attempts);
                }
            }
        }
    }

    fn on_address_change(
        &mut self,
        AddressChange {
            peer_id, old, new, ..
        }: AddressChange,
    ) {
        let mut old = address_from_connection_point(old);

        if matches!(old.iter().last(), Some(Protocol::P2p(_))) {
            old.pop();
        }

        let mut new = address_from_connection_point(new);

        if matches!(new.iter().last(), Some(Protocol::P2p(_))) {
            new.pop();
        }

        if let Entry::Occupied(mut e) = self.peer_addresses.entry(peer_id) {
            let entry = e.get_mut();
            entry.insert(new);
            entry.remove(&old);
        }
    }

    fn on_dial_failure(
        &mut self,
        DialFailure {
            peer_id,
            error,
            connection_id,
        }: DialFailure,
    ) {
        let Some(peer_id) = peer_id else {
            return;
        };

        match error {
            DialError::LocalPeerId { .. } => {
                tracing::error!(%peer_id, %connection_id, "local peer id is not allowed to dial");
                self.reconnect_peers.remove(&peer_id);
                self.peer_reconnect_attempts.remove(&peer_id);
                self.peer_keepalive.remove(&peer_id);
                self.peer_addresses.remove(&peer_id);
                return;
            }
            DialError::NoAddresses => {
                tracing::error!(%peer_id, %connection_id, "no addresses to dial");
                self.reconnect_peers.remove(&peer_id);
                self.peer_reconnect_attempts.remove(&peer_id);
                return;
            }
            DialError::DialPeerConditionFalse(_) => {}
            DialError::Aborted => {}
            DialError::WrongPeerId { .. } => {
                tracing::error!(%peer_id, %connection_id, "wrong peer id");
                self.reconnect_peers.remove(&peer_id);
                self.peer_reconnect_attempts.remove(&peer_id);
                self.peer_keepalive.remove(&peer_id);
                self.peer_addresses.remove(&peer_id);
                return;
            }
            DialError::Denied { .. } => {}
            DialError::Transport(_) => {}
        }

        if let Some((duration, _)) = self.can_reconnect.get(&peer_id) {
            if let Entry::Occupied(mut entry) = self.peer_reconnect_attempts.entry(peer_id) {
                let current_attempts = entry.get_mut();
                if *current_attempts == 0 {
                    entry.remove();
                    self.reconnect_peers.remove(&peer_id);
                    self.peer_reconnect_attempts.remove(&peer_id);
                    return;
                }
                *current_attempts -= 1;

                if !self.reconnect_peers.contains_key(&peer_id) {
                    self.reconnect_peers.insert(peer_id, Delay::new(*duration));
                } else {
                    let timer = self
                        .reconnect_peers
                        .get_mut(&peer_id)
                        .expect("timer available");
                    timer.reset(*duration);
                }
            }
        }
    }
}

impl NetworkBehaviour for Behaviour {
    type ConnectionHandler = handler::Handler;
    type ToSwarm = void::Void;

    fn handle_pending_outbound_connection(
        &mut self,
        _: ConnectionId,
        peer_id: Option<PeerId>,
        _: &[Multiaddr],
        _: Endpoint,
    ) -> Result<Vec<Multiaddr>, ConnectionDenied> {
        let Some(peer_id) = peer_id else {
            return Ok(vec![]);
        };

        let list = self
            .peer_addresses
            .get(&peer_id)
            .cloned()
            .map(Vec::from_iter)
            .unwrap_or_default();

        Ok(list)
    }

    fn handle_established_inbound_connection(
        &mut self,
        _: ConnectionId,
        peer_id: PeerId,
        _: &Multiaddr,
        _: &Multiaddr,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        let keepalive = self.peer_keepalive.contains(&peer_id);
        Ok(handler::Handler::new(keepalive))
    }

    fn handle_established_outbound_connection(
        &mut self,
        _: ConnectionId,
        peer_id: PeerId,
        _: &Multiaddr,
        _: Endpoint,
        _: PortUse,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        let keepalive = self.peer_keepalive.contains(&peer_id);
        Ok(handler::Handler::new(keepalive))
    }

    fn on_connection_handler_event(
        &mut self,
        _: PeerId,
        _: ConnectionId,
        _: swarm::THandlerOutEvent<Self>,
    ) {
    }

    fn on_swarm_event(&mut self, event: FromSwarm) {
        match event {
            FromSwarm::AddressChange(ev) => self.on_address_change(ev),
            FromSwarm::ConnectionEstablished(ev) => self.on_connection_established(ev),
            FromSwarm::ConnectionClosed(ev) => self.on_connection_closed(ev),
            FromSwarm::DialFailure(ev) => self.on_dial_failure(ev),
            FromSwarm::ListenFailure(_) => {}
            FromSwarm::NewListener(_) => {}
            FromSwarm::NewListenAddr(_) => {}
            FromSwarm::ExpiredListenAddr(_) => {}
            FromSwarm::ListenerError(_) => {}
            FromSwarm::ListenerClosed(_) => {}
            FromSwarm::NewExternalAddrCandidate(_) => {}
            FromSwarm::ExternalAddrConfirmed(_) => {}
            FromSwarm::ExternalAddrExpired(_) => {}
            _ => {}
        }
    }

    fn poll(&mut self, cx: &mut Context) -> Poll<ToSwarm<Self::ToSwarm, THandlerInEvent<Self>>> {
        if let Some(event) = self.events.pop_front() {
            return Poll::Ready(event);
        }

        while let Poll::Ready(Some((peer_id, _))) = self.reconnect_peers.poll_next_unpin(cx) {
            let opts = DialOpts::peer_id(peer_id).build();
            self.events.push_back(ToSwarm::Dial { opts });
        }

        Poll::Pending
    }
}

fn address_from_connection_point(connection_point: &ConnectedPoint) -> Multiaddr {
    match connection_point {
        ConnectedPoint::Dialer { address, .. } => address.clone(),
        ConnectedPoint::Listener { local_addr, .. } if connection_point.is_relayed() => {
            local_addr.clone()
        }
        ConnectedPoint::Listener { send_back_addr, .. } => send_back_addr.clone(),
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use futures::{FutureExt, StreamExt};
    use libp2p::{
        swarm::{dial_opts::DialOpts, SwarmEvent},
        Multiaddr, PeerId, Swarm, SwarmBuilder,
    };

    use crate::AddPeerOpt;

    #[tokio::test]
    async fn dial_with_peer_id() -> anyhow::Result<()> {
        let (_, _, mut swarm1) = build_swarm(false).await;
        let (peer2, addr2, mut swarm2) = build_swarm(false).await;

        let opts = AddPeerOpt::with_peer_id(peer2).add_address(addr2);

        swarm1.behaviour_mut().add_address(opts);

        swarm1.dial(peer2)?;

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
        Ok(())
    }

    #[tokio::test]
    async fn remove_peer_address() -> anyhow::Result<()> {
        let (_, _, mut swarm1) = build_swarm(false).await;
        let (peer2, addr2, mut swarm2) = build_swarm(false).await;
        let opts = AddPeerOpt::with_peer_id(peer2).add_address(addr2);
        swarm1.behaviour_mut().add_address(opts);

        swarm1.dial(peer2)?;

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

        swarm1.disconnect_peer_id(peer2).expect("Shouldnt fail");

        loop {
            futures::select! {
                event = swarm1.select_next_some() => {
                    if let SwarmEvent::ConnectionClosed { peer_id, .. } = event {
                        assert_eq!(peer_id, peer2);
                        break;
                    }
                }
                _ = swarm2.next() => {}
            }
        }

        swarm1.behaviour_mut().remove_peer(&peer2);

        assert!(swarm1.dial(peer2).is_err());

        Ok(())
    }

    #[tokio::test]
    async fn dial_and_keepalive() -> anyhow::Result<()> {
        let (peer1, addr1, mut swarm1) = build_swarm(false).await;
        let (peer2, addr2, mut swarm2) = build_swarm(false).await;
        let opts_1 = AddPeerOpt::with_peer_id(peer2)
            .add_address(addr2)
            .keepalive();
        swarm1.behaviour_mut().add_address(opts_1);

        let opts_2 = AddPeerOpt::with_peer_id(peer1)
            .add_address(addr1)
            .keepalive();
        swarm2.behaviour_mut().add_address(opts_2);

        swarm1.dial(peer2)?;

        let mut peer_a_connected = false;
        let mut peer_b_connected = false;

        loop {
            futures::select! {
                event = swarm1.select_next_some() => {
                    if let SwarmEvent::ConnectionEstablished { peer_id, .. } = event {
                        assert_eq!(peer_id, peer2);
                        peer_b_connected = true;
                    }
                }
                event = swarm2.select_next_some() => {
                     if let SwarmEvent::ConnectionEstablished { peer_id, .. } = event {
                        assert_eq!(peer_id, peer1);
                        peer_a_connected = true;
                    }
                }
            }

            if peer_a_connected && peer_b_connected {
                break;
            }
        }

        let mut timer = futures_timer::Delay::new(Duration::from_secs(4)).fuse();

        loop {
            futures::select! {
                _ = &mut timer => {
                    break;
                }
                event = swarm1.select_next_some() => {
                    if let SwarmEvent::ConnectionClosed { peer_id, .. } = event {
                        assert_eq!(peer_id, peer2);
                        unreachable!("connection shouldnt have closed")
                    }
                }
                event = swarm2.select_next_some() => {
                    if let SwarmEvent::ConnectionClosed { peer_id, .. } = event {
                        assert_eq!(peer_id, peer1);
                        unreachable!("connection shouldnt have closed")
                    }
                }
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn store_address() -> anyhow::Result<()> {
        let (_, _, mut swarm1) = build_swarm(true).await;
        let (peer2, addr2, mut swarm2) = build_swarm(true).await;

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

        let addrs = swarm1
            .behaviour()
            .get_peer_addresses(&peer2)
            .expect("Exist");

        for addr in addrs {
            assert_eq!(addr, addr2);
        }
        Ok(())
    }

    async fn build_swarm(
        store_on_connection: bool,
    ) -> (PeerId, Multiaddr, Swarm<super::Behaviour>) {
        let mut swarm = SwarmBuilder::with_new_identity()
            .with_tokio()
            .with_tcp(
                libp2p::tcp::Config::default(),
                libp2p::noise::Config::new,
                libp2p::yamux::Config::default,
            )
            .expect("")
            .with_behaviour(|_| {
                super::Behaviour::with_config(super::Config {
                    store_on_connection,
                    ..Default::default()
                })
            })
            .expect("")
            .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(3)))
            .build();

        Swarm::listen_on(&mut swarm, "/ip4/127.0.0.1/tcp/0".parse().unwrap()).unwrap();

        if let Some(SwarmEvent::NewListenAddr { address, .. }) = swarm.next().await {
            let peer_id = swarm.local_peer_id();
            return (*peer_id, address, swarm);
        }

        panic!("no new addrs")
    }
}
