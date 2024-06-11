mod handler;

use std::{
    collections::{hash_map::Entry, HashMap, HashSet, VecDeque},
    task::{Context, Poll},
};

use crate::AddPeerOpt;
use libp2p::swarm::ConnectionClosed;
use libp2p::{
    core::{ConnectedPoint, Endpoint},
    multiaddr::Protocol,
    swarm::{
        self, behaviour::ConnectionEstablished, AddressChange, ConnectionDenied, ConnectionId,
        FromSwarm, NetworkBehaviour, THandler, THandlerInEvent, ToSwarm,
    },
    Multiaddr, PeerId,
};

#[derive(Default, Debug, Copy, Clone)]
pub struct Config {
    /// Store peer address on an established connection
    pub store_on_connection: bool,
    /// Keep connection alive automatically if peer is added through `Behaviour::add_address`
    pub keep_connection_alive: bool,
}

#[derive(Default, Debug)]
pub struct Behaviour {
    events: VecDeque<ToSwarm<<Self as NetworkBehaviour>::ToSwarm, THandlerInEvent<Self>>>,
    connections: HashMap<PeerId, HashSet<ConnectionId>>,
    peer_addresses: HashMap<PeerId, HashSet<Multiaddr>>,
    peer_keepalive: HashSet<PeerId>,
    config: Config,
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

        if !self.config.store_on_connection {
            return;
        }

        let mut addr = match endpoint {
            ConnectedPoint::Dialer { address, .. } => address.clone(),
            ConnectedPoint::Listener { local_addr, .. } if endpoint.is_relayed() => {
                local_addr.clone()
            }
            ConnectedPoint::Listener { send_back_addr, .. } => send_back_addr.clone(),
        };

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
            FromSwarm::AddressChange(AddressChange {
                peer_id, old, new, ..
            }) => {
                let mut old = match old {
                    ConnectedPoint::Dialer { address, .. } => address.clone(),
                    ConnectedPoint::Listener { local_addr, .. } if old.is_relayed() => {
                        local_addr.clone()
                    }
                    ConnectedPoint::Listener { send_back_addr, .. } => send_back_addr.clone(),
                };

                if matches!(old.iter().last(), Some(Protocol::P2p(_))) {
                    old.pop();
                }

                let mut new = match new {
                    ConnectedPoint::Dialer { address, .. } => address.clone(),
                    ConnectedPoint::Listener { local_addr, .. } if new.is_relayed() => {
                        local_addr.clone()
                    }
                    ConnectedPoint::Listener { send_back_addr, .. } => send_back_addr.clone(),
                };

                if matches!(new.iter().last(), Some(Protocol::P2p(_))) {
                    new.pop();
                }

                if let Entry::Occupied(mut e) = self.peer_addresses.entry(peer_id) {
                    let entry = e.get_mut();
                    entry.insert(new);
                    entry.remove(&old);
                }
            }
            FromSwarm::ConnectionEstablished(ev) => self.on_connection_established(ev),
            FromSwarm::ConnectionClosed(ev) => self.on_connection_closed(ev),
            FromSwarm::DialFailure(_) => {}
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

    fn poll(&mut self, _: &mut Context) -> Poll<ToSwarm<Self::ToSwarm, THandlerInEvent<Self>>> {
        if let Some(event) = self.events.pop_front() {
            return Poll::Ready(event);
        }
        Poll::Pending
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
