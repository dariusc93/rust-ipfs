use std::{
    collections::{hash_map::Entry, HashMap, VecDeque},
    task::{Context, Poll},
};

use libp2p::{
    core::{ConnectedPoint, Endpoint},
    multiaddr::Protocol,
    swarm::{
        self, derive_prelude::ConnectionEstablished,
        dummy::ConnectionHandler as DummyConnectionHandler, AddressChange, ConnectionDenied,
        ConnectionId, DialFailure, FromSwarm, NetworkBehaviour, PollParameters, THandler,
        THandlerInEvent, ToSwarm,
    },
    Multiaddr, PeerId,
};

#[derive(Debug)]
pub struct Config {
    pub store_on_connection: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            store_on_connection: true,
        }
    }
}

#[derive(Default, Debug)]
pub struct Behaviour {
    events: VecDeque<ToSwarm<<Self as NetworkBehaviour>::OutEvent, THandlerInEvent<Self>>>,
    peer_addresses: HashMap<PeerId, Vec<Multiaddr>>,
    config: Config,
}

impl Behaviour {
    pub fn add_address(&mut self, peer_id: PeerId, addr: Multiaddr) -> bool {
        if let Some(Protocol::P2p(_)) = addr.iter().last() {
            return false;
        }

        match self.peer_addresses.entry(peer_id) {
            Entry::Occupied(mut e) => {
                let entry = e.get_mut();
                if entry.contains(&addr) {
                    return false;
                }

                entry.push(addr);
            }
            Entry::Vacant(e) => {
                e.insert(vec![addr]);
            }
        }
        true
    }

    pub fn remove_address(&mut self, peer_id: &PeerId, addr: &Multiaddr) -> bool {
        if let Entry::Occupied(mut e) = self.peer_addresses.entry(*peer_id) {
            let entry = e.get_mut();
            if !entry.contains(addr) {
                return false;
            }
            entry.retain(|item| addr.ne(item));
        }
        true
    }

    pub fn remove_peer(&mut self, peer_id: &PeerId) -> bool {
        self.peer_addresses.remove(peer_id).is_some()
    }

    pub fn contains(&self, peer_id: &PeerId, addr: &Multiaddr) -> bool {
        self.get_peer_addresses(peer_id)
            .map(|list| !list.is_empty() && list.contains(addr))
            .unwrap_or_default()
    }

    pub fn get_peer_addresses(&self, peer_id: &PeerId) -> Option<&Vec<Multiaddr>> {
        self.peer_addresses.get(peer_id)
    }
}

impl NetworkBehaviour for Behaviour {
    type ConnectionHandler = DummyConnectionHandler;
    type OutEvent = void::Void;

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
        peer_id: Option<PeerId>,
        _: &[Multiaddr],
        _: Endpoint,
    ) -> Result<Vec<Multiaddr>, ConnectionDenied> {
        if let Some(peer_id) = peer_id {
            if let Entry::Occupied(e) = self.peer_addresses.entry(peer_id) {
                return Ok(e.get().clone());
            }
        }
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
        peer_id: PeerId,
        addr: &Multiaddr,
        _: Endpoint,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        if self.config.store_on_connection {
            match self.peer_addresses.entry(peer_id) {
                Entry::Occupied(mut e) => {
                    let entry = e.get_mut();
                    if !entry.contains(addr) {
                        entry.push(addr.clone());
                    }
                }
                Entry::Vacant(e) => {
                    e.insert(vec![addr.clone()]);
                }
            }
        }
        Ok(DummyConnectionHandler)
    }

    fn on_connection_handler_event(
        &mut self,
        _: libp2p::PeerId,
        _: swarm::ConnectionId,
        _: swarm::THandlerOutEvent<Self>,
    ) {
    }

    fn on_swarm_event(&mut self, event: FromSwarm<Self::ConnectionHandler>) {
        match event {
            FromSwarm::ConnectionEstablished(ConnectionEstablished {
                peer_id,
                endpoint,
                failed_addresses,
                ..
            }) => {
                if self.config.store_on_connection {
                    let multiaddr = match endpoint {
                        ConnectedPoint::Dialer { address, .. } => address.clone(),
                        ConnectedPoint::Listener { send_back_addr, .. } => send_back_addr.clone(),
                    };

                    match self.peer_addresses.entry(peer_id) {
                        Entry::Occupied(mut e) => {
                            let entry = e.get_mut();
                            if !entry.contains(&multiaddr) {
                                entry.push(multiaddr);
                            }

                            for failed in failed_addresses {
                                entry.retain(|addr| addr != failed);
                            }
                        }
                        Entry::Vacant(e) => {
                            e.insert(vec![multiaddr]);
                        }
                    }
                }
            }
            FromSwarm::AddressChange(AddressChange {
                peer_id, old, new, ..
            }) => {
                let old = match old {
                    ConnectedPoint::Dialer { address, .. } => address.clone(),
                    ConnectedPoint::Listener { send_back_addr, .. } => send_back_addr.clone(),
                };

                let new = match new {
                    ConnectedPoint::Dialer { address, .. } => address.clone(),
                    ConnectedPoint::Listener { send_back_addr, .. } => send_back_addr.clone(),
                };

                if let Entry::Occupied(mut e) = self.peer_addresses.entry(peer_id) {
                    let entry = e.get_mut();
                    if !entry.contains(&new) {
                        entry.push(new);
                    }

                    if entry.contains(&old) {
                        entry.retain(|addr| addr != &old);
                    }
                }
            }
            FromSwarm::DialFailure(DialFailure { peer_id: _, .. }) => {}
            _ => {}
        }
    }

    fn poll(
        &mut self,
        _: &mut Context,
        _: &mut impl PollParameters,
    ) -> Poll<ToSwarm<Self::OutEvent, THandlerInEvent<Self>>> {
        if let Some(event) = self.events.pop_front() {
            return Poll::Ready(event);
        }
        Poll::Pending
    }
}

#[cfg(test)]
mod test {
    use crate::p2p::transport::build_transport;
    use futures::StreamExt;
    use libp2p::{
        identity::Keypair,
        multiaddr::Protocol,
        swarm::{dial_opts::DialOpts, NetworkBehaviour, SwarmBuilder, SwarmEvent},
        Multiaddr, PeerId, Swarm,
    };

    #[derive(NetworkBehaviour)]
    struct Behaviour {
        address_book: super::Behaviour,
    }

    #[tokio::test]
    async fn dial_with_peer_id() -> anyhow::Result<()> {
        let (_, _, mut swarm1) = build_swarm().await;
        let (peer2, addr2, mut swarm2) = build_swarm().await;

        swarm1
            .behaviour_mut()
            .address_book
            .add_address(peer2, addr2);

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
    async fn store_address() -> anyhow::Result<()> {
        let (_, _, mut swarm1) = build_swarm().await;
        let (peer2, addr2, mut swarm2) = build_swarm().await;

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
            .address_book
            .get_peer_addresses(&peer2)
            .cloned()
            .expect("Exist");
        let addr2 = addr2.with(Protocol::P2p(peer2.into()));
        for addr in addrs {
            assert_eq!(addr, addr2);
        }
        Ok(())
    }

    async fn build_swarm() -> (PeerId, Multiaddr, libp2p::swarm::Swarm<Behaviour>) {
        let key = Keypair::generate_ed25519();
        let pubkey = key.public();
        let peer_id = pubkey.to_peer_id();
        let transport = build_transport(key, None, Default::default()).unwrap();

        let behaviour = Behaviour {
            address_book: super::Behaviour::default(),
        };

        let mut swarm = SwarmBuilder::with_tokio_executor(transport, behaviour, peer_id).build();

        Swarm::listen_on(&mut swarm, "/ip4/127.0.0.1/tcp/0".parse().unwrap()).unwrap();

        if let Some(SwarmEvent::NewListenAddr { address, .. }) = swarm.next().await {
            return (peer_id, address, swarm);
        }

        panic!("no new addrs")
    }
}
