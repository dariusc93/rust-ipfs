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
            store_on_connection: true
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
    pub fn add_address(&mut self, peer_id: PeerId, addr: Multiaddr) -> Result<(), anyhow::Error> {
        //TODO: Instead of giving an error, we could extract/ignore the peer id.
        if let Some(Protocol::P2p(_)) = addr.iter().last() {
            anyhow::bail!("Address contains peer id")
        }

        match self.peer_addresses.entry(peer_id) {
            Entry::Occupied(mut e) => {
                let entry = e.get_mut();
                if entry.contains(&addr) {
                    anyhow::bail!("Address already exist");
                }

                entry.push(addr);
            }
            Entry::Vacant(e) => {
                e.insert(vec![addr]);
            }
        }
        Ok(())
    }

    pub fn remove_address(
        &mut self,
        peer_id: PeerId,
        addr: Multiaddr,
    ) -> Result<(), anyhow::Error> {
        if let Entry::Occupied(mut e) = self.peer_addresses.entry(peer_id) {
            let entry = e.get_mut();
            entry.retain(|item| addr.ne(item));
        }
        Ok(())
    }

    pub fn get_peer_addresses(&self, peer_id: PeerId) -> Option<&Vec<Multiaddr>> {
        self.peer_addresses.get(&peer_id)
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
