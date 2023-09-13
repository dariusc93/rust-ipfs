mod handler;

use std::{
    collections::{hash_map::Entry, HashMap, HashSet, VecDeque},
    error::Error,
    hash::Hash,
    task::{Context, Poll},
    time::Duration,
};

use futures::StreamExt;
use futures_timer::Delay;
use libp2p::{
    core::Endpoint,
    multiaddr::Protocol,
    swarm::{
        derive_prelude::{ConnectionEstablished, ListenerId},
        dial_opts::DialOpts,
        AddressChange, ConnectionClosed, ConnectionDenied, ConnectionId, DialFailure,
        ExpiredListenAddr, FromSwarm, ListenOpts, ListenerClosed, ListenerError, NetworkBehaviour,
        NewListenAddr, PollParameters, THandler, THandlerInEvent, THandlerOutEvent, ToSwarm,
    },
    Multiaddr, PeerId,
};
use rand::seq::SliceRandom;

#[derive(Debug)]
pub enum Event {
    ReservationSuccessful {
        peer_id: PeerId,
        initial_addr: Multiaddr,
    },
    ReservationClosed {
        peer_id: PeerId,
        result: Result<(), Box<dyn Error + Send>>,
    },
    ReservationFailure {
        peer_id: PeerId,
        result: Box<dyn Error + Send>,
    },
    FindRelays {
        /// Namespace of where to locate possible relay candidates
        namespace: Option<String>,
        /// Channel to send peer ids of the candidates
        channel: futures::channel::mpsc::Sender<HashSet<PeerId>>,
    },
}

#[derive(Debug)]
struct Connection {
    pub peer_id: PeerId,
    pub id: ConnectionId,
    pub addr: Multiaddr,
    pub candidacy: Candidate,
    pub rtt: Option<[Duration; 3]>,
}

#[derive(Debug)]
enum Candidate {
    Pending,
    Unsupported,
    Confirmed {
        listener_id: Option<ListenerId>,
        addresses: Vec<Multiaddr>,
    },
}

impl PartialEq for Connection {
    fn eq(&self, other: &Self) -> bool {
        self.id.eq(&other.id)
    }
}

impl Eq for Connection {}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PendingReservation {
    peer_id: PeerId,
    connection_id: ConnectionId,
    listener_id: ListenerId,
}

impl Hash for PendingReservation {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.listener_id.hash(state)
    }
}

#[derive(Debug)]
#[allow(dead_code)]
enum ReconnectState {
    Idle {
        backoff: bool,
        delay: Delay,
    },
    Pending {
        connection_id: ConnectionId,
        backoff: bool,
    },
}

#[derive(Default, Debug)]
#[allow(dead_code)]
pub struct Behaviour {
    events: VecDeque<ToSwarm<<Self as NetworkBehaviour>::ToSwarm, THandlerInEvent<Self>>>,
    relays: HashMap<PeerId, Vec<Multiaddr>>,
    connections: HashMap<PeerId, Vec<Connection>>,

    reconnect: HashMap<PeerId, ReconnectState>,

    discovery_channel: HashMap<u64, futures::channel::mpsc::Receiver<HashSet<PeerId>>>,

    pending_connection: HashSet<ConnectionId>,
    pending_selection: HashSet<PeerId>,
    config: Config,
}

#[derive(Debug, Default)]
pub struct Config {
    /// Automatically add confirmed connections to the relay list
    pub auto_relay: bool,

    /// Automatically connect to peers that are added
    pub auto_connect: bool,

    /// Min data limit for relay reservation. Anything under this value would reject the relay reservation
    pub limit: Option<u64>,

    /// Retry relay connection
    pub backoff: Duration,
}

impl Behaviour {
    pub fn new(config: Config) -> Behaviour {
        Self {
            config,
            events: VecDeque::default(),
            relays: HashMap::default(),
            connections: HashMap::default(),
            reconnect: HashMap::default(),
            discovery_channel: HashMap::default(),
            pending_connection: HashSet::default(),
            pending_selection: HashSet::default(),
        }
    }

    pub fn add_address(&mut self, peer_id: PeerId, addr: Multiaddr) {
        match self.relays.entry(peer_id) {
            Entry::Vacant(entry) => {
                entry.insert(vec![addr.clone()]);
            }
            Entry::Occupied(mut entry) => {
                let list = entry.get_mut();
                if list.contains(&addr) {
                    return;
                }
                list.push(addr.clone());
            }
        }
        if self.config.auto_connect {
            if let Entry::Occupied(entry) = self.connections.entry(peer_id) {
                if entry.get().iter().any(|connection| connection.addr == addr) {
                    return;
                }
            }

            let opts = DialOpts::peer_id(peer_id).build();
            self.events.push_back(ToSwarm::Dial { opts })
        }
    }

    pub fn remove_address(&mut self, peer_id: PeerId, addr: Multiaddr) {
        if let Entry::Occupied(mut entry) = self.relays.entry(peer_id) {
            let list = entry.get_mut();

            if let Some(connection) = self.connections.get(&peer_id).and_then(|connections| {
                connections
                    .iter()
                    .find(|connection| connection.addr.eq(&addr))
            }) {
                if let Candidate::Confirmed {
                    listener_id: Some(id),
                    ..
                } = connection.candidacy
                {
                    self.events.push_back(ToSwarm::RemoveListener { id });
                }
            }

            list.retain(|inner_addr| addr.ne(inner_addr));
            if list.is_empty() {
                entry.remove();
            }
        }
    }

    pub fn list_relays(&self) -> impl Iterator<Item = (&PeerId, &Vec<Multiaddr>)> {
        self.relays.iter()
    }

    pub fn list_active_relays(&self) -> Vec<(PeerId, Vec<Multiaddr>)> {
        self.connections
            .iter()
            .filter(|(_, connections)| {
                connections.iter().any(|connection| {
                    matches!(
                        connection.candidacy,
                        Candidate::Confirmed {
                            listener_id: Some(_),
                            ..
                        }
                    )
                })
            })
            .map(|(peer_id, connections)| {
                (
                    *peer_id,
                    connections
                        .iter()
                        .map(|connection| &connection.addr)
                        .cloned()
                        .collect::<Vec<_>>(),
                )
            })
            .collect()
    }

    #[allow(dead_code)]
    fn avg_rtt(&self, connection: &Connection) -> u128 {
        let rtts = connection.rtt.unwrap_or_default();
        let avg: u128 = rtts.iter().map(|duration| duration.as_millis()).sum();
        // used in case we cant produce a full avg
        let div = rtts.iter().filter(|i| !i.is_zero()).count() as u128;
        avg / div
    }

    pub fn select(&mut self, peer_id: PeerId) {
        if !self.relays.contains_key(&peer_id) {
            return;
        }

        if self.pending_selection.contains(&peer_id) {
            return;
        }

        if !self.connections.contains_key(&peer_id) {
            let opts = DialOpts::peer_id(peer_id).build();
            let id = opts.connection_id();
            self.pending_connection.insert(id);
            self.events.push_back(ToSwarm::Dial { opts });
            self.pending_selection.insert(peer_id);
            return;
        }

        let connections = match self.connections.get_mut(&peer_id) {
            Some(conns) => conns,
            None => return,
        };

        if connections.is_empty() {
            return;
        }

        let mut blacklist = Vec::new();
        let mut rng = rand::thread_rng();
        let connection = loop {
            let connection = connections
                .choose_mut(&mut rng)
                .expect("Connections to be available");

            if blacklist.contains(&connection.id) {
                continue;
            }

            if let Candidate::Confirmed {
                listener_id: Some(_),
                ..
            } = connection.candidacy
            {
                blacklist.push(connection.id);
                continue;
            }

            break connection;
        };

        if matches!(connection.candidacy, Candidate::Pending) {
            self.pending_selection.insert(peer_id);
            return;
        }

        let relay_addr = connection.addr.clone().with(Protocol::P2pCircuit);

        let opts = ListenOpts::new(relay_addr);

        let id = opts.listener_id();

        if let Candidate::Confirmed { listener_id, .. } = &mut connection.candidacy {
            *listener_id = Some(id);
        }

        self.events.push_back(ToSwarm::ListenOn { opts });
    }

    pub fn random_select(&mut self) -> Option<PeerId> {
        let relay_peers = self.relays.keys().copied().collect::<Vec<_>>();
        if relay_peers.is_empty() {
            return None;
        }

        let mut rng = rand::thread_rng();

        let peer_id = relay_peers.choose(&mut rng)?;

        self.select(*peer_id);

        Some(*peer_id)
    }

    pub fn disable_relay(&mut self, peer_id: PeerId) {
        for connection in self
            .connections
            .iter()
            .filter(|(peer, _)| peer_id == **peer)
            .flat_map(|(_, connections)| connections)
        {
            if let Candidate::Confirmed { .. } = connection.candidacy {
                //TODO: Use ListenerId instead of closing a connection
                let connection = libp2p::swarm::CloseConnection::One(connection.id);
                self.events.push_back(ToSwarm::CloseConnection {
                    peer_id,
                    connection,
                });
            }
        }
    }

    pub fn set_peer_rtt(&mut self, peer_id: PeerId, connection_id: ConnectionId, rtt: Duration) {
        if let Entry::Occupied(mut entry) = self.connections.entry(peer_id) {
            let connections = entry.get_mut();
            if let Some(connection) = connections
                .iter_mut()
                .find(|connection| connection.id == connection_id)
            {
                match connection.rtt.as_mut() {
                    Some(connection_rtt) => {
                        connection_rtt.rotate_left(1);
                        connection_rtt[2] = rtt;
                    }
                    None => connection.rtt = Some([Duration::ZERO, Duration::ZERO, rtt]),
                }
            }
        }
    }

    fn on_listen_on(
        &mut self,
        NewListenAddr {
            listener_id,
            addr: direct_addr,
        }: NewListenAddr,
    ) {
        if !direct_addr
            .iter()
            .any(|proto| matches!(proto, Protocol::P2pCircuit))
        {
            return;
        }

        for connection in self
            .connections
            .values_mut()
            .flatten()
            .filter(|connection| {
                if let Candidate::Confirmed {
                    listener_id: Some(id),
                    ..
                } = connection.candidacy
                {
                    id == listener_id
                } else {
                    false
                }
            })
        {
            match &mut connection.candidacy {
                Candidate::Confirmed {
                    listener_id: id,
                    addresses,
                } => {
                    *id = Some(listener_id);
                    let first = addresses.is_empty();
                    if !addresses.contains(direct_addr) {
                        addresses.push(direct_addr.clone());
                        if first {
                            self.events.push_back(ToSwarm::GenerateEvent(
                                Event::ReservationSuccessful {
                                    peer_id: connection.peer_id,
                                    initial_addr: direct_addr.clone(),
                                },
                            ))
                        }
                    }
                }
                Candidate::Pending | Candidate::Unsupported => {
                    // Maybe panic if we reach this clause?
                }
            };
        }
    }

    fn on_listener_close(
        &mut self,
        ListenerClosed {
            listener_id,
            reason,
        }: ListenerClosed,
    ) {
        let Some(connection) =
            self.connections
                .values_mut()
                .flatten()
                .find(|connection| match connection.candidacy {
                    Candidate::Confirmed {
                        listener_id: Some(id),
                        ..
                    } => id == listener_id,
                    _ => false,
                })
        else {
            return;
        };

        if let Candidate::Confirmed {
            listener_id,
            addresses,
        } = &mut connection.candidacy
        {
            listener_id.take();
            let addrs = std::mem::take(addresses);
            let has_addresses = addrs.is_empty();

            for addr in addrs {
                self.events.push_back(ToSwarm::ExternalAddrExpired(addr));
            }

            match (has_addresses, reason) {
                (true, result) => {
                    self.events
                        .push_back(ToSwarm::GenerateEvent(Event::ReservationClosed {
                            peer_id: connection.peer_id,
                            result: result
                                .map_err(|e| std::io::Error::new(e.kind(), e.to_string()))
                                .map_err(|e| Box::new(e) as Box<_>),
                        }))
                }
                (false, Err(e)) => {
                    self.events
                        .push_back(ToSwarm::GenerateEvent(Event::ReservationFailure {
                            peer_id: connection.peer_id,
                            result: Box::new(std::io::Error::new(e.kind(), e.to_string())),
                        }))
                }
                _ => {}
            }
        }
    }

    fn on_listener_error(&mut self, ListenerError { listener_id, err }: ListenerError) {
        let Some(connection) =
            self.connections
                .values_mut()
                .flatten()
                .find(|connection| match connection.candidacy {
                    Candidate::Confirmed {
                        listener_id: Some(id),
                        ..
                    } => id == listener_id,
                    _ => false,
                })
        else {
            return;
        };

        if let Candidate::Confirmed {
            listener_id,
            addresses,
        } = &mut connection.candidacy
        {
            listener_id.take();
            let addrs = std::mem::take(addresses);
            for addr in addrs {
                self.events.push_back(ToSwarm::ExternalAddrExpired(addr));
            }
            self.events
                .push_back(ToSwarm::GenerateEvent(Event::ReservationFailure {
                    peer_id: connection.peer_id,
                    result: Box::new(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        err.to_string(),
                    )),
                }))
        }
    }

    fn on_listener_expired(&mut self, ExpiredListenAddr { listener_id, addr }: ExpiredListenAddr) {
        let Some(connection) =
            self.connections
                .values_mut()
                .flatten()
                .find(|connection| match connection.candidacy {
                    Candidate::Confirmed {
                        listener_id: Some(id),
                        ..
                    } => id == listener_id,
                    _ => false,
                })
        else {
            return;
        };

        if let Candidate::Confirmed { addresses, .. } = &mut connection.candidacy {
            if !addresses.contains(addr) {
                return;
            }

            addresses.retain(|a| a != addr);

            self.events
                .push_back(ToSwarm::ExternalAddrExpired(addr.clone()));
        }
    }

    fn on_address_change(
        &mut self,
        AddressChange {
            peer_id,
            connection_id,
            old,
            new,
        }: AddressChange,
    ) {
        let Some(connections) = self.connections.get_mut(&peer_id) else {
            return;
        };

        let Some(connection) = connections
            .iter_mut()
            .find(|connection| connection.id == connection_id)
        else {
            return;
        };

        let old_addr = match old {
            libp2p::core::ConnectedPoint::Dialer { address, .. } => address,
            libp2p::core::ConnectedPoint::Listener { send_back_addr, .. } => send_back_addr,
        };

        let new_addr = match new {
            libp2p::core::ConnectedPoint::Dialer { address, .. } => address,
            libp2p::core::ConnectedPoint::Listener { send_back_addr, .. } => send_back_addr,
        };

        if old_addr == new_addr {
            return;
        }

        connection.addr = new_addr.clone();
    }

    fn on_dial_failure(
        &mut self,
        DialFailure {
            peer_id: _,
            error: _,
            connection_id,
        }: DialFailure,
    ) {
        self.pending_connection.remove(&connection_id);

        //TODO: perform checks and do a reconnect attempt

        // let Some(peer_id) = peer_id else {
        //     return;
        // };

        // match error {
        //     libp2p::swarm::DialError::LocalPeerId { .. } => {
        //         self.relays.remove(&peer_id);
        //     }
        //     libp2p::swarm::DialError::NoAddresses => {
        //         self.relays.remove(&peer_id);
        //     }
        //     libp2p::swarm::DialError::DialPeerConditionFalse(_) => {}
        //     libp2p::swarm::DialError::Aborted => {}
        //     libp2p::swarm::DialError::WrongPeerId { obtained, endpoint } => {}
        //     libp2p::swarm::DialError::Denied { cause } => {}
        //     libp2p::swarm::DialError::Transport(_) => {}
        // }
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
        self.pending_connection.remove(&connection_id);

        let addr = match endpoint {
            libp2p::core::ConnectedPoint::Dialer { address, .. } => address,
            libp2p::core::ConnectedPoint::Listener { send_back_addr, .. } => send_back_addr,
        };

        match self.relays.entry(peer_id) {
            Entry::Occupied(entry) => {
                let mut addr = addr.clone();
                addr.pop();

                if !entry.get().contains(&addr) {
                    return;
                }
            }
            Entry::Vacant(_) if self.config.auto_connect => {}
            _ => return,
        };

        let connection = Connection {
            peer_id,
            id: connection_id,
            addr: addr.clone(),
            candidacy: Candidate::Pending,
            rtt: None,
        };

        self.connections
            .entry(peer_id)
            .or_default()
            .push(connection);

        if self.pending_selection.remove(&peer_id) {
            self.select(peer_id);
        }
    }

    fn on_connection_closed(
        &mut self,
        ConnectionClosed {
            peer_id,
            connection_id,
            ..
        }: ConnectionClosed<'_, <Self as NetworkBehaviour>::ConnectionHandler>,
    ) {
        if let Entry::Occupied(mut entry) = self.connections.entry(peer_id) {
            let connections = entry.get_mut();
            let Some(connection) = connections
                .iter_mut()
                .find(|connection| connection.id == connection_id)
            else {
                return;
            };

            //Note: If the listener has been closed, then this condition may not happen
            //      but is set as a precaution
            //TODO: Confirm that the order is consistent if the relay is removed
            if let Candidate::Confirmed {
                listener_id,
                addresses,
            } = &mut connection.candidacy
            {
                if let Some(listener_id) = listener_id.take() {
                    let addrs = std::mem::take(addresses);
                    for addr in addrs {
                        self.events.push_back(ToSwarm::ExternalAddrExpired(addr));
                    }
                    self.events
                        .push_back(ToSwarm::RemoveListener { id: listener_id });

                    self.events
                        .push_back(ToSwarm::GenerateEvent(Event::ReservationClosed {
                            peer_id: connection.peer_id,
                            result: Ok(()),
                        }))
                }
            }

            connections.retain(|connection| connection.id != connection_id);

            if connections.is_empty() {
                entry.remove();
            }
        }
    }

    pub fn process_relay_event(&mut self, event: libp2p::relay::client::Event) {
        //TODO: Perform checks on limit reported from the reservation and either accept it or
        //      disconnect and attempt a different connection to a relay with a higher
        //      limit requirement
        //NOTE: This is helpful if one knows that the relays will have a higher limit, otherwise
        //      this may cause long waits when attempting to find relays with higher limits
        //      for the reservation
        match event {
            libp2p::relay::client::Event::ReservationReqAccepted { .. } => {}
            libp2p::relay::client::Event::ReservationReqFailed { .. } => {}
            _ => {}
        }
    }
}

impl NetworkBehaviour for Behaviour {
    type ToSwarm = Event;
    type ConnectionHandler = handler::Handler;

    fn handle_established_inbound_connection(
        &mut self,
        _connection_id: ConnectionId,
        _peer: PeerId,
        _local_addr: &Multiaddr,
        _remote_addr: &Multiaddr,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        Ok(handler::Handler::default())
    }

    fn handle_established_outbound_connection(
        &mut self,
        _connection_id: ConnectionId,
        _peer: PeerId,
        _addr: &Multiaddr,
        _role_override: Endpoint,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        Ok(handler::Handler::default())
    }

    fn handle_pending_outbound_connection(
        &mut self,
        _: ConnectionId,
        maybe_peer: Option<PeerId>,
        _: &[Multiaddr],
        _: Endpoint,
    ) -> Result<Vec<Multiaddr>, ConnectionDenied> {
        let addrs = maybe_peer
            .and_then(|peer_id| self.relays.get(&peer_id))
            .cloned()
            .unwrap_or_default();

        Ok(addrs)
    }

    fn on_swarm_event(&mut self, event: FromSwarm<Self::ConnectionHandler>) {
        match event {
            FromSwarm::ConnectionEstablished(event) => self.on_connection_established(event),
            FromSwarm::ConnectionClosed(event) => self.on_connection_closed(event),
            FromSwarm::NewListenAddr(event) => self.on_listen_on(event),
            FromSwarm::ListenerClosed(event) => self.on_listener_close(event),
            FromSwarm::DialFailure(event) => self.on_dial_failure(event),
            FromSwarm::ListenerError(event) => self.on_listener_error(event),
            FromSwarm::ExpiredListenAddr(event) => self.on_listener_expired(event),
            FromSwarm::AddressChange(event) => self.on_address_change(event),
            FromSwarm::ExternalAddrConfirmed(_)
            | FromSwarm::NewExternalAddrCandidate(_)
            | FromSwarm::ExternalAddrExpired(_)
            | FromSwarm::ListenFailure(_)
            | FromSwarm::NewListener(_) => {}
        }
    }

    fn on_connection_handler_event(
        &mut self,
        peer_id: PeerId,
        connection_id: ConnectionId,
        event: THandlerOutEvent<Self>,
    ) {
        match event {
            handler::Out::Supported => {
                if let Entry::Occupied(mut entry) = self.connections.entry(peer_id) {
                    let list = entry.get_mut();
                    if let Some(connection) = list
                        .iter_mut()
                        .find(|connection| connection.id == connection_id)
                    {
                        let canadate_state = &mut connection.candidacy;

                        if matches!(canadate_state, Candidate::Pending | Candidate::Unsupported) {
                            *canadate_state = Candidate::Confirmed {
                                listener_id: None,
                                addresses: vec![],
                            };
                            if self.pending_selection.remove(&peer_id) {
                                self.select(peer_id);
                            }
                        }
                    }
                }
            }
            handler::Out::Unsupported => {
                if let Entry::Occupied(mut entry) = self.connections.entry(peer_id) {
                    let list = entry.get_mut();
                    if let Some(connection) = list
                        .iter_mut()
                        .find(|connection| connection.id == connection_id)
                    {
                        let canadate_state = &mut connection.candidacy;

                        if let Candidate::Confirmed {
                            listener_id: Some(id),
                            ..
                        } = canadate_state
                        {
                            let id = *id;
                            self.events.push_back(ToSwarm::RemoveListener { id });
                        }

                        *canadate_state = Candidate::Unsupported;
                        self.pending_selection.remove(&peer_id);
                    }
                }
            }
        }
    }

    fn poll(
        &mut self,
        cx: &mut Context<'_>,
        _: &mut impl PollParameters,
    ) -> Poll<ToSwarm<Self::ToSwarm, THandlerInEvent<Self>>> {
        if let Some(event) = self.events.pop_front() {
            return Poll::Ready(event);
        }

        self.discovery_channel
            .retain(|_, rx| match rx.poll_next_unpin(cx) {
                Poll::Ready(Some(list)) => {
                    for peer_id in list {
                        self.relays.entry(peer_id).or_default();
                    }
                    false
                }
                Poll::Ready(None) => false,
                Poll::Pending => true,
            });

        Poll::Pending
    }
}
