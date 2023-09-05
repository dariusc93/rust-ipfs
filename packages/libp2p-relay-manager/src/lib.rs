mod handler;

use std::{
    collections::{hash_map::Entry, HashMap, HashSet, VecDeque},
    error::Error,
    task::{Context, Poll},
    time::Duration,
};

use libp2p::{
    core::Endpoint,
    multiaddr::Protocol,
    swarm::{
        derive_prelude::{ConnectionEstablished, ListenerId},
        dial_opts::DialOpts,
        ConnectionClosed, ConnectionDenied, ConnectionId, FromSwarm, ListenOpts, ListenerClosed,
        NetworkBehaviour, NewListenAddr, PollParameters, THandler, THandlerInEvent,
        THandlerOutEvent, ToSwarm,
    },
    Multiaddr, PeerId,
};
use rand::seq::SliceRandom;

#[derive(Debug)]
pub enum Event {
    ReservationSuccessful {
        peer_id: PeerId,
        addr: Multiaddr,
    },
    ReservationFailure {
        peer_id: PeerId,
        result: Box<dyn Error + Send>,
    },
}

#[derive(Debug)]
struct Connection {
    pub id: ConnectionId,
    pub addr: Multiaddr,
    pub confirmed: bool,
    pub reserved: Option<ListenerId>,
    pub accepted: bool,
    pub rtt: [Duration; 3],
}

impl PartialEq for Connection {
    fn eq(&self, other: &Self) -> bool {
        self.id.eq(&other.id)
    }
}

#[derive(Debug)]
struct PendingReservation {
    peer_id: PeerId,
    connection_id: ConnectionId,
}

#[derive(Debug)]
pub struct Behaviour {
    local_peer_id: PeerId,
    events: VecDeque<ToSwarm<<Self as NetworkBehaviour>::ToSwarm, THandlerInEvent<Self>>>,
    relays: HashMap<PeerId, Vec<Multiaddr>>,
    connections: HashMap<PeerId, Vec<Connection>>,

    pending_connection: HashSet<ConnectionId>,
    pending_selection: HashMap<PeerId, SelectOpt>,
    pending_reservation: HashMap<ListenerId, PendingReservation>,
    config: Config,
}

#[derive(Debug, Default, Clone)]
pub enum SelectOpt {
    Multiaddr {
        addr: Multiaddr,
    },
    Connection {
        connection_id: ConnectionId,
    },
    LowestRTT,
    #[default]
    Random,
    Index {
        index: usize,
    },
}

#[derive(Debug, Default)]
pub struct Config {
    /// Automatically add confirmed connections to the relay list
    pub auto_relay: bool,

    /// Automatically connect to peers that are added
    pub auto_connect: bool,

    /// Min data limit for relay reservation. Anything under this value would reject the relay reservation
    pub limit: Option<u64>,
}

impl Behaviour {
    pub fn new(local_peer_id: PeerId, config: Config) -> Behaviour {
        Self {
            config,
            events: VecDeque::default(),
            local_peer_id,
            relays: HashMap::default(),
            connections: HashMap::default(),
            pending_connection: HashSet::default(),
            pending_selection: HashMap::default(),
            pending_reservation: HashMap::default(),
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
                if let Some(id) = connection.reserved {
                    self.events.push_back(ToSwarm::RemoveListener { id });
                }
            }

            list.retain(|inner_addr| addr.ne(inner_addr));
            if list.is_empty() {
                entry.remove();
            }
        }
    }

    fn avg_rtt(&self, connection: &Connection) -> u128 {
        let rtts = connection.rtt;
        let avg: u128 = rtts.iter().map(|duration| duration.as_millis()).sum();
        // used in case we cant produce a full avg
        let div = rtts.iter().filter(|i| !i.is_zero()).count() as u128;
        avg / div
    }

    pub fn select(&mut self, peer_id: PeerId, opts: SelectOpt) {
        if !self.relays.contains_key(&peer_id) {
            return;
        }

        if self.pending_selection.contains_key(&peer_id) {
            return;
        }

        if !self.connections.contains_key(&peer_id) {
            let select_opts = opts;
            let opts = DialOpts::peer_id(peer_id).build();
            let id = opts.connection_id();
            self.pending_connection.insert(id);
            self.events.push_back(ToSwarm::Dial { opts });
            self.pending_selection.insert(peer_id, select_opts);
            return;
        }

        let connections = match self.connections.get(&peer_id) {
            Some(conns) => conns,
            None => return,
        };

        let connection = match &opts {
            SelectOpt::Multiaddr { addr } => connections
                .iter()
                .find(|connection| connection.addr.eq(addr))
                .expect("Address to be within a connection"),
            SelectOpt::LowestRTT => {
                let mut lowest_connection = None;
                for connection in connections {
                    match lowest_connection {
                        Some(current) => {
                            let current_avg_rtt = self.avg_rtt(current);
                            let connection_avg_rtt = self.avg_rtt(connection);

                            if connection_avg_rtt < current_avg_rtt {
                                lowest_connection = Some(connection);
                            }
                        }
                        None => {
                            lowest_connection = Some(connection);
                        }
                    }
                }
                lowest_connection.expect("connection with lowest avg rtt to be available")
            }
            SelectOpt::Random => {
                let mut blacklist = Vec::new();
                let mut rng = rand::thread_rng();
                loop {
                    let connection = connections
                        .choose(&mut rng)
                        .expect("Connections to be available");
                    if blacklist.contains(&connection) {
                        continue;
                    }
                    if connection.reserved.is_some() {
                        blacklist.push(connection);
                        continue;
                    }
                    break connection;
                }
            }
            SelectOpt::Connection { connection_id } => connections
                .iter()
                .find(|connection| connection.id.eq(connection_id))
                .expect("Address to be within a connection"),
            SelectOpt::Index { index } => connections.get(*index).expect("Index to exist"),
        };

        if !connection.confirmed {
            self.pending_selection.insert(peer_id, opts);
            return;
        }

        let relay_addr = connection.addr.clone().with(Protocol::P2pCircuit);

        let pending_reservation = PendingReservation {
            peer_id,
            connection_id: connection.id,
        };

        let opts = ListenOpts::new(relay_addr);

        let id = opts.listener_id();

        self.events.push_back(ToSwarm::ListenOn { opts });

        self.pending_reservation.insert(id, pending_reservation);
    }

    pub fn set_peer_rtt(&mut self, peer_id: PeerId, connection_id: ConnectionId, rtt: Duration) {
        if let Entry::Occupied(mut entry) = self.connections.entry(peer_id) {
            let connections = entry.get_mut();
            if let Some(connection) = connections
                .iter_mut()
                .find(|connection| connection.id == connection_id)
            {
                connection.rtt.rotate_left(1);
                connection.rtt[2] = rtt;
            }
        }
    }

    fn on_listen_on(&mut self, NewListenAddr { listener_id, addr }: NewListenAddr) {
        let mut addr = addr.clone();
        if !addr
            .iter()
            .any(|proto| matches!(proto, Protocol::P2pCircuit))
        {
            return;
        }


        addr.pop();

        let pending_reservation = match self.pending_reservation.remove(&listener_id) {
            Some(reservation) => reservation,
            None => return,
        };

        if let Entry::Occupied(mut entry) = self.connections.entry(pending_reservation.peer_id) {
            let connections = entry.get_mut();
            let mut raw_addr = addr.clone();
            raw_addr.pop();
            if let Some(connection) = connections
                .iter_mut()
                .find(|connection| connection.id.eq(&pending_reservation.connection_id))
            {
                connection.reserved = Some(listener_id);
                connection.accepted = true;
                self.events.push_back(ToSwarm::ExternalAddrConfirmed(addr));
            }
        }
    }

    fn on_close_listener(
        &mut self,
        ListenerClosed {
            listener_id: _id, ..
        }: ListenerClosed,
    ) {
        if let Some(connection) = self
            .connections
            .values_mut()
            .flatten()
            .find(|connection| matches!(connection.reserved, Some(_id)))
        {
            if connection.reserved.take().is_some() {
                let addr = connection
                    .addr
                    .clone()
                    .with(Protocol::P2pCircuit)
                    .with(Protocol::P2p(self.local_peer_id));
                self.events.push_back(ToSwarm::ExternalAddrExpired(addr));
            }
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
            Entry::Vacant(_) if self.config.auto_relay => {}
            _ => return,
        };
        let connection = Connection {
            id: connection_id,
            addr: addr.clone(),
            confirmed: false,
            reserved: None,
            accepted: false,
            rtt: [Duration::ZERO, Duration::ZERO, Duration::ZERO],
        };

        self.connections
            .entry(peer_id)
            .or_default()
            .push(connection);

        if let Some(opts) = self.pending_selection.remove(&peer_id) {
            self.select(peer_id, opts);
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
            if let Some(connection) = connections
                .iter_mut()
                .find(|connection| connection.id == connection_id)
            {
                if connection.reserved.take().is_some() {
                    let addr = connection
                        .addr
                        .clone()
                        .with(Protocol::P2pCircuit)
                        .with(Protocol::P2p(self.local_peer_id));
                    self.events.push_back(ToSwarm::ExternalAddrExpired(addr));
                }
            }

            connections.retain(|connection| connection.id != connection_id);

            if connections.is_empty() {
                entry.remove();
            }
        }
    }

    pub fn process_relay_event(&mut self, event: libp2p::relay::client::Event) {
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
            FromSwarm::ListenerClosed(event) => self.on_close_listener(event),
            FromSwarm::ExternalAddrConfirmed(_) => {}
            FromSwarm::NewExternalAddrCandidate(_)
            | FromSwarm::ExpiredListenAddr(_)
            | FromSwarm::ListenerError(_)
            | FromSwarm::ExternalAddrExpired(_)
            | FromSwarm::AddressChange(_)
            | FromSwarm::DialFailure(_)
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
                        connection.confirmed = true;
                        if let Some(opts) = self.pending_selection.remove(&peer_id) {
                            self.select(peer_id, opts);
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
                        connection.confirmed = false;
                        self.pending_selection.remove(&peer_id);
                    }
                }
            }
        }
    }

    fn poll(
        &mut self,
        _: &mut Context<'_>,
        _: &mut impl PollParameters,
    ) -> Poll<ToSwarm<Self::ToSwarm, THandlerInEvent<Self>>> {
        if let Some(event) = self.events.pop_front() {
            return Poll::Ready(event);
        }

        Poll::Pending
    }
}
