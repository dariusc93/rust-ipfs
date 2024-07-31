use anyhow::{anyhow, format_err};
use either::Either;
use futures::{
    channel::{
        mpsc::{unbounded, Receiver, UnboundedSender},
        oneshot,
    },
    stream::Fuse,
    FutureExt, StreamExt,
};

use crate::TSwarmEvent;
use crate::{p2p::MultiaddrExt, Channel, InnerPubsubEvent};

use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    time::Duration,
};

use crate::{config::BOOTSTRAP_NODES, IpfsEvent, TSwarmEventFn};
use futures_timer::Delay;
use ipld_core::cid::Cid;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::{
    p2p::TSwarm,
    repo::{Repo, RepoEvent},
    AddPeerOpt,
};

pub use crate::{p2p::BehaviourEvent, p2p::KadResult};

pub use libp2p::{self, core::transport::ListenerId, swarm::NetworkBehaviour, Multiaddr, PeerId};
use multibase::Base;

use libp2p::{
    autonat,
    identify::{Event as IdentifyEvent, Info as IdentifyInfo},
    kad::{
        AddProviderError, AddProviderOk, BootstrapError, BootstrapOk, Event as KademliaEvent,
        GetClosestPeersError, GetClosestPeersOk, GetProvidersError, GetProvidersOk, GetRecordError,
        GetRecordOk, PutRecordError, PutRecordOk, QueryId, QueryResult::*, Record,
    },
    rendezvous::{Cookie, Namespace},
    swarm::{ConnectionId, SwarmEvent},
};

#[cfg(not(target_arch = "wasm32"))]
use libp2p::mdns::Event as MdnsEvent;
use tokio::sync::Notify;

/// Background task of `Ipfs` created when calling `UninitializedIpfs::start`.
// The receivers are Fuse'd so that we don't have to manage state on them being exhausted.
#[allow(clippy::type_complexity)]
#[allow(dead_code)]
pub(crate) struct IpfsTask<C: NetworkBehaviour<ToSwarm = void::Void>> {
    pub(crate) swarm: TSwarm<C>,
    pub(crate) repo_events: Fuse<Receiver<RepoEvent>>,
    pub(crate) from_facade: Fuse<Receiver<IpfsEvent>>,
    pub(crate) bitswap_cancellable: HashMap<Cid, Vec<Arc<Notify>>>,
    pub(crate) listening_addresses: HashMap<ListenerId, Vec<Multiaddr>>,
    pub(crate) provider_stream: HashMap<QueryId, UnboundedSender<PeerId>>,
    pub(crate) record_stream: HashMap<QueryId, UnboundedSender<Record>>,
    pub(crate) repo: Repo,
    pub(crate) kad_subscriptions: HashMap<QueryId, Channel<KadResult>>,
    pub(crate) dht_peer_lookup: HashMap<PeerId, Vec<Channel<libp2p::identify::Info>>>,
    pub(crate) bootstraps: HashSet<Multiaddr>,
    pub(crate) swarm_event: Option<TSwarmEventFn<C>>,
    pub(crate) pubsub_event_stream: Vec<UnboundedSender<InnerPubsubEvent>>,
    pub(crate) timer: TaskTimer,
    pub(crate) local_external_addr: bool,
    pub(crate) relay_listener: HashMap<PeerId, Vec<Channel<()>>>,
    pub(crate) rzv_register_pending: HashMap<(PeerId, Namespace), Vec<Channel<()>>>,
    pub(crate) rzv_discover_pending:
        HashMap<(PeerId, Namespace), Vec<Channel<HashMap<PeerId, Vec<Multiaddr>>>>>,
    pub(crate) rzv_cookie: HashMap<PeerId, Option<Cookie>>,

    pub(crate) pending_connection: HashMap<ConnectionId, Channel<()>>,
    pub(crate) pending_disconnection: HashMap<PeerId, Vec<Channel<()>>>,
    pub(crate) pending_add_listener: HashMap<ListenerId, Channel<Multiaddr>>,
    pub(crate) pending_remove_listener: HashMap<ListenerId, Channel<()>>,
}

impl<C: NetworkBehaviour<ToSwarm = void::Void>> IpfsTask<C> {
    pub fn new(
        swarm: TSwarm<C>,
        repo_events: Fuse<Receiver<RepoEvent>>,
        from_facade: Fuse<Receiver<IpfsEvent>>,
        repo: &Repo,
    ) -> Self {
        IpfsTask {
            repo_events,
            from_facade,
            swarm,
            provider_stream: HashMap::new(),
            record_stream: HashMap::new(),
            dht_peer_lookup: Default::default(),
            pubsub_event_stream: Default::default(),
            kad_subscriptions: Default::default(),
            bitswap_cancellable: Default::default(),
            repo: repo.clone(),
            bootstraps: Default::default(),
            swarm_event: Default::default(),
            timer: Default::default(),
            relay_listener: Default::default(),
            local_external_addr: false,
            rzv_register_pending: Default::default(),
            rzv_discover_pending: Default::default(),
            rzv_cookie: Default::default(),
            listening_addresses: HashMap::new(),
            pending_disconnection: Default::default(),
            pending_connection: Default::default(),
            pending_add_listener: Default::default(),
            pending_remove_listener: Default::default(),
        }
    }
}

pub(crate) struct TaskTimer {
    pub(crate) event_cleanup: Delay,
}

impl Default for TaskTimer {
    fn default() -> Self {
        let event_cleanup = Delay::new(Duration::from_secs(60));

        Self { event_cleanup }
    }
}

impl<C: NetworkBehaviour<ToSwarm = void::Void>> futures::Future for IpfsTask<C> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            match self.swarm.poll_next_unpin(cx) {
                Poll::Ready(Some(event)) => self.handle_swarm_event(event),
                Poll::Ready(None) => return Poll::Ready(()),
                Poll::Pending => break,
            }
        }
        loop {
            match self.from_facade.poll_next_unpin(cx) {
                Poll::Ready(Some(event)) => self.handle_event(event),
                Poll::Ready(None) => return Poll::Ready(()),
                Poll::Pending => break,
            }
        }
        loop {
            match self.repo_events.poll_next_unpin(cx) {
                Poll::Ready(Some(event)) => self.handle_repo_event(event),
                Poll::Ready(None) => return Poll::Ready(()),
                Poll::Pending => break,
            }
        }

        if self.timer.event_cleanup.poll_unpin(cx).is_ready() {
            self.pubsub_event_stream.retain(|ch| !ch.is_closed());
            self.timer.event_cleanup.reset(Duration::from_secs(60));
        }

        Poll::Pending
    }
}

impl<C: NetworkBehaviour<ToSwarm = void::Void>> IpfsTask<C> {
    pub(crate) async fn run(&mut self) {
        let mut event_cleanup = futures_timer::Delay::new(Duration::from_secs(60));

        loop {
            tokio::select! {
                biased;
                Some(swarm) = self.swarm.next() => {
                    self.handle_swarm_event(swarm);
                },
                Some(repo) = self.repo_events.next() => {
                    self.handle_repo_event(repo);
                },
                Some(event) = self.from_facade.next() => {
                    if matches!(event, IpfsEvent::Exit) {
                        break;
                    }
                    self.handle_event(event);
                },
                _ = &mut event_cleanup => {
                    self.pubsub_event_stream.retain(|ch| !ch.is_closed());
                    event_cleanup.reset(Duration::from_secs(60));
                }
            }
        }
    }

    fn emit_pubsub_event(&self, event: InnerPubsubEvent) {
        for ch in &self.pubsub_event_stream {
            let event = event.clone();
            let _ = ch.unbounded_send(event);
        }
    }

    fn handle_swarm_event(&mut self, swarm_event: TSwarmEvent<C>) {
        if let Some(handler) = self.swarm_event.as_ref() {
            handler(&mut self.swarm, &swarm_event)
        }
        match swarm_event {
            SwarmEvent::NewListenAddr {
                listener_id,
                address,
            } => {
                if self.local_external_addr
                    && !address.is_relay()
                    && (address.is_loopback() || address.is_private())
                {
                    self.swarm.add_external_address(address.clone());
                }

                if !address.is_loopback() && !address.is_private() {
                    // We will assume that the address is global and reachable externally
                    self.swarm.add_external_address(address.clone());
                }

                self.listening_addresses
                    .entry(listener_id)
                    .or_default()
                    .push(address.clone());

                if let Some(ret) = self.pending_add_listener.remove(&listener_id) {
                    let _ = ret.send(Ok(address));
                }
            }
            SwarmEvent::ConnectionEstablished { connection_id, .. } => {
                if let Some(ch) = self.pending_connection.remove(&connection_id) {
                    _ = ch.send(Ok(()));
                }
            }
            SwarmEvent::OutgoingConnectionError {
                connection_id,
                error,
                ..
            } => {
                if let Some(ch) = self.pending_connection.remove(&connection_id) {
                    _ = ch.send(Err(anyhow::Error::from(error)));
                }
            }
            SwarmEvent::ConnectionClosed { peer_id, .. } => {
                if let Some(ch) = self.pending_disconnection.remove(&peer_id) {
                    for ch in ch {
                        let _ = ch.send(Ok(()));
                    }
                }
            }
            SwarmEvent::ExpiredListenAddr {
                listener_id,
                address,
            } => {
                if let Some(list) = self.listening_addresses.get_mut(&listener_id) {
                    list.retain(|addr| &address != addr);
                }

                self.swarm.remove_external_address(&address);
            }
            SwarmEvent::ListenerClosed {
                listener_id,
                reason,
                addresses,
            } => {
                for address in addresses {
                    self.listening_addresses.remove(&listener_id);
                    self.swarm.remove_external_address(&address);
                }

                if let Some(ret) = self.pending_remove_listener.remove(&listener_id) {
                    let _ = ret.send(reason.map_err(anyhow::Error::from));
                }
            }
            SwarmEvent::ListenerError { listener_id, error } => {
                if let Some(ret) = self.pending_add_listener.remove(&listener_id) {
                    let _ = ret.send(Err(error.into()));
                }
            }
            #[cfg(not(target_arch = "wasm32"))]
            SwarmEvent::Behaviour(BehaviourEvent::Mdns(event)) => match event {
                MdnsEvent::Discovered(list) => {
                    for (peer, addr) in list {
                        self.swarm.behaviour_mut().add_peer((peer, addr));
                    }
                }
                MdnsEvent::Expired(list) => {
                    for (peer, _) in list {
                        if let Some(mdns) = self.swarm.behaviour().mdns.as_ref() {
                            if !mdns.discovered_nodes().any(|p| p == &peer) {
                                trace!("mdns: Expired peer {}", peer.to_base58());
                            }
                        }
                    }
                }
            },
            SwarmEvent::Behaviour(BehaviourEvent::Kademlia(event)) => {
                match event {
                    KademliaEvent::InboundRequest { request } => {
                        trace!("kad: inbound {:?} request handled", request);
                    }
                    KademliaEvent::OutboundQueryProgressed {
                        result, id, step, ..
                    } => {
                        // make sure the query is exhausted

                        if self
                            .swarm
                            .behaviour()
                            .kademlia
                            .as_ref()
                            .and_then(|kad| kad.query(&id))
                            .is_none()
                        {
                            match result {
                                // these subscriptions return actual values
                                GetClosestPeers(_) | GetProviders(_) | GetRecord(_) => {}
                                // we want to return specific errors for the following
                                Bootstrap(Err(_)) | StartProviding(Err(_)) | PutRecord(Err(_)) => {}
                                // and the rest can just return a general KadResult::Complete
                                _ => {
                                    if let Some(ret) = self.kad_subscriptions.remove(&id) {
                                        let _ = ret.send(Ok(KadResult::Complete));
                                    }
                                }
                            }
                        }

                        match result {
                            Bootstrap(Ok(BootstrapOk {
                                peer,
                                num_remaining,
                            })) => {
                                debug!(
                                    "kad: bootstrapped with {}, {} peers remain",
                                    peer, num_remaining
                                );
                            }
                            Bootstrap(Err(BootstrapError::Timeout { .. })) => {
                                warn!("kad: timed out while trying to bootstrap");

                                if self
                                    .swarm
                                    .behaviour()
                                    .kademlia
                                    .as_ref()
                                    .and_then(|kad| kad.query(&id))
                                    .is_none()
                                {
                                    if let Some(ret) = self.kad_subscriptions.remove(&id) {
                                        let _ = ret.send(Err(anyhow::anyhow!(
                                            "kad: timed out while trying to bootstrap"
                                        )));
                                    }
                                }
                            }
                            GetClosestPeers(Ok(GetClosestPeersOk { key, peers })) => {
                                if self
                                    .swarm
                                    .behaviour()
                                    .kademlia
                                    .as_ref()
                                    .and_then(|kad| kad.query(&id))
                                    .is_none()
                                {
                                    if let Some(ret) = self.kad_subscriptions.remove(&id) {
                                        let _ = ret.send(Ok(KadResult::Peers(peers.clone())));
                                    }
                                    if let Ok(peer_id) = PeerId::from_bytes(&key) {
                                        if let Some(rets) = self.dht_peer_lookup.remove(&peer_id) {
                                            if !peers.contains(&peer_id) {
                                                for ret in rets {
                                                    let _ = ret.send(Err(anyhow::anyhow!(
                                                        "Could not locate peer"
                                                    )));
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            GetClosestPeers(Err(GetClosestPeersError::Timeout {
                                key,
                                peers: _,
                            })) => {
                                // don't mention the key here, as this is just the id of our node
                                warn!("kad: timed out while trying to find all closest peers");

                                if self
                                    .swarm
                                    .behaviour()
                                    .kademlia
                                    .as_ref()
                                    .and_then(|kad| kad.query(&id))
                                    .is_none()
                                {
                                    if let Some(ret) = self.kad_subscriptions.remove(&id) {
                                        let _ = ret.send(Err(anyhow::anyhow!(
                                            "timed out while trying to find all closest peers"
                                        )));
                                    }
                                    if let Ok(peer_id) = PeerId::from_bytes(&key) {
                                        if let Some(rets) = self.dht_peer_lookup.remove(&peer_id) {
                                            for ret in rets {
                                                let _ = ret.send(Err(anyhow::anyhow!(
                                                    "timed out while trying to find all closest peers"
                                                )));
                                            }
                                        }
                                    }
                                }
                            }
                            GetProviders(Ok(GetProvidersOk::FoundProviders {
                                key: _,
                                providers,
                            })) => {
                                if let Entry::Occupied(entry) = self.provider_stream.entry(id) {
                                    let tx = entry.get();
                                    for provider in providers {
                                        let _ = tx.unbounded_send(provider);
                                    }
                                }
                            }
                            GetProviders(Ok(GetProvidersOk::FinishedWithNoAdditionalRecord {
                                ..
                            })) => {
                                if step.last {
                                    if let Some(tx) = self.provider_stream.remove(&id) {
                                        tx.close_channel();
                                    }
                                }
                            }
                            GetProviders(Err(GetProvidersError::Timeout { key, .. })) => {
                                let key = multibase::encode(Base::Base32Lower, key);
                                warn!("kad: timed out while trying to get providers for {}", key);

                                if self
                                    .swarm
                                    .behaviour()
                                    .kademlia
                                    .as_ref()
                                    .and_then(|kad| kad.query(&id))
                                    .is_none()
                                {
                                    if let Some(ret) = self.kad_subscriptions.remove(&id) {
                                        let _ = ret.send(Err(anyhow::anyhow!("timed out while trying to get providers for the given key")));
                                    }
                                }
                            }
                            StartProviding(Ok(AddProviderOk { key })) => {
                                let key = multibase::encode(Base::Base32Lower, key);
                                debug!("kad: providing {}", key);
                            }
                            StartProviding(Err(AddProviderError::Timeout { key })) => {
                                let key = multibase::encode(Base::Base32Lower, key);
                                warn!("kad: timed out while trying to provide {}", key);

                                if self
                                    .swarm
                                    .behaviour()
                                    .kademlia
                                    .as_ref()
                                    .and_then(|kad| kad.query(&id))
                                    .is_none()
                                {
                                    if let Some(ret) = self.kad_subscriptions.remove(&id) {
                                        let _ = ret.send(Err(anyhow::anyhow!(
                                            "kad: timed out while trying to provide the record"
                                        )));
                                    }
                                }
                            }
                            RepublishProvider(Ok(AddProviderOk { key })) => {
                                let key = multibase::encode(Base::Base32Lower, key);
                                debug!("kad: republished provider {}", key);
                            }
                            RepublishProvider(Err(AddProviderError::Timeout { key })) => {
                                let key = multibase::encode(Base::Base32Lower, key);
                                warn!("kad: timed out while trying to republish provider {}", key);
                            }
                            GetRecord(Ok(GetRecordOk::FoundRecord(record))) => {
                                if let Entry::Occupied(entry) = self.record_stream.entry(id) {
                                    let _ = entry.get().unbounded_send(record.record);
                                }
                            }
                            GetRecord(Ok(GetRecordOk::FinishedWithNoAdditionalRecord {
                                ..
                            })) => {
                                if step.last {
                                    if let Some(tx) = self.record_stream.remove(&id) {
                                        tx.close_channel();
                                    }
                                }
                            }
                            GetRecord(Err(GetRecordError::NotFound {
                                key,
                                closest_peers: _,
                            })) => {
                                let key = multibase::encode(Base::Base32Lower, key);
                                warn!("kad: couldn't find record {}", key);

                                if self
                                    .swarm
                                    .behaviour()
                                    .kademlia
                                    .as_ref()
                                    .and_then(|kad| kad.query(&id))
                                    .is_none()
                                {
                                    if let Some(tx) = self.record_stream.remove(&id) {
                                        tx.close_channel();
                                    }
                                }
                            }
                            GetRecord(Err(GetRecordError::QuorumFailed {
                                key,
                                records: _,
                                quorum,
                            })) => {
                                let key = multibase::encode(Base::Base32Lower, key);
                                warn!(
                                    "kad: quorum failed {} when trying to get key {}",
                                    quorum, key
                                );

                                if self
                                    .swarm
                                    .behaviour()
                                    .kademlia
                                    .as_ref()
                                    .and_then(|kad| kad.query(&id))
                                    .is_none()
                                {
                                    if let Some(tx) = self.record_stream.remove(&id) {
                                        tx.close_channel();
                                    }
                                }
                            }
                            GetRecord(Err(GetRecordError::Timeout { key })) => {
                                let key = multibase::encode(Base::Base32Lower, key);
                                warn!("kad: timed out while trying to get key {}", key);

                                if self
                                    .swarm
                                    .behaviour()
                                    .kademlia
                                    .as_ref()
                                    .and_then(|kad| kad.query(&id))
                                    .is_none()
                                {
                                    if let Some(tx) = self.record_stream.remove(&id) {
                                        tx.close_channel();
                                    }
                                }
                            }
                            PutRecord(Ok(PutRecordOk { key }))
                            | RepublishRecord(Ok(PutRecordOk { key })) => {
                                let key = multibase::encode(Base::Base32Lower, key);
                                debug!("kad: successfully put record {}", key);
                            }
                            PutRecord(Err(PutRecordError::QuorumFailed {
                                key,
                                success: _,
                                quorum,
                            }))
                            | RepublishRecord(Err(PutRecordError::QuorumFailed {
                                key,
                                success: _,
                                quorum,
                            })) => {
                                let key = multibase::encode(Base::Base32Lower, key);
                                warn!(
                                    "kad: quorum failed ({}) when trying to put record {}",
                                    quorum, key
                                );

                                if self
                                    .swarm
                                    .behaviour()
                                    .kademlia
                                    .as_ref()
                                    .and_then(|kad| kad.query(&id))
                                    .is_none()
                                {
                                    if let Some(ret) = self.kad_subscriptions.remove(&id) {
                                        let _ = ret.send(Err(anyhow::anyhow!(
                                            "kad: quorum failed when trying to put the record"
                                        )));
                                    }
                                }
                            }
                            PutRecord(Err(PutRecordError::Timeout {
                                key,
                                success: _,
                                quorum: _,
                            })) => {
                                let key = multibase::encode(Base::Base32Lower, key);
                                warn!("kad: timed out while trying to put record {}", key);

                                if self
                                    .swarm
                                    .behaviour()
                                    .kademlia
                                    .as_ref()
                                    .and_then(|kad| kad.query(&id))
                                    .is_none()
                                {
                                    if let Some(ret) = self.kad_subscriptions.remove(&id) {
                                        let _ = ret.send(Err(anyhow::anyhow!(
                                            "kad: timed out while trying to put record {}",
                                            key
                                        )));
                                    }
                                }
                            }
                            RepublishRecord(Err(PutRecordError::Timeout {
                                key,
                                success: _,
                                quorum: _,
                            })) => {
                                let key = multibase::encode(Base::Base32Lower, key);
                                warn!("kad: timed out while trying to republish record {}", key);
                            }
                        }
                    }
                    KademliaEvent::RoutingUpdated {
                        peer,
                        is_new_peer: _,
                        addresses,
                        bucket_range: _,
                        old_peer: _,
                    } => {
                        trace!("kad: routing updated; {}: {:?}", peer, addresses);
                    }
                    KademliaEvent::UnroutablePeer { peer } => {
                        trace!("kad: peer {} is unroutable", peer);
                    }
                    KademliaEvent::RoutablePeer { peer, address } => {
                        trace!("kad: peer {} ({}) is routable", peer, address);
                    }
                    KademliaEvent::PendingRoutablePeer { peer, address } => {
                        trace!("kad: pending routable peer {} ({})", peer, address);
                    }
                    KademliaEvent::ModeChanged { new_mode } => {
                        _ = new_mode;
                    }
                }
            }
            SwarmEvent::Behaviour(BehaviourEvent::Pubsub(
                libp2p::gossipsub::Event::Subscribed { peer_id, topic },
            )) => self.emit_pubsub_event(InnerPubsubEvent::Subscribe {
                topic: topic.to_string(),
                peer_id,
            }),
            SwarmEvent::Behaviour(BehaviourEvent::Pubsub(
                libp2p::gossipsub::Event::Unsubscribed { peer_id, topic },
            )) => self.emit_pubsub_event(InnerPubsubEvent::Unsubscribe {
                topic: topic.to_string(),
                peer_id,
            }),
            SwarmEvent::Behaviour(BehaviourEvent::Ping(event)) => match event {
                libp2p::ping::Event {
                    peer,
                    connection,
                    result: Result::Ok(rtt),
                } => {
                    trace!(
                        "ping: rtt to {} is {} ms",
                        peer.to_base58(),
                        rtt.as_millis()
                    );
                    self.swarm.behaviour_mut().peerbook.set_peer_rtt(peer, rtt);

                    if let Some(m) = self.swarm.behaviour_mut().relay_manager.as_mut() {
                        m.set_peer_rtt(peer, connection, rtt)
                    }
                }
                libp2p::ping::Event { .. } => {
                    //TODO: Determine if we should continue handling ping errors and if we should disconnect/close connection.
                }
            },
            SwarmEvent::Behaviour(BehaviourEvent::RelayClient(event)) => {
                debug!("Relay Client Event: {event:?}");
                if let Some(m) = self.swarm.behaviour_mut().relay_manager.as_mut() {
                    m.process_relay_event(event);
                }
            }
            SwarmEvent::Behaviour(BehaviourEvent::RelayManager(event)) => {
                debug!("Relay Manager Event: {event:?}");
                match event {
                    libp2p_relay_manager::Event::ReservationSuccessful { peer_id, .. } => {
                        if let Some(chs) = self.relay_listener.remove(&peer_id) {
                            for ch in chs {
                                let _ = ch.send(Ok(()));
                            }
                        }
                    }
                    libp2p_relay_manager::Event::ReservationClosed { peer_id, result } => {
                        if let Some(chs) = self.relay_listener.remove(&peer_id) {
                            match result {
                                Ok(()) => {
                                    for ch in chs {
                                        let _ = ch.send(Ok(()));
                                    }
                                }
                                Err(e) => {
                                    let e = e.to_string();
                                    for ch in chs {
                                        let _ = ch.send(Err(anyhow::anyhow!("{}", e.clone())));
                                    }
                                }
                            }
                        }
                    }
                    libp2p_relay_manager::Event::ReservationFailure {
                        peer_id,
                        result: err,
                    } => {
                        if let Some(chs) = self.relay_listener.remove(&peer_id) {
                            let e = err.to_string();
                            for ch in chs {
                                let _ = ch.send(Err(anyhow::anyhow!("{}", e.clone())));
                            }
                        }
                    }
                    _ => {}
                }
            }

            SwarmEvent::Behaviour(BehaviourEvent::Identify(event)) => match event {
                IdentifyEvent::Received { peer_id, info } => {
                    let IdentifyInfo {
                        listen_addrs,
                        protocols,
                        ..
                    } = &info;

                    if let Some(kad) = self.swarm.behaviour_mut().kademlia.as_mut() {
                        if protocols.iter().any(|p| libp2p::kad::PROTOCOL_NAME.eq(p)) {
                            for addr in listen_addrs {
                                kad.add_address(&peer_id, addr.clone());
                            }
                        }
                    }

                    if protocols
                        .iter()
                        .any(|p| libp2p::autonat::DEFAULT_PROTOCOL_NAME.eq(p))
                    {
                        if let Some(autonat) = self.swarm.behaviour_mut().autonat.as_mut() {
                            for addr in listen_addrs {
                                autonat.add_server(peer_id, Some(addr.clone()));
                            }
                        }
                    }

                    if let Some(rets) = self.dht_peer_lookup.remove(&peer_id) {
                        for ret in rets {
                            let _ = ret.send(Ok(info.clone()));
                        }
                    }

                    self.swarm.behaviour_mut().peerbook.inject_peer_info(info);
                }
                event => debug!("identify: {:?}", event),
            },
            SwarmEvent::Behaviour(BehaviourEvent::Autonat(autonat::Event::StatusChanged {
                old,
                new,
            })) => {
                //TODO: Use status to indicate if we should use a relay or not
                debug!("Old Nat Status: {:?}", old);
                debug!("New Nat Status: {:?}", new);
            }
            SwarmEvent::Behaviour(BehaviourEvent::RendezvousClient(
                libp2p::rendezvous::client::Event::Discovered {
                    rendezvous_node,
                    registrations,
                    cookie,
                },
            )) => {
                self.rzv_cookie.insert(rendezvous_node, Some(cookie));
                let mut ns_list = HashSet::new();
                let addrbook = &mut self.swarm.behaviour_mut().addressbook;
                let mut ns_book: HashMap<Namespace, HashMap<PeerId, Vec<Multiaddr>>> =
                    HashMap::new();
                for registration in registrations {
                    let namespace = registration.namespace.clone();
                    let peer_id = registration.record.peer_id();
                    let addrs = registration.record.addresses();

                    // info!("Discovered {peer_id} with address {addr} in {namespace}");

                    let opts = AddPeerOpt::with_peer_id(peer_id).set_addresses(addrs.to_vec());

                    addrbook.add_address(opts);

                    ns_book
                        .entry(namespace.clone())
                        .or_default()
                        .entry(peer_id)
                        .or_default()
                        .extend(addrs.to_vec());
                    ns_list.insert(namespace);
                }

                for ns in ns_list {
                    let map = ns_book.remove(&ns).unwrap_or_default();
                    if let Some(channels) = self.rzv_discover_pending.remove(&(rendezvous_node, ns))
                    {
                        for ch in channels {
                            let _ = ch.send(Ok(map.clone()));
                        }
                    }
                }
            }
            SwarmEvent::Behaviour(BehaviourEvent::RendezvousClient(
                libp2p::rendezvous::client::Event::DiscoverFailed {
                    rendezvous_node,
                    namespace,
                    error,
                },
            )) => {
                let Some(ns) = namespace else {
                    error!("Error registering to {rendezvous_node}: {error:?}");
                    return;
                };

                error!("Error registering namespace {ns} to {rendezvous_node}: {error:?}");

                if let Some(channels) = self.rzv_discover_pending.remove(&(rendezvous_node, ns)) {
                    for ch in channels {
                        let _ = ch.send(Err(anyhow::anyhow!(
                            "Error discovering peers on {rendezvous_node}: {error:?}"
                        )));
                    }
                }
            }
            SwarmEvent::Behaviour(BehaviourEvent::RendezvousClient(
                libp2p::rendezvous::client::Event::Registered {
                    rendezvous_node,
                    ttl,
                    namespace,
                },
            )) => {
                info!("Registered to {rendezvous_node} under {namespace} for {ttl} secs");

                if let Some(channels) = self
                    .rzv_register_pending
                    .remove(&(rendezvous_node, namespace.clone()))
                {
                    for ch in channels {
                        let _ = ch.send(Ok(()));
                    }
                }
            }
            SwarmEvent::Behaviour(BehaviourEvent::RendezvousClient(
                libp2p::rendezvous::client::Event::RegisterFailed {
                    rendezvous_node,
                    namespace,
                    error,
                },
            )) => {
                error!("Error registering namespace {namespace} to {rendezvous_node}: {error:?}");

                if let Some(channels) = self
                    .rzv_register_pending
                    .remove(&(rendezvous_node, namespace.clone()))
                {
                    for ch in channels {
                        let _ = ch.send(Err(anyhow::anyhow!("Error registering namespace {namespace} to {rendezvous_node}: {error:?}")));
                    }
                }
            }
            SwarmEvent::Behaviour(BehaviourEvent::Bitswap(event)) => match event {
                crate::p2p::bitswap::Event::NeedBlock { cid } => {
                    if let Some(kad) = self.swarm.behaviour_mut().kademlia.as_mut() {
                        info!("Looking for providers for {cid}");
                        let key = cid.hash().to_bytes();
                        kad.get_providers(key.into());
                    }
                }
                crate::p2p::bitswap::Event::CancelBlock { cid } => {
                    info!(%cid, "block request cancelled");
                    if let Some(list) = self.bitswap_cancellable.remove(&cid) {
                        for signal in list {
                            signal.notify_waiters();
                        }
                    }
                }
                crate::p2p::bitswap::Event::BlockRetrieved { cid } => {
                    info!(%cid, "block retrieved")
                }
            },
            _ => debug!("Swarm event: {:?}", swarm_event),
        }
    }

    fn handle_event(&mut self, event: IpfsEvent) {
        match event {
            IpfsEvent::Connect(target, ret) => {
                let connection_id = target.connection_id();

                if let Err(e) = self.swarm.dial(target) {
                    _ = ret.send(Err(anyhow::Error::from(e)));
                    return;
                }
                self.pending_connection.insert(connection_id, ret);
            }
            IpfsEvent::Protocol(ret) => {
                let info = self.swarm.behaviour().supported_protocols();
                let _ = ret.send(info);
            }
            #[cfg(feature = "experimental_stream")]
            IpfsEvent::StreamControlHandle(ret) => {
                let Some(stream) = self.swarm.behaviour_mut().stream.as_ref() else {
                    let _ = ret.send(Err(anyhow!("stream protocol is disabled")));
                    return;
                };

                _ = ret.send(Ok(stream.new_control()))
            }
            #[cfg(feature = "experimental_stream")]
            IpfsEvent::NewStream(protocol, ret) => {
                let Some(stream) = self.swarm.behaviour_mut().stream.as_ref() else {
                    let _ = ret.send(Err(anyhow!("stream protocol is disabled")));
                    return;
                };

                _ = ret.send(
                    stream
                        .new_control()
                        .accept(protocol)
                        .map_err(anyhow::Error::from),
                )
            }
            IpfsEvent::Addresses(ret) => {
                let addrs = self.swarm.behaviour_mut().addrs();
                ret.send(Ok(addrs)).ok();
            }
            IpfsEvent::Listeners(ret) => {
                let listeners = self.swarm.listeners().cloned().collect::<Vec<Multiaddr>>();
                ret.send(Ok(listeners)).ok();
            }
            IpfsEvent::ExternalAddresses(ret) => {
                let external = self
                    .swarm
                    .external_addresses()
                    .cloned()
                    .collect::<Vec<Multiaddr>>();

                ret.send(Ok(external)).ok();
            }
            IpfsEvent::IsConnected(peer_id, ret) => {
                let connected = self.swarm.is_connected(&peer_id);
                ret.send(Ok(connected)).ok();
            }
            IpfsEvent::Connected(ret) => {
                let connections = self.swarm.connected_peers().copied();
                ret.send(Ok(connections.collect())).ok();
            }
            IpfsEvent::Disconnect(peer, ret) => {
                if self.swarm.disconnect_peer_id(peer).is_err() {
                    _ = ret.send(Err(anyhow::anyhow!("Peer is not connected")));
                    return;
                }

                self.pending_disconnection
                    .entry(peer)
                    .or_default()
                    .push(ret);
            }
            IpfsEvent::Ban(peer, ret) => {
                self.swarm.behaviour_mut().block_list.block_peer(peer);
                let _ = ret.send(Ok(()));
            }
            IpfsEvent::Unban(peer, ret) => {
                self.swarm.behaviour_mut().block_list.unblock_peer(peer);
                let _ = ret.send(Ok(()));
            }
            IpfsEvent::PubsubSubscribe(topic, ret) => {
                let Some(pubsub) = self.swarm.behaviour_mut().pubsub.as_mut() else {
                    let _ = ret.send(Err(anyhow!("pubsub protocol is disabled")));
                    return;
                };

                let _ = ret.send(Ok(pubsub.subscribe(topic).ok()));
            }
            IpfsEvent::PubsubUnsubscribe(topic, ret) => {
                let Some(pubsub) = self.swarm.behaviour_mut().pubsub.as_mut() else {
                    let _ = ret.send(Err(anyhow!("pubsub protocol is disabled")));
                    return;
                };

                let _ = ret.send(Ok(pubsub.unsubscribe(topic)));
            }
            IpfsEvent::PubsubPublish(topic, data, ret) => {
                let Some(pubsub) = self.swarm.behaviour_mut().pubsub.as_mut() else {
                    let _ = ret.send(Err(anyhow!("pubsub protocol is disabled")));
                    return;
                };

                let _ = ret.send(Ok(pubsub.publish(topic, data)));
            }
            IpfsEvent::PubsubPeers(Some(topic), ret) => {
                let Some(pubsub) = self.swarm.behaviour_mut().pubsub.as_mut() else {
                    let _ = ret.send(Err(anyhow!("pubsub protocol is disabled")));
                    return;
                };

                let _ = ret.send(Ok(pubsub.subscribed_peers(topic)));
            }
            IpfsEvent::PubsubPeers(None, ret) => {
                let Some(pubsub) = self.swarm.behaviour_mut().pubsub.as_mut() else {
                    let _ = ret.send(Err(anyhow!("pubsub protocol is disabled")));
                    return;
                };

                let _ = ret.send(Ok(pubsub.known_peers()));
            }
            IpfsEvent::PubsubSubscribed(ret) => {
                let Some(pubsub) = self.swarm.behaviour_mut().pubsub.as_mut() else {
                    let _ = ret.send(Err(anyhow!("pubsub protocol is disabled")));
                    return;
                };
                let list = pubsub.topics().map(|t| t.to_string()).collect();
                let _ = ret.send(Ok(list));
            }
            // IpfsEvent::WantList(peer, ret) => {
            //     let list = if let Some(peer) = peer {
            //         self.swarm
            //             .behaviour_mut()
            //             .bitswap()
            //             .peer_wantlist(&peer)
            //             .unwrap_or_default()
            //     } else {
            //         self.swarm.behaviour_mut().bitswap().local_wantlist()
            //     };
            //     let _ = ret.send(list);
            // }
            // IpfsEvent::BitswapStats(ret) => {
            //     let stats = self.swarm.behaviour_mut().bitswap().stats();
            //     let peers = self.swarm.behaviour_mut().bitswap().peers();
            //     let wantlist = self.swarm.behaviour_mut().bitswap().local_wantlist();
            //     let _ = ret.send((stats, peers, wantlist).into());
            // }
            IpfsEvent::PubsubEventStream(ret) => {
                let (tx, rx) = unbounded();
                self.pubsub_event_stream.push(tx);
                let _ = ret.send(rx);
            }
            IpfsEvent::AddListeningAddress(addr, ret) => match self.swarm.listen_on(addr) {
                Ok(id) => {
                    self.pending_add_listener.insert(id, ret);
                }
                Err(e) => {
                    let _ = ret.send(Err(anyhow::anyhow!(e)));
                }
            },
            IpfsEvent::RemoveListeningAddress(addr, ret) => {
                let Some(listener_id) = self.listening_addresses.iter().find_map(|(id, list)| {
                    if list.contains(&addr) {
                        return Some(id);
                    }

                    None
                }) else {
                    let _ = ret.send(Err(format_err!(
                        "Address was not listened to before: {}",
                        addr
                    )));
                    return;
                };

                match self.swarm.remove_listener(*listener_id) {
                    true => {
                        self.pending_remove_listener.insert(*listener_id, ret);
                    }
                    false => {
                        let _ = ret.send(Err(anyhow::anyhow!(
                            "Failed to remove previously added listening address: {}",
                            addr
                        )));
                    }
                }
            }
            IpfsEvent::AddExternalAddress(addr, ret) => {
                self.swarm.add_external_address(addr);
                _ = ret.send(Ok(()))
            }
            IpfsEvent::RemoveExternalAddress(addr, ret) => {
                self.swarm.remove_external_address(&addr);
                _ = ret.send(Ok(()))
            }
            IpfsEvent::Bootstrap(ret) => {
                let Some(kad) = self.swarm.behaviour_mut().kademlia.as_mut() else {
                    let _ = ret.send(Err(anyhow!("kad protocol is disabled")));
                    return;
                };

                let future = match kad.bootstrap() {
                    Ok(id) => {
                        let (tx, rx) = oneshot::channel();
                        self.kad_subscriptions.insert(id, tx);
                        Ok(rx)
                    }
                    Err(e) => {
                        error!("kad: can't bootstrap the node: {:?}", e);
                        Err(anyhow!("kad: can't bootstrap the node: {:?}", e))
                    }
                };
                let _ = ret.send(future);
            }
            IpfsEvent::AddPeer(opt, ret) => {
                let result = match self.swarm.behaviour_mut().add_peer(opt) {
                    true => Ok(()),
                    false => Err(anyhow::anyhow!("unable to add peer")),
                };

                let _ = ret.send(result);
            }
            IpfsEvent::RemovePeer(peer_id, addr, ret) => {
                let result = match addr {
                    Some(addr) => Ok(self
                        .swarm
                        .behaviour_mut()
                        .addressbook
                        .remove_address(&peer_id, &addr)),
                    None => Ok(self.swarm.behaviour_mut().addressbook.remove_peer(&peer_id)),
                };

                let _ = ret.send(result);
            }
            IpfsEvent::GetClosestPeers(peer_id, ret) => {
                let Some(kad) = self.swarm.behaviour_mut().kademlia.as_mut() else {
                    let _ = ret.send(Err(anyhow!("kad protocol is disabled")));
                    return;
                };

                let id = kad.get_closest_peers(peer_id);

                let (tx, rx) = oneshot::channel();

                self.kad_subscriptions.insert(id, tx);
                let _ = ret.send(Ok(rx));
            }
            IpfsEvent::WantList(peer, ret) => {
                let Some(bitswap) = self.swarm.behaviour().bitswap.as_ref() else {
                    let _ = ret.send(Ok(futures::future::ready(vec![]).boxed()));
                    return;
                };
                let list = match peer {
                    Some(peer_id) => bitswap.peer_wantlist(peer_id),
                    None => bitswap.local_wantlist(),
                };
                let _ = ret.send(Ok(futures::future::ready(list).boxed()));
            }
            IpfsEvent::GetBitswapPeers(ret) => {
                let _ = ret.send(Ok(futures::future::ready(vec![]).boxed()));
            }
            IpfsEvent::FindPeerIdentity(peer_id, ret) => {
                let locally_known = self.swarm.behaviour().peerbook.get_peer_info(peer_id);

                let (tx, rx) = oneshot::channel();

                match locally_known {
                    Some(info) => {
                        let _ = tx.send(Ok(info.clone()));
                    }
                    None => {
                        let Some(kad) = self.swarm.behaviour_mut().kademlia.as_mut() else {
                            let _ = ret.send(Err(anyhow!("kad protocol is disabled")));
                            return;
                        };

                        kad.get_closest_peers(peer_id);

                        self.dht_peer_lookup.entry(peer_id).or_default().push(tx);
                    }
                }

                let _ = ret.send(Ok(rx));
            }
            IpfsEvent::FindPeer(peer_id, local_only, ret) => {
                let listener_addrs = self
                    .swarm
                    .behaviour_mut()
                    .peerbook
                    .peer_connections(peer_id)
                    .unwrap_or_default()
                    .into_iter()
                    .map(|mut addr| {
                        addr.extract_peer_id();
                        addr
                    })
                    .collect::<Vec<_>>();

                let locally_known_addrs = if !listener_addrs.is_empty() {
                    listener_addrs
                } else {
                    self.swarm
                        .behaviour()
                        .addressbook
                        .get_peer_addresses(&peer_id)
                        .unwrap_or_default()
                };

                let addrs = if !locally_known_addrs.is_empty() || local_only {
                    Either::Left(locally_known_addrs)
                } else {
                    let Some(kad) = self.swarm.behaviour_mut().kademlia.as_mut() else {
                        let _ = ret.send(Err(anyhow!("kad protocol is disabled")));
                        return;
                    };

                    Either::Right({
                        let id = kad.get_closest_peers(peer_id);

                        let (tx, rx) = oneshot::channel();
                        self.kad_subscriptions.insert(id, tx);

                        rx
                    })
                };
                let _ = ret.send(Ok(addrs));
            }
            IpfsEvent::GetProviders(key, ret) => {
                let Some(kad) = self.swarm.behaviour_mut().kademlia.as_mut() else {
                    let _ = ret.send(Err(anyhow!("kad protocol is disabled")));
                    return;
                };

                let id = kad.get_providers(key);

                let (tx, mut rx) = futures::channel::mpsc::unbounded();
                let stream = async_stream::stream! {
                    let mut current_providers: HashSet<PeerId> = Default::default();
                    while let Some(provider) = rx.next().await {
                        if current_providers.insert(provider) {
                            yield provider;
                        }
                    }
                };
                self.provider_stream.insert(id, tx);

                let _ = ret.send(Ok(Some(stream.boxed())));
            }
            IpfsEvent::Provide(key, ret) => {
                let Some(kad) = self.swarm.behaviour_mut().kademlia.as_mut() else {
                    let _ = ret.send(Err(anyhow!("kad protocol is disabled")));
                    return;
                };

                let future = match kad.start_providing(key) {
                    Ok(id) => {
                        let (tx, rx) = oneshot::channel();
                        self.kad_subscriptions.insert(id, tx);
                        Ok(rx)
                    }
                    Err(e) => {
                        error!("kad: can't provide a key: {:?}", e);
                        Err(anyhow!("kad: can't provide the key: {:?}", e))
                    }
                };
                let _ = ret.send(future);
            }
            IpfsEvent::DhtMode(mode, ret) => {
                let Some(kad) = self.swarm.behaviour_mut().kademlia.as_mut() else {
                    let _ = ret.send(Err(anyhow!("kad protocol is disabled")));
                    return;
                };

                kad.set_mode(mode.into());

                let _ = ret.send(Ok(()));
            }
            IpfsEvent::DhtGet(key, ret) => {
                let Some(kad) = self.swarm.behaviour_mut().kademlia.as_mut() else {
                    let _ = ret.send(Err(anyhow!("kad protocol is disabled")));
                    return;
                };

                let id = kad.get_record(key);

                let (tx, mut rx) = futures::channel::mpsc::unbounded();
                let stream = async_stream::stream! {
                    while let Some(record) = rx.next().await {
                            yield record;
                    }
                };
                self.record_stream.insert(id, tx);

                let _ = ret.send(Ok(stream.boxed()));
            }
            IpfsEvent::DhtPut(key, value, quorum, ret) => {
                let Some(kad) = self.swarm.behaviour_mut().kademlia.as_mut() else {
                    let _ = ret.send(Err(anyhow!("kad protocol is disabled")));
                    return;
                };

                let record = Record {
                    key,
                    value,
                    publisher: None,
                    expires: None,
                };

                let future = match kad.put_record(record, quorum) {
                    Ok(id) => {
                        let (tx, rx) = oneshot::channel();
                        self.kad_subscriptions.insert(id, tx);
                        Ok(rx)
                    }
                    Err(e) => {
                        error!("kad: can't put a record: {:?}", e);
                        Err(anyhow!("kad: can't provide the record: {:?}", e))
                    }
                };

                let _ = ret.send(future);
            }
            IpfsEvent::GetBootstrappers(ret) => {
                let list = Vec::from_iter(self.bootstraps.iter().cloned());
                let _ = ret.send(list);
            }
            IpfsEvent::AddBootstrapper(mut addr, ret) => {
                if !self.swarm.behaviour().kademlia.is_enabled() {
                    let _ = ret.send(Err(anyhow!("kad protocol is disabled")));
                    return;
                };

                let ret_addr = addr.clone();

                if self.bootstraps.insert(addr.clone()) {
                    if let Some(peer_id) = addr.extract_peer_id() {
                        self.swarm.behaviour_mut().add_peer((peer_id, addr));
                        // the return value of add_address doesn't implement Debug
                        trace!(peer_id=%peer_id, "tried to add a bootstrapper");
                    }
                }
                let _ = ret.send(Ok(ret_addr));
            }
            IpfsEvent::RemoveBootstrapper(mut addr, ret) => {
                let Some(kad) = self.swarm.behaviour_mut().kademlia.as_mut() else {
                    let _ = ret.send(Err(anyhow!("kad protocol is disabled")));
                    return;
                };

                let result = addr.clone();

                if self.bootstraps.remove(&addr) {
                    if let Some(peer_id) = addr.extract_peer_id() {
                        let prefix: Multiaddr = addr;

                        if let Some(e) = kad.remove_address(&peer_id, &prefix) {
                            info!(peer_id=%peer_id, status=?e.status, "removed bootstrapper");
                        } else {
                            warn!(peer_id=%peer_id, "attempted to remove an unknown bootstrapper");
                        }
                    }

                    let _ = ret.send(Ok(result));
                }
            }
            IpfsEvent::ClearBootstrappers(ret) => {
                let Some(kad) = self.swarm.behaviour_mut().kademlia.as_mut() else {
                    let _ = ret.send(Err(anyhow!("kad protocol is disabled")));
                    return;
                };

                let removed = self.bootstraps.drain().collect::<Vec<_>>();
                let mut list = Vec::with_capacity(removed.len());

                for mut addr_with_peer_id in removed {
                    let priginal = addr_with_peer_id.clone();
                    let Some(peer_id) = addr_with_peer_id.extract_peer_id() else {
                        continue;
                    };
                    let prefix: Multiaddr = addr_with_peer_id;

                    if let Some(e) = kad.remove_address(&peer_id, &prefix) {
                        info!(peer_id=%peer_id, status=?e.status, "cleared bootstrapper");
                        list.push(priginal);
                    } else {
                        error!(peer_id=%peer_id, "attempted to clear an unknown bootstrapper");
                    }
                }

                let _ = ret.send(Ok(list));
            }
            IpfsEvent::DefaultBootstrap(ret) => {
                if !self.swarm.behaviour().kademlia.is_enabled() {
                    let _ = ret.send(Err(anyhow!("kad protocol is disabled")));
                    return;
                };

                let mut rets = Vec::new();
                for addr in BOOTSTRAP_NODES {
                    let mut addr = addr
                        .parse::<Multiaddr>()
                        .expect("see test bootstrap_nodes_are_multiaddr_with_peerid");
                    let original: Multiaddr = addr.clone();
                    if self.bootstraps.insert(addr.clone()) {
                        let Some(peer_id) = addr.extract_peer_id() else {
                            continue;
                        };

                        if self.swarm.behaviour_mut().add_peer((peer_id, addr.clone())) {
                            trace!(peer_id=%peer_id, "tried to restore a bootstrapper");
                            // report with the peerid
                            rets.push(original);
                        }
                    }
                }

                let _ = ret.send(Ok(rets));
            }
            IpfsEvent::AddRelay(peer_id, addr, tx) => {
                let Some(relay) = self.swarm.behaviour_mut().relay_manager.as_mut() else {
                    let _ = tx.send(Err(anyhow::anyhow!("Relay is not enabled")));
                    return;
                };

                relay.add_address(peer_id, addr);

                let _ = tx.send(Ok(()));
            }
            IpfsEvent::RemoveRelay(peer_id, addr, tx) => {
                let Some(relay) = self.swarm.behaviour_mut().relay_manager.as_mut() else {
                    let _ = tx.send(Err(anyhow::anyhow!("Relay is not enabled")));
                    return;
                };

                relay.remove_address(peer_id, addr);

                let _ = tx.send(Ok(()));
            }
            IpfsEvent::EnableRelay(Some(peer_id), tx) => {
                let Some(relay) = self.swarm.behaviour_mut().relay_manager.as_mut() else {
                    let _ = tx.send(Err(anyhow::anyhow!("Relay is not enabled")));
                    return;
                };

                relay.select(peer_id);

                self.relay_listener.entry(peer_id).or_default().push(tx);
            }
            IpfsEvent::EnableRelay(None, tx) => {
                let Some(relay) = self.swarm.behaviour_mut().relay_manager.as_mut() else {
                    let _ = tx.send(Err(anyhow::anyhow!("Relay is not enabled")));
                    return;
                };

                let Some(peer_id) = relay.random_select() else {
                    let _ = tx.send(Err(anyhow::anyhow!(
                        "No relay was selected or was unavailable"
                    )));
                    return;
                };

                self.relay_listener.entry(peer_id).or_default().push(tx);
            }
            IpfsEvent::DisableRelay(peer_id, tx) => {
                let Some(relay) = self.swarm.behaviour_mut().relay_manager.as_mut() else {
                    let _ = tx.send(Err(anyhow::anyhow!("Relay is not enabled")));
                    return;
                };
                relay.disable_relay(peer_id);

                let _ = tx.send(Ok(()));
            }
            IpfsEvent::ListRelays(tx) => {
                let Some(relay) = self.swarm.behaviour().relay_manager.as_ref() else {
                    let _ = tx.send(Err(anyhow::anyhow!("Relay is not enabled")));
                    return;
                };

                let list = relay
                    .list_relays()
                    .map(|(peer_id, addrs)| (*peer_id, addrs.clone()))
                    .collect();

                let _ = tx.send(Ok(list));
            }
            IpfsEvent::ListActiveRelays(tx) => {
                let Some(relay) = self.swarm.behaviour().relay_manager.as_ref() else {
                    let _ = tx.send(Err(anyhow::anyhow!("Relay is not enabled")));
                    return;
                };

                let list = relay.list_active_relays();

                let _ = tx.send(Ok(list));
            }
            IpfsEvent::RegisterRendezvousNamespace(ns, peer_id, ttl, res) => {
                let Some(rz) = self.swarm.behaviour_mut().rendezvous_client.as_mut() else {
                    let _ = res.send(Err(anyhow::anyhow!("Rendezvous client is not enabled")));
                    return;
                };

                if let Err(e) = rz.register(ns.clone(), peer_id, ttl) {
                    let _ = res.send(Err(anyhow::Error::from(e)));
                    return;
                }
                self.rzv_register_pending
                    .entry((peer_id, ns))
                    .or_default()
                    .push(res);
            }
            IpfsEvent::UnregisterRendezvousNamespace(ns, peer_id, res) => {
                let Some(rz) = self.swarm.behaviour_mut().rendezvous_client.as_mut() else {
                    let _ = res.send(Err(anyhow::anyhow!("Rendezvous client is not enabled")));
                    return;
                };

                rz.unregister(ns.clone(), peer_id);

                let _ = res.send(Ok(()));
            }
            IpfsEvent::RendezvousNamespaceDiscovery(ns, use_cookie, ttl, peer_id, res) => {
                let Some(rz) = self.swarm.behaviour_mut().rendezvous_client.as_mut() else {
                    let _ = res.send(Err(anyhow::anyhow!("Rendezvous client is not enabled")));
                    return;
                };

                let cookie = use_cookie
                    .then(|| self.rzv_cookie.get(&peer_id).cloned().flatten())
                    .flatten();

                rz.discover(ns.clone(), cookie, ttl, peer_id);

                match ns {
                    Some(ns) => self
                        .rzv_discover_pending
                        .entry((peer_id, ns))
                        .or_default()
                        .push(res),
                    None => {
                        let _ = res.send(Ok(HashMap::new()));
                    }
                }
            }
            IpfsEvent::Exit => {
                // FIXME: we could do a proper teardown
            }
        }
    }

    fn handle_repo_event(&mut self, event: RepoEvent) {
        match event {
            RepoEvent::WantBlock(cids, peers, timeout, signals) => {
                let Some(bs) = self.swarm.behaviour_mut().bitswap.as_mut() else {
                    return;
                };
                if let Some(signals) = signals {
                    for (cid, signals) in signals {
                        if signals.is_empty() {
                            continue;
                        }

                        let entries = self.bitswap_cancellable.entry(cid).or_default();
                        entries.extend(signals);
                    }
                }
                bs.gets(cids, &peers, timeout);
            }
            RepoEvent::UnwantBlock(cid) => {
                let Some(bs) = self.swarm.behaviour_mut().bitswap.as_mut() else {
                    return;
                };
                bs.cancel(cid);
                if let Some(list) = self.bitswap_cancellable.remove(&cid) {
                    for signal in list {
                        signal.notify_waiters();
                    }
                }
            }
            RepoEvent::NewBlock(block) => {
                let Some(bs) = self.swarm.behaviour_mut().bitswap.as_mut() else {
                    return;
                };
                bs.notify_new_blocks([*block.cid()]);
            }
            RepoEvent::RemovedBlock(_) => {}
        }
    }
}
