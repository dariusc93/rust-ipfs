use anyhow::anyhow;
use futures::{
    channel::{
        mpsc::{Receiver, UnboundedSender},
        oneshot,
    },
    FutureExt,
};
use pollable_map::optional::Optional;

use crate::ConnectionEvents;
use crate::{p2p, p2p::MultiaddrExt, Channel};

use crate::repo::{Repo, RepoEvent};
use crate::{config::BOOTSTRAP_NODES, IpfsEvent};
use connexa::{
    behaviour,
    prelude::transport::{ConnectedPoint, Endpoint},
};
use ipld_core::cid::Cid;
use std::collections::{hash_map::Entry, HashMap, HashSet};
use std::fmt::Debug;
use std::sync::Arc;

pub use crate::{p2p::BehaviourEvent, p2p::KadResult};

use multibase::Base;

use crate::repo::DefaultStorage;

use connexa::behaviour::{Behaviour as ConnexaBehaviour, BehaviourEvent as ConnexaBehaviourEvent};
use connexa::prelude::swarm::derive_prelude::ListenerId;
use connexa::prelude::swarm::{NetworkBehaviour, Swarm};
use connexa::prelude::{
    dht::{
        AddProviderError, AddProviderOk, BootstrapError, BootstrapOk, Event as KademliaEvent,
        GetClosestPeersError, GetClosestPeersOk, GetProvidersError, GetProvidersOk, GetRecordError,
        GetRecordOk, PutRecordError, PutRecordOk, QueryId, QueryResult::*, Record,
    },
    identify::Event as IdentifyEvent,
    rendezvous::{Cookie, Namespace},
    swarm::SwarmEvent,
};
use connexa::prelude::{Multiaddr, PeerId, Protocol};
use tokio::sync::Notify;

/// Background task of `Ipfs` created when calling `UninitializedIpfs::start`.
// The receivers are Fuse'd so that we don't have to manage state on them being exhausted.
#[allow(clippy::type_complexity)]
#[allow(dead_code)]
pub struct IpfsContext {
    pub repo_events: Optional<Receiver<RepoEvent>>,
    pub bitswap_cancellable: HashMap<Cid, Vec<Arc<Notify>>>,
    pub listening_addresses: HashMap<ListenerId, Vec<Multiaddr>>,
    pub provider_stream: HashMap<QueryId, UnboundedSender<PeerId>>,
    pub record_stream: HashMap<QueryId, UnboundedSender<Record>>,
    pub repo: Repo<DefaultStorage>,
    pub kad_subscriptions: HashMap<QueryId, Channel<KadResult>>,
    pub dht_peer_lookup: HashMap<PeerId, Vec<Channel<connexa::prelude::identify::Info>>>,
    pub bootstraps: HashSet<Multiaddr>,

    pub relay_listener: HashMap<PeerId, Vec<Channel<()>>>,
    pub rzv_register_pending: HashMap<(PeerId, Namespace), Vec<Channel<()>>>,
    pub rzv_discover_pending:
        HashMap<(PeerId, Namespace), Vec<Channel<HashMap<PeerId, Vec<Multiaddr>>>>>,
    pub rzv_cookie: HashMap<PeerId, Option<Cookie>>,

    pub connection_events: Vec<futures::channel::mpsc::Sender<ConnectionEvents>>,

    pub event_capacity: usize,
}

impl Default for IpfsContext {
    fn default() -> Self {
        Self {
            repo_events: Default::default(),
            bitswap_cancellable: Default::default(),
            listening_addresses: Default::default(),
            provider_stream: Default::default(),
            record_stream: Default::default(),
            repo: Repo::new_memory(),
            kad_subscriptions: Default::default(),
            dht_peer_lookup: Default::default(),
            bootstraps: Default::default(),
            relay_listener: Default::default(),
            rzv_register_pending: Default::default(),
            rzv_discover_pending: Default::default(),
            rzv_cookie: Default::default(),
            connection_events: vec![],
            event_capacity: 0,
        }
    }
}

impl IpfsContext {
    pub fn new(repo: &Repo<DefaultStorage>, event_capacity: usize) -> Self {
        Self {
            repo_events: Default::default(),
            event_capacity,
            provider_stream: HashMap::new(),
            record_stream: HashMap::new(),
            dht_peer_lookup: Default::default(),
            kad_subscriptions: Default::default(),
            bitswap_cancellable: Default::default(),
            repo: repo.clone(),
            bootstraps: Default::default(),
            relay_listener: Default::default(),
            rzv_register_pending: Default::default(),
            rzv_discover_pending: Default::default(),
            rzv_cookie: Default::default(),
            listening_addresses: HashMap::new(),
            connection_events: Vec::new(),
        }
    }
}

impl IpfsContext {
    // FIXME: Determine if we should restructure this for handling of swarm
    #[allow(dead_code)]
    fn handle_swarm_event<N: NetworkBehaviour + Send + Sync + 'static>(
        &mut self,
        swarm: &mut Swarm<ConnexaBehaviour<p2p::Behaviour<N>>>,
        swarm_event: SwarmEvent<ConnexaBehaviourEvent<p2p::Behaviour<N>>>,
        custom: &mut p2p::Behaviour<N>,
    ) where
        N::ToSwarm: Debug + Send + Sync + 'static,
    {
        match swarm_event {
            SwarmEvent::ConnectionEstablished {
                peer_id,
                connection_id,
                endpoint,
                ..
            } => {
                let (ep, mut addr) = match &endpoint {
                    ConnectedPoint::Dialer { address, .. } => (Endpoint::Dialer, address.clone()),
                    ConnectedPoint::Listener { local_addr, .. } if endpoint.is_relayed() => {
                        (Endpoint::Listener, local_addr.clone())
                    }
                    ConnectedPoint::Listener { send_back_addr, .. } => {
                        (Endpoint::Listener, send_back_addr.clone())
                    }
                };

                if matches!(addr.iter().last(), Some(Protocol::P2p(_))) {
                    addr.pop();
                }

                for ch in &mut self.connection_events {
                    let ev = match ep {
                        Endpoint::Dialer => ConnectionEvents::OutgoingConnection {
                            peer_id,
                            connection_id,
                            addr: addr.clone(),
                        },
                        Endpoint::Listener => ConnectionEvents::IncomingConnection {
                            peer_id,
                            connection_id,
                            addr: addr.clone(),
                        },
                    };

                    let _ = ch.try_send(ev);
                }
            }
            SwarmEvent::Behaviour(behaviour::BehaviourEvent::Kademlia(event)) => {
                match event {
                    KademliaEvent::InboundRequest { request } => {
                        trace!("kad: inbound {:?} request handled", request);
                    }
                    KademliaEvent::OutboundQueryProgressed {
                        result, id, step, ..
                    } => {
                        // make sure the query is exhausted

                        if swarm
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

                                if let Some(ret) = self.kad_subscriptions.remove(&id) {
                                    let _ = ret.send(Err(anyhow::anyhow!(
                                        "kad: timed out while trying to bootstrap"
                                    )));
                                }
                            }
                            GetClosestPeers(Ok(GetClosestPeersOk { key, peers })) => {
                                if let Some(ret) = self.kad_subscriptions.remove(&id) {
                                    let _ = ret.send(Ok(KadResult::Peers(
                                        peers.iter().map(|info| info.peer_id).collect(),
                                    )));
                                }
                                if let Ok(peer_id) = PeerId::from_bytes(&key) {
                                    if let Some(rets) = self.dht_peer_lookup.remove(&peer_id) {
                                        if !peers.iter().any(|info| info.peer_id == peer_id) {
                                            for ret in rets {
                                                let _ = ret.send(Err(anyhow::anyhow!(
                                                    "Could not locate peer"
                                                )));
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

                                if let Some(ret) = self.kad_subscriptions.remove(&id) {
                                    let _ = ret.send(Err(anyhow::anyhow!(
                                        "timed out while trying to get providers for the given key"
                                    )));
                                }
                            }
                            StartProviding(Ok(AddProviderOk { key })) => {
                                let key = multibase::encode(Base::Base32Lower, key);
                                debug!("kad: providing {}", key);
                            }
                            StartProviding(Err(AddProviderError::Timeout { key })) => {
                                let key = multibase::encode(Base::Base32Lower, key);
                                warn!("kad: timed out while trying to provide {}", key);

                                if let Some(ret) = self.kad_subscriptions.remove(&id) {
                                    let _ = ret.send(Err(anyhow::anyhow!(
                                        "kad: timed out while trying to provide the record"
                                    )));
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

                                if let Some(tx) = self.record_stream.remove(&id) {
                                    tx.close_channel();
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

                                if let Some(tx) = self.record_stream.remove(&id) {
                                    tx.close_channel();
                                }
                            }
                            GetRecord(Err(GetRecordError::Timeout { key })) => {
                                let key = multibase::encode(Base::Base32Lower, key);
                                warn!("kad: timed out while trying to get key {}", key);

                                if let Some(tx) = self.record_stream.remove(&id) {
                                    tx.close_channel();
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

                                if let Some(ret) = self.kad_subscriptions.remove(&id) {
                                    let _ = ret.send(Err(anyhow::anyhow!(
                                        "kad: quorum failed when trying to put the record"
                                    )));
                                }
                            }
                            PutRecord(Err(PutRecordError::Timeout {
                                key,
                                success: _,
                                quorum: _,
                            })) => {
                                let key = multibase::encode(Base::Base32Lower, key);
                                warn!("kad: timed out while trying to put record {}", key);

                                if let Some(ret) = self.kad_subscriptions.remove(&id) {
                                    let _ = ret.send(Err(anyhow::anyhow!(
                                        "kad: timed out while trying to put record {}",
                                        key
                                    )));
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
                        let _ = new_mode;
                        trace!("kad: mode changed to {:?}", new_mode);
                    }
                }
            }
            SwarmEvent::Behaviour(behaviour::BehaviourEvent::Ping(event)) => match event {
                connexa::prelude::ping::Event {
                    peer,
                    connection,
                    result: Result::Ok(rtt),
                } => {
                    trace!(
                        "ping: rtt to {} is {} ms",
                        peer.to_base58(),
                        rtt.as_millis()
                    );

                    if let Some(m) = custom.relay_manager.as_mut() {
                        m.set_peer_rtt(peer, connection, rtt)
                    }
                }
                connexa::prelude::ping::Event { .. } => {
                    //TODO: Determine if we should continue handling ping errors and if we should disconnect/close connection.
                }
            },
            SwarmEvent::Behaviour(behaviour::BehaviourEvent::Custom(
                BehaviourEvent::RelayManager(event),
            )) => {
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
            SwarmEvent::Behaviour(behaviour::BehaviourEvent::Identify(event)) => match event {
                IdentifyEvent::Received { info, .. } => {
                    custom.peerbook.inject_peer_info(info);
                }
                event => debug!("identify: {:?}", event),
            },

            SwarmEvent::Behaviour(ConnexaBehaviourEvent::Custom(BehaviourEvent::Bitswap(
                event,
            ))) => match event {
                crate::p2p::bitswap::Event::NeedBlock { cid } => {
                    if let Some(kad) = swarm.behaviour_mut().kademlia.as_mut() {
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

    fn custom_behaviour<'a, N: NetworkBehaviour>(
        &self,
        swarm: &'a mut Swarm<ConnexaBehaviour<p2p::Behaviour<N>>>,
    ) -> &'a mut p2p::Behaviour<N>
    where
        N::ToSwarm: Debug,
    {
        swarm
            .behaviour_mut()
            .custom
            .as_mut()
            .expect("behaviour enabled")
    }

    pub(crate) fn handle_event<N: NetworkBehaviour>(
        &mut self,
        swarm: &mut Swarm<ConnexaBehaviour<p2p::Behaviour<N>>>,
        event: IpfsEvent,
    ) where
        N::ToSwarm: Debug,
    {
        match event {
            IpfsEvent::AddPeer(opt, ret) => {
                if let Some(kad) = swarm.behaviour_mut().kademlia.as_mut() {
                    let peer_id = opt.peer_id();
                    let addrs = opt.addresses().to_vec();
                    for addr in addrs {
                        kad.add_address(peer_id, addr);
                    }
                }
                let result = match self.custom_behaviour(swarm).add_peer(opt) {
                    true => Ok(()),
                    false => Err(anyhow::anyhow!("unable to add peer")),
                };

                let _ = ret.send(result);
            }
            IpfsEvent::RemovePeer(peer_id, addr, ret) => {
                let custom_behaviour = self.custom_behaviour(swarm);
                let result = match addr {
                    Some(addr) => Ok(custom_behaviour.addressbook.remove_address(&peer_id, &addr)),
                    None => Ok(custom_behaviour.addressbook.remove_peer(&peer_id)),
                };

                let _ = ret.send(result);
            }
            IpfsEvent::Protocol(ret) => {
                let info = self.custom_behaviour(swarm).supported_protocols();
                let _ = ret.send(info);
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
            IpfsEvent::ConnectionEvents(ret) => {
                let (tx, rx) = futures::channel::mpsc::channel(self.event_capacity);
                self.connection_events.push(tx);
                let _ = ret.send(Ok(rx));
            }
            IpfsEvent::WantList(peer, ret) => {
                let Some(bitswap) = self.custom_behaviour(swarm).bitswap.as_ref() else {
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
                let locally_known = self.custom_behaviour(swarm).peerbook.get_peer_info(peer_id);

                let (tx, rx) = oneshot::channel();

                match locally_known {
                    Some(info) => {
                        let _ = tx.send(Ok(info.clone()));
                    }
                    None => {
                        let Some(kad) = swarm.behaviour_mut().kademlia.as_mut() else {
                            let _ = ret.send(Err(anyhow!("kad protocol is disabled")));
                            return;
                        };

                        kad.get_closest_peers(peer_id);

                        self.dht_peer_lookup.entry(peer_id).or_default().push(tx);
                    }
                }

                let _ = ret.send(Ok(rx));
            }
            IpfsEvent::GetBootstrappers(ret) => {
                let list = Vec::from_iter(self.bootstraps.iter().cloned());
                let _ = ret.send(list);
            }
            IpfsEvent::AddBootstrapper(mut addr, ret) => {
                let Some(kad) = swarm.behaviour_mut().kademlia.as_mut() else {
                    let _ = ret.send(Err(anyhow!("kad protocol is disabled")));
                    return;
                };

                let ret_addr = addr.clone();

                if self.bootstraps.insert(addr.clone()) {
                    if let Some(peer_id) = addr.extract_peer_id() {
                        kad.add_address(&peer_id, addr.clone());
                        self.custom_behaviour(swarm).add_peer((peer_id, addr));
                        // the return value of add_address doesn't implement Debug
                        trace!(peer_id=%peer_id, "tried to add a bootstrapper");
                    }
                }
                let _ = ret.send(Ok(ret_addr));
            }
            IpfsEvent::RemoveBootstrapper(mut addr, ret) => {
                let Some(kad) = swarm.behaviour_mut().kademlia.as_mut() else {
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
                let Some(kad) = swarm.behaviour_mut().kademlia.as_mut() else {
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
                if !swarm.behaviour().kademlia.is_enabled() {
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

                        if self
                            .custom_behaviour(swarm)
                            .add_peer((peer_id, addr.clone()))
                        {
                            trace!(peer_id=%peer_id, "tried to restore a bootstrapper");
                            // report with the peerid
                            rets.push(original);
                        }

                        let kad = swarm
                            .behaviour_mut()
                            .kademlia
                            .as_mut()
                            .expect("kad enabled");

                        kad.add_address(&peer_id, addr.clone());
                    }
                }

                let _ = ret.send(Ok(rets));
            }
            IpfsEvent::AddRelay(peer_id, addr, tx) => {
                let Some(relay) = self.custom_behaviour(swarm).relay_manager.as_mut() else {
                    let _ = tx.send(Err(anyhow::anyhow!("Relay is not enabled")));
                    return;
                };

                relay.add_address(peer_id, addr);

                let _ = tx.send(Ok(()));
            }
            IpfsEvent::RemoveRelay(peer_id, addr, tx) => {
                let Some(relay) = self.custom_behaviour(swarm).relay_manager.as_mut() else {
                    let _ = tx.send(Err(anyhow::anyhow!("Relay is not enabled")));
                    return;
                };

                relay.remove_address(peer_id, addr);

                let _ = tx.send(Ok(()));
            }
            IpfsEvent::EnableRelay(Some(peer_id), tx) => {
                let Some(relay) = self.custom_behaviour(swarm).relay_manager.as_mut() else {
                    let _ = tx.send(Err(anyhow::anyhow!("Relay is not enabled")));
                    return;
                };

                relay.select(peer_id);

                self.relay_listener.entry(peer_id).or_default().push(tx);
            }
            IpfsEvent::EnableRelay(None, tx) => {
                let Some(relay) = self.custom_behaviour(swarm).relay_manager.as_mut() else {
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
                let Some(relay) = self.custom_behaviour(swarm).relay_manager.as_mut() else {
                    let _ = tx.send(Err(anyhow::anyhow!("Relay is not enabled")));
                    return;
                };
                relay.disable_relay(peer_id);

                let _ = tx.send(Ok(()));
            }
            IpfsEvent::ListRelays(tx) => {
                let Some(relay) = self.custom_behaviour(swarm).relay_manager.as_ref() else {
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
                let Some(relay) = self.custom_behaviour(swarm).relay_manager.as_ref() else {
                    let _ = tx.send(Err(anyhow::anyhow!("Relay is not enabled")));
                    return;
                };

                let list = relay.list_active_relays();

                let _ = tx.send(Ok(list));
            }
        }
    }

    pub(crate) fn handle_repo_event<N: NetworkBehaviour>(
        &mut self,
        custom: &mut p2p::Behaviour<N>,
        event: RepoEvent,
    ) where
        N::ToSwarm: Debug,
    {
        match event {
            RepoEvent::WantBlock(cids, peers, timeout, signals) => {
                let Some(bs) = custom.bitswap.as_mut() else {
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
                let Some(bs) = custom.bitswap.as_mut() else {
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
                let Some(bs) = custom.bitswap.as_mut() else {
                    return;
                };
                bs.notify_new_blocks([*block.cid()]);
            }
            RepoEvent::RemovedBlock(_) => {}
        }
    }
}
