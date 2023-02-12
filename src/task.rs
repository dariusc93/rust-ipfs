use anyhow::{anyhow, format_err};
use either::Either;
use futures::{
    channel::mpsc::{Receiver, UnboundedSender},
    sink::SinkExt,
    stream::Fuse,
    StreamExt,
};

use crate::subscription::SubscriptionRegistry;
use crate::{
    p2p::{ProviderStream, RecordStream},
    TSwarmEvent,
};
use ipfs_bitswap::BitswapEvent;
use tokio::sync::Notify;

use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use crate::{config::BOOTSTRAP_NODES, repo::BlockPut, IpfsEvent, IpfsTypes, TSwarmEventFn};

use crate::{
    p2p::TSwarm,
    repo::{Repo, RepoEvent},
};

pub use crate::{
    error::Error,
    p2p::BehaviourEvent,
    p2p::{Connection, KadResult, MultiaddrWithPeerId, MultiaddrWithoutPeerId},
    path::IpfsPath,
    repo::{PinKind, PinMode, RepoTypes},
};

use libipld::multibase::{self, Base};
pub use libp2p::{
    self,
    core::transport::ListenerId,
    gossipsub::{error::PublishError, MessageId},
    identity::Keypair,
    identity::PublicKey,
    kad::{record::Key, Quorum},
    multiaddr::multiaddr,
    multiaddr::Protocol,
    swarm::NetworkBehaviour,
    Multiaddr, PeerId,
};

use libp2p::{
    autonat,
    identify::{Event as IdentifyEvent, Info as IdentifyInfo},
    kad::{
        AddProviderError, AddProviderOk, BootstrapError, BootstrapOk, GetClosestPeersError,
        GetClosestPeersOk, GetProvidersError, GetProvidersOk, GetRecordError, GetRecordOk,
        KademliaEvent::*, PutRecordError, PutRecordOk, QueryId, QueryResult::*, Record,
    },
    mdns::Event as MdnsEvent,
    ping::Success as PingSuccess,
    swarm::SwarmEvent,
};

/// Background task of `Ipfs` created when calling `UninitializedIpfs::start`.
// The receivers are Fuse'd so that we don't have to manage state on them being exhausted.
#[allow(clippy::type_complexity)]
pub(crate) struct IpfsTask<Types: IpfsTypes> {
    pub(crate) swarm: TSwarm,
    pub(crate) repo_events: Fuse<Receiver<RepoEvent>>,
    pub(crate) from_facade: Fuse<Receiver<IpfsEvent>>,
    pub(crate) listening_addresses: HashMap<Multiaddr, ListenerId>,
    pub(crate) listeners: HashSet<ListenerId>,
    pub(crate) provider_stream: HashMap<QueryId, UnboundedSender<PeerId>>,
    pub(crate) record_stream: HashMap<QueryId, UnboundedSender<Record>>,
    pub(crate) repo: Arc<Repo<Types>>,
    pub(crate) kad_subscriptions: SubscriptionRegistry<KadResult, String>,
    pub(crate) listener_subscriptions: SubscriptionRegistry<Option<Option<Multiaddr>>, String>,
    pub(crate) autonat_limit: Arc<AtomicU64>,
    pub(crate) autonat_counter: Arc<AtomicU64>,
    pub(crate) bootstraps: HashSet<MultiaddrWithPeerId>,
    pub(crate) swarm_event: Option<TSwarmEventFn>,
}

impl<TRepoTypes: RepoTypes> IpfsTask<TRepoTypes> {
    pub(crate) async fn run(&mut self, notify: Arc<Notify>) {
        let mut first_run = false;
        loop {
            tokio::select! {
                swarm = self.swarm.next() => {
                    if let Some(swarm) = swarm {
                        self.handle_swarm_event(swarm).await;
                    }
                },
                event = self.from_facade.next() => {
                    if let Some(event) = event {
                        if matches!(event, IpfsEvent::Exit) {
                            break;
                        }
                        self.handle_event(event).await;
                    }
                },
                repo = self.repo_events.next() => {
                    if let Some(repo) = repo {
                        self.handle_repo_event(repo).await;
                    }
                }
            }
            if !first_run {
                first_run = true;
                notify.notify_one();
            }
        }
    }

    async fn handle_swarm_event(&mut self, swarm_event: TSwarmEvent) {
        if let Some(handler) = self.swarm_event.clone() {
            handler(&mut self.swarm, &swarm_event)
        }
        match swarm_event {
            SwarmEvent::NewListenAddr {
                listener_id,
                address,
            } => {
                self.listening_addresses
                    .insert(address.clone(), listener_id);

                self.listener_subscriptions
                    .finish_subscription(listener_id.into(), Ok(Some(Some(address))));
            }
            SwarmEvent::ExpiredListenAddr {
                listener_id,
                address,
            } => {
                self.listeners.remove(&listener_id);
                self.listening_addresses.remove(&address);
                self.listener_subscriptions
                    .finish_subscription(listener_id.into(), Ok(Some(None)));
            }
            SwarmEvent::ListenerClosed {
                listener_id,
                reason,
                addresses,
            } => {
                self.listeners.remove(&listener_id);
                for address in addresses {
                    self.listening_addresses.remove(&address);
                }
                let reason = reason.map(|_| Some(None)).map_err(|e| e.to_string());
                self.listener_subscriptions
                    .finish_subscription(listener_id.into(), reason);
            }
            SwarmEvent::ListenerError { listener_id, error } => {
                self.listeners.remove(&listener_id);
                self.listener_subscriptions
                    .finish_subscription(listener_id.into(), Err(error.to_string()));
            }
            SwarmEvent::Behaviour(BehaviourEvent::Mdns(event)) => match event {
                MdnsEvent::Discovered(list) => {
                    for (peer, addr) in list {
                        trace!("mdns: Discovered peer {}", peer.to_base58());
                        self.swarm.behaviour_mut().add_peer(peer, Some(addr));
                    }
                }
                MdnsEvent::Expired(list) => {
                    for (peer, _) in list {
                        if let Some(mdns) = self.swarm.behaviour().mdns.as_ref() {
                            if !mdns.has_node(&peer) {
                                trace!("mdns: Expired peer {}", peer.to_base58());
                                self.swarm.behaviour_mut().remove_peer(&peer);
                            }
                        }
                    }
                }
            },
            SwarmEvent::Behaviour(BehaviourEvent::Kad(event)) => {
                match event {
                    InboundRequest { request } => {
                        trace!("kad: inbound {:?} request handled", request);
                    }
                    OutboundQueryProgressed {
                        result, id, step, ..
                    } => {
                        // make sure the query is exhausted
                        if self.swarm.behaviour().kademlia.query(&id).is_none() {
                            match result {
                                // these subscriptions return actual values
                                GetClosestPeers(_) | GetProviders(_) | GetRecord(_) => {}
                                // we want to return specific errors for the following
                                Bootstrap(Err(_)) | StartProviding(Err(_)) | PutRecord(Err(_)) => {}
                                // and the rest can just return a general KadResult::Complete
                                _ => {
                                    self.kad_subscriptions
                                        .finish_subscription(id.into(), Ok(KadResult::Complete));
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

                                if self.swarm.behaviour().kademlia.query(&id).is_none() {
                                    self.kad_subscriptions.finish_subscription(
                                        id.into(),
                                        Err("kad: timed out while trying to bootstrap".into()),
                                    );
                                }
                            }
                            GetClosestPeers(Ok(GetClosestPeersOk { key: _, peers })) => {
                                if self.swarm.behaviour().kademlia.query(&id).is_none() {
                                    self.kad_subscriptions.finish_subscription(
                                        id.into(),
                                        Ok(KadResult::Peers(peers)),
                                    );
                                }
                            }
                            GetClosestPeers(Err(GetClosestPeersError::Timeout {
                                key: _,
                                peers: _,
                            })) => {
                                // don't mention the key here, as this is just the id of our node
                                warn!("kad: timed out while trying to find all closest peers");

                                if self.swarm.behaviour().kademlia.query(&id).is_none() {
                                    self.kad_subscriptions.finish_subscription(
                        id.into(),
                        Err("timed out while trying to get providers for the given key"
                            .into()),
                    );
                                }
                            }
                            GetProviders(Ok(GetProvidersOk::FoundProviders {
                                key: _,
                                providers,
                            })) => {
                                if let Entry::Occupied(entry) = self.provider_stream.entry(id) {
                                    if !providers.is_empty() {
                                        tokio::spawn({
                                            let mut tx = entry.get().clone();
                                            async move {
                                                for provider in providers {
                                                    let _ = tx.send(provider).await;
                                                }
                                            }
                                        });
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

                                if self.swarm.behaviour().kademlia.query(&id).is_none() {
                                    self.kad_subscriptions.finish_subscription(id.into(),Err("timed out while trying to get providers for the given key".into()));
                                }
                            }
                            StartProviding(Ok(AddProviderOk { key })) => {
                                let key = multibase::encode(Base::Base32Lower, key);
                                debug!("kad: providing {}", key);
                            }
                            StartProviding(Err(AddProviderError::Timeout { key })) => {
                                let key = multibase::encode(Base::Base32Lower, key);
                                warn!("kad: timed out while trying to provide {}", key);

                                if self.swarm.behaviour().kademlia.query(&id).is_none() {
                                    self.kad_subscriptions.finish_subscription(
                                        id.into(),
                                        Err("kad: timed out while trying to provide the record"
                                            .into()),
                                    );
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
                                    tokio::spawn({
                                        let mut tx = entry.get().clone();
                                        async move {
                                            let _ = tx.send(record.record).await;
                                        }
                                    });
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

                                if self.swarm.behaviour().kademlia.query(&id).is_none() {
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

                                if self.swarm.behaviour().kademlia.query(&id).is_none() {
                                    if let Some(tx) = self.record_stream.remove(&id) {
                                        tx.close_channel();
                                    }
                                }
                            }
                            GetRecord(Err(GetRecordError::Timeout { key })) => {
                                let key = multibase::encode(Base::Base32Lower, key);
                                warn!("kad: timed out while trying to get key {}", key);

                                if self.swarm.behaviour().kademlia.query(&id).is_none() {
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

                                if self.swarm.behaviour().kademlia.query(&id).is_none() {
                                    self.kad_subscriptions.finish_subscription(
                                        id.into(),
                                        Err("kad: quorum failed when trying to put the record"
                                            .into()),
                                    );
                                }
                            }
                            PutRecord(Err(PutRecordError::Timeout {
                                key,
                                success: _,
                                quorum: _,
                            })) => {
                                let key = multibase::encode(Base::Base32Lower, key);
                                warn!("kad: timed out while trying to put record {}", key);

                                if self.swarm.behaviour().kademlia.query(&id).is_none() {
                                    self.kad_subscriptions.finish_subscription(
                                        id.into(),
                                        Err("kad: timed out while trying to put the record".into()),
                                    );
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
                    RoutingUpdated {
                        peer,
                        is_new_peer: _,
                        addresses,
                        bucket_range: _,
                        old_peer: _,
                    } => {
                        trace!("kad: routing updated; {}: {:?}", peer, addresses);
                    }
                    UnroutablePeer { peer } => {
                        trace!("kad: peer {} is unroutable", peer);
                    }
                    RoutablePeer { peer, address } => {
                        trace!("kad: peer {} ({}) is routable", peer, address);
                    }
                    PendingRoutablePeer { peer, address } => {
                        trace!("kad: pending routable peer {} ({})", peer, address);
                    }
                }
            }
            SwarmEvent::Behaviour(BehaviourEvent::Bitswap(event)) => match event {
                BitswapEvent::ReceivedBlock(peer_id, block) => {
                    let repo = self.repo.clone();
                    let peer_stats =
                        Arc::clone(self.swarm.behaviour().bitswap.stats.get(&peer_id).unwrap());
                    tokio::task::spawn(async move {
                        let bytes = block.data().len() as u64;
                        let res = repo.put_block(block.clone()).await;
                        match res {
                            Ok((_, uniqueness)) => match uniqueness {
                                BlockPut::NewBlock => peer_stats.update_incoming_unique(bytes),
                                BlockPut::Existed => peer_stats.update_incoming_duplicate(bytes),
                            },
                            Err(e) => {
                                debug!(
                                    "Got block {} from peer {} but failed to store it: {}",
                                    block.cid(),
                                    peer_id.to_base58(),
                                    e
                                );
                            }
                        };
                    });
                }
                BitswapEvent::ReceivedWant(peer_id, cid, priority) => {
                    info!(
                        "Peer {} wants block {} with priority {}",
                        peer_id, cid, priority
                    );

                    let queued_blocks = self.swarm.behaviour_mut().bitswap().queued_blocks.clone();
                    let repo = self.repo.clone();

                    tokio::task::spawn(async move {
                        match repo.get_block_now(&cid).await {
                            Ok(Some(block)) => {
                                let _ = queued_blocks.unbounded_send((peer_id, block));
                            }
                            Ok(None) => {}
                            Err(err) => {
                                warn!(
                                    "Peer {} wanted block {} but we failed: {}",
                                    peer_id.to_base58(),
                                    cid,
                                    err,
                                );
                            }
                        }
                    });
                }
                BitswapEvent::ReceivedCancel(..) => {}
            },
            SwarmEvent::Behaviour(BehaviourEvent::Ping(event)) => match event {
                libp2p::ping::Event {
                    peer,
                    result: Result::Ok(PingSuccess::Ping { rtt }),
                } => {
                    trace!(
                        "ping: rtt to {} is {} ms",
                        peer.to_base58(),
                        rtt.as_millis()
                    );
                    self.swarm.behaviour_mut().swarm.set_rtt(&peer, rtt);
                }
                libp2p::ping::Event {
                    peer,
                    result: Result::Ok(PingSuccess::Pong),
                } => {
                    trace!("ping: pong from {}", peer);
                }
                libp2p::ping::Event {
                    peer,
                    result: Result::Err(libp2p::ping::Failure::Timeout),
                } => {
                    trace!("ping: timeout to {}", peer);
                    self.swarm.behaviour_mut().remove_peer(&peer);
                }
                libp2p::ping::Event {
                    peer,
                    result: Result::Err(libp2p::ping::Failure::Other { error }),
                } => {
                    error!("ping: failure with {}: {}", peer.to_base58(), error);
                }
                libp2p::ping::Event {
                    peer,
                    result: Result::Err(libp2p::ping::Failure::Unsupported),
                } => {
                    error!("ping: failure with {}: unsupported", peer.to_base58());
                }
            },
            SwarmEvent::Behaviour(BehaviourEvent::Identify(event)) => match event {
                IdentifyEvent::Received { peer_id, info } => {
                    self.swarm
                        .behaviour_mut()
                        .swarm
                        .inject_identify_info(peer_id, info.clone());

                    let IdentifyInfo {
                        listen_addrs,
                        protocols,
                        ..
                    } = info;

                    if protocols
                        .iter()
                        .any(|p| p.as_bytes() == libp2p::kad::protocol::DEFAULT_PROTO_NAME)
                    {
                        for addr in &listen_addrs {
                            self.swarm
                                .behaviour_mut()
                                .kademlia()
                                .add_address(&peer_id, addr.clone());
                        }
                    }

                    #[allow(clippy::collapsible_if)]
                    if protocols
                        .iter()
                        .any(|p| p.as_bytes() == libp2p::autonat::DEFAULT_PROTOCOL_NAME)
                    {
                        if self.autonat_counter.load(Ordering::Relaxed)
                            <= self.autonat_limit.load(Ordering::Relaxed)
                        {
                            for addr in listen_addrs {
                                self.swarm
                                    .behaviour_mut()
                                    .autonat
                                    .add_server(peer_id, Some(addr));
                            }

                            let mut counter = self.autonat_counter.load(Ordering::Relaxed);
                            counter += 1;
                            self.autonat_counter.store(counter, Ordering::Relaxed);
                        }
                    }
                }
                event => trace!("identify: {:?}", event),
            },
            SwarmEvent::Behaviour(BehaviourEvent::Autonat(autonat::Event::StatusChanged {
                old,
                new,
            })) => {
                //TODO: Use status to indicate if we should use a relay or not
                debug!("Old Nat Status: {:?}", old);
                debug!("New Nat Status: {:?}", new);
            }
            _ => trace!("Swarm event: {:?}", swarm_event),
        }
    }

    async fn handle_event(&mut self, event: IpfsEvent) {
        match event {
            IpfsEvent::Connect(target, ret) => {
                ret.send(self.swarm.behaviour_mut().connect(target)).ok();
            }
            IpfsEvent::Protocol(ret) => {
                let info = self.swarm.behaviour().supported_protocols();
                let _ = ret.send(info);
            }
            IpfsEvent::SwarmDial(opt, ret) => {
                let result = self.swarm.dial(opt);
                let _ = ret.send(result);
            }
            IpfsEvent::Addresses(ret) => {
                let addrs = self.swarm.behaviour_mut().addrs();
                ret.send(Ok(addrs)).ok();
            }
            IpfsEvent::Listeners(ret) => {
                let listeners = self.swarm.listeners().cloned().collect::<Vec<Multiaddr>>();
                ret.send(Ok(listeners)).ok();
            }
            IpfsEvent::Connections(ret) => {
                let connections = self.swarm.behaviour_mut().connections();
                ret.send(Ok(connections.collect())).ok();
            }
            IpfsEvent::IsConnected(peer_id, ret) => {
                let connected = self.swarm.is_connected(&peer_id);
                ret.send(Ok(connected)).ok();
            }
            IpfsEvent::Connected(ret) => {
                let connections = self.swarm.connected_peers().cloned();
                ret.send(Ok(connections.collect())).ok();
            }
            IpfsEvent::Disconnect(peer, ret) => {
                let _ = ret.send(
                    self.swarm
                        .disconnect_peer_id(peer)
                        .map_err(|_| anyhow::anyhow!("Peer was not connected")),
                );
            }
            IpfsEvent::Ban(peer, ret) => {
                self.swarm.ban_peer_id(peer);
                let _ = ret.send(Ok(()));
            }
            IpfsEvent::Unban(peer, ret) => {
                self.swarm.unban_peer_id(peer);
                let _ = ret.send(Ok(()));
            }
            IpfsEvent::GetAddresses(ret) => {
                // perhaps this could be moved under `IpfsEvent` or free functions?
                let mut addresses = Vec::new();
                addresses.extend(self.swarm.listeners().map(|a| a.to_owned()));
                addresses.extend(self.swarm.external_addresses().map(|ar| ar.addr.to_owned()));
                // ignore error, perhaps caller went away already
                let _ = ret.send(addresses);
            }
            IpfsEvent::PubsubSubscribe(topic, ret) => {
                let pubsub = self.swarm.behaviour_mut().pubsub().subscribe(topic).ok();
                let _ = ret.send(pubsub);
            }
            IpfsEvent::PubsubUnsubscribe(topic, ret) => {
                let _ = ret.send(self.swarm.behaviour_mut().pubsub().unsubscribe(topic));
            }
            IpfsEvent::PubsubPublish(topic, data, ret) => {
                let _ = ret.send(self.swarm.behaviour_mut().pubsub().publish(topic, data));
            }
            IpfsEvent::PubsubPeers(Some(topic), ret) => {
                let _ = ret.send(self.swarm.behaviour_mut().pubsub().subscribed_peers(&topic));
            }
            IpfsEvent::PubsubPeers(None, ret) => {
                let _ = ret.send(self.swarm.behaviour_mut().pubsub().known_peers());
            }
            IpfsEvent::PubsubSubscribed(ret) => {
                let _ = ret.send(self.swarm.behaviour_mut().pubsub().subscribed_topics());
            }
            IpfsEvent::WantList(peer, ret) => {
                let list = if let Some(peer) = peer {
                    self.swarm
                        .behaviour_mut()
                        .bitswap()
                        .peer_wantlist(&peer)
                        .unwrap_or_default()
                } else {
                    self.swarm.behaviour_mut().bitswap().local_wantlist()
                };
                let _ = ret.send(list);
            }
            IpfsEvent::BitswapStats(ret) => {
                let stats = self.swarm.behaviour_mut().bitswap().stats();
                let peers = self.swarm.behaviour_mut().bitswap().peers();
                let wantlist = self.swarm.behaviour_mut().bitswap().local_wantlist();
                let _ = ret.send((stats, peers, wantlist).into());
            }
            IpfsEvent::AddListeningAddress(addr, ret) => match self.swarm.listen_on(addr) {
                Ok(id) => {
                    self.listeners.insert(id);
                    let fut = self
                        .listener_subscriptions
                        .create_subscription(id.into(), None);
                    let _ = ret.send(Ok(fut));
                }
                Err(e) => {
                    let _ = ret.send(Err(anyhow::anyhow!(e)));
                }
            },
            IpfsEvent::RemoveListeningAddress(addr, ret) => {
                let removed = if let Some(id) = self.listening_addresses.remove(&addr) {
                    if !self.swarm.remove_listener(id) {
                        Err(format_err!(
                            "Failed to remove previously added listening address: {}",
                            addr
                        ))
                    } else {
                        self.listeners.remove(&id);
                        let fut = self
                            .listener_subscriptions
                            .create_subscription(id.into(), None);
                        Ok(fut)
                    }
                } else {
                    Err(format_err!("Address was not listened to before: {}", addr))
                };

                let _ = ret.send(removed);
            }
            IpfsEvent::Bootstrap(ret) => {
                let future = match self.swarm.behaviour_mut().kademlia.bootstrap() {
                    Ok(id) => Ok(self.kad_subscriptions.create_subscription(id.into(), None)),
                    Err(e) => {
                        error!("kad: can't bootstrap the node: {:?}", e);
                        Err(anyhow!("kad: can't bootstrap the node: {:?}", e))
                    }
                };
                let _ = ret.send(future);
            }
            IpfsEvent::AddPeer(peer_id, addr) => {
                self.swarm.behaviour_mut().add_peer(peer_id, addr);
            }
            IpfsEvent::GetClosestPeers(peer_id, ret) => {
                let id = self
                    .swarm
                    .behaviour_mut()
                    .kademlia
                    .get_closest_peers(peer_id);

                let future = self.kad_subscriptions.create_subscription(id.into(), None);

                let _ = ret.send(future);
            }
            IpfsEvent::GetBitswapPeers(ret) => {
                let peers = self
                    .swarm
                    .behaviour_mut()
                    .bitswap()
                    .connected_peers
                    .keys()
                    .cloned()
                    .collect();
                let _ = ret.send(peers);
            }
            IpfsEvent::FindPeerIdentity(peer_id, local_only, ret) => {
                let locally_known = self
                    .swarm
                    .behaviour()
                    .swarm
                    .peers()
                    .find(|(k, _)| peer_id.eq(k))
                    .and_then(|(_, v)| v.clone())
                    .map(|v| v.into());

                let addrs = if locally_known.is_some() || local_only {
                    Either::Left(locally_known)
                } else {
                    Either::Right({
                        let id = self
                            .swarm
                            .behaviour_mut()
                            .kademlia
                            .get_closest_peers(peer_id);

                        self.kad_subscriptions.create_subscription(id.into(), None)
                    })
                };
                let _ = ret.send(addrs);
            }
            IpfsEvent::FindPeer(peer_id, local_only, ret) => {
                let swarm_addrs = self.swarm.behaviour_mut().swarm.connections_to(&peer_id);
                let locally_known_addrs = if !swarm_addrs.is_empty() {
                    swarm_addrs
                } else {
                    self.swarm
                        .behaviour_mut()
                        .kademlia()
                        .addresses_of_peer(&peer_id)
                };
                let addrs = if !locally_known_addrs.is_empty() || local_only {
                    Either::Left(locally_known_addrs)
                } else {
                    Either::Right({
                        let id = self
                            .swarm
                            .behaviour_mut()
                            .kademlia
                            .get_closest_peers(peer_id);

                        self.kad_subscriptions.create_subscription(id.into(), None)
                    })
                };
                let _ = ret.send(addrs);
            }
            IpfsEvent::GetProviders(cid, ret) => {
                let key = Key::from(cid.hash().to_bytes());
                let id = self.swarm.behaviour_mut().kademlia.get_providers(key);
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

                let _ = ret.send(Some(ProviderStream(stream.boxed())));
            }
            IpfsEvent::Provide(cid, ret) => {
                let key = Key::from(cid.hash().to_bytes());
                let future = match self.swarm.behaviour_mut().kademlia.start_providing(key) {
                    Ok(id) => Ok(self.kad_subscriptions.create_subscription(id.into(), None)),
                    Err(e) => {
                        error!("kad: can't provide a key: {:?}", e);
                        Err(anyhow!("kad: can't provide the key: {:?}", e))
                    }
                };
                let _ = ret.send(future);
            }
            IpfsEvent::DhtGet(key, ret) => {
                let id = self.swarm.behaviour_mut().kademlia.get_record(key);
                let (tx, mut rx) = futures::channel::mpsc::unbounded();
                let stream = async_stream::stream! {
                    while let Some(record) = rx.next().await {
                            yield record;
                    }
                };
                self.record_stream.insert(id, tx);

                let _ = ret.send(RecordStream(stream.boxed()));
            }
            IpfsEvent::DhtPut(key, value, quorum, ret) => {
                let record = Record {
                    key,
                    value,
                    publisher: None,
                    expires: None,
                };
                let future = match self
                    .swarm
                    .behaviour_mut()
                    .kademlia
                    .put_record(record, quorum)
                {
                    Ok(id) => Ok(self.kad_subscriptions.create_subscription(id.into(), None)),
                    Err(e) => {
                        error!("kad: can't put a record: {:?}", e);
                        Err(anyhow!("kad: can't provide the record: {:?}", e))
                    }
                };
                let _ = ret.send(future);
            }
            IpfsEvent::GetBootstrappers(ret) => {
                let list = self
                    .bootstraps
                    .iter()
                    .map(MultiaddrWithPeerId::clone)
                    .map(Multiaddr::from)
                    .collect::<Vec<_>>();
                let _ = ret.send(list);
            }
            IpfsEvent::AddBootstrapper(addr, ret) => {
                let ret_addr = addr.clone().into();
                if self.bootstraps.insert(addr.clone()) {
                    let MultiaddrWithPeerId {
                        multiaddr: ma,
                        peer_id,
                    } = addr;

                    self.swarm
                        .behaviour_mut()
                        .kademlia
                        .add_address(&peer_id, ma.into());
                    // the return value of add_address doesn't implement Debug
                    trace!(peer_id=%peer_id, "tried to add a bootstrapper");
                }
                let _ = ret.send(Ok(ret_addr));
            }
            IpfsEvent::RemoveBootstrapper(addr, ret) => {
                let result = addr.clone().into();
                if self.bootstraps.remove(&addr) {
                    let peer_id = addr.peer_id;
                    let prefix: Multiaddr = addr.multiaddr.into();

                    if let Some(e) = self
                        .swarm
                        .behaviour_mut()
                        .kademlia
                        .remove_address(&peer_id, &prefix)
                    {
                        info!(peer_id=%peer_id, status=?e.status, "removed bootstrapper");
                    } else {
                        warn!(peer_id=%peer_id, "attempted to remove an unknown bootstrapper");
                    }
                }
                let _ = ret.send(Ok(result));
            }
            IpfsEvent::ClearBootstrappers(ret) => {
                let removed = self.bootstraps.drain().collect::<Vec<_>>();
                let mut list = Vec::with_capacity(removed.len());
                for addr_with_peer_id in removed {
                    let peer_id = &addr_with_peer_id.peer_id;
                    let prefix: Multiaddr = addr_with_peer_id.multiaddr.clone().into();

                    if let Some(e) = self
                        .swarm
                        .behaviour_mut()
                        .kademlia
                        .remove_address(peer_id, &prefix)
                    {
                        info!(peer_id=%peer_id, status=?e.status, "cleared bootstrapper");
                        list.push(addr_with_peer_id.into());
                    } else {
                        error!(peer_id=%peer_id, "attempted to clear an unknown bootstrapper");
                    }
                }
                let _ = ret.send(list);
            }
            IpfsEvent::DefaultBootstrap(ret) => {
                let mut rets = Vec::new();
                for addr in BOOTSTRAP_NODES {
                    let addr = addr
                        .parse::<MultiaddrWithPeerId>()
                        .expect("see test bootstrap_nodes_are_multiaddr_with_peerid");
                    if self.bootstraps.insert(addr.clone()) {
                        let MultiaddrWithPeerId {
                            multiaddr: ma,
                            peer_id,
                        } = addr.clone();

                        // this is intentionally the multiaddr without peerid turned into plain multiaddr:
                        // libp2p cannot dial addresses which include peerids.
                        let ma: Multiaddr = ma.into();

                        // same as with add_bootstrapper: the return value from kademlia.add_address
                        // doesn't implement Debug
                        self.swarm
                            .behaviour_mut()
                            .kademlia
                            .add_address(&peer_id, ma.clone());
                        trace!(peer_id=%peer_id, "tried to restore a bootstrapper");

                        // report with the peerid
                        let reported: Multiaddr = addr.into();
                        rets.push(reported);
                    }
                }

                let _ = ret.send(Ok(rets));
            }
            IpfsEvent::Exit => {
                // FIXME: we could do a proper teardown
            }
        }
    }
    async fn handle_repo_event(&mut self, event: RepoEvent) {
        match event {
            RepoEvent::WantBlock(cid) => self.swarm.behaviour_mut().want_block(cid),
            RepoEvent::UnwantBlock(cid) => self.swarm.behaviour_mut().bitswap().cancel_block(&cid),
            RepoEvent::NewBlock(cid, ret) => {
                // TODO: consider if cancel is applicable in cases where we provide the
                // associated Block ourselves
                self.swarm.behaviour_mut().bitswap().cancel_block(&cid);
                // currently disabled; see https://github.com/rs-ipfs/rust-ipfs/pull/281#discussion_r465583345
                // for details regarding the concerns about enabling this functionality as-is
                if false {
                    let key = Key::from(cid.hash().to_bytes());
                    let future = match self.swarm.behaviour_mut().kademlia.start_providing(key) {
                        Ok(id) => Ok(self.kad_subscriptions.create_subscription(id.into(), None)),
                        Err(e) => {
                            error!("kad: can't provide a key: {:?}", e);
                            Err(anyhow!("kad: can't provide the key: {:?}", e))
                        }
                    };

                    let _ = ret.send(future);
                } else {
                    let _ = ret.send(Err(anyhow!("not actively providing blocks yet")));
                }
            }
            RepoEvent::RemovedBlock(cid) => self.swarm.behaviour_mut().stop_providing_block(&cid),
        }
    }
}
