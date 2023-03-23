use anyhow::{anyhow, format_err};
use either::Either;
use futures::{
    channel::{
        mpsc::{Receiver, UnboundedSender},
        oneshot,
    },
    sink::SinkExt,
    stream::Fuse,
    StreamExt,
};

use crate::{p2p::PeerInfo, Channel};
use crate::{
    p2p::{ProviderStream, RecordStream},
    TSwarmEvent,
};
use ipfs_bitswap::BitswapEvent;
use tokio::sync::Notify;

use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    io,
    sync::Arc,
    time::Duration,
};

use crate::{config::BOOTSTRAP_NODES, repo::BlockPut, IpfsEvent, TSwarmEventFn};

use crate::{
    p2p::TSwarm,
    repo::{Repo, RepoEvent},
};

pub use crate::{
    error::Error,
    p2p::BehaviourEvent,
    p2p::{Connection, KadResult, MultiaddrWithPeerId, MultiaddrWithoutPeerId},
    path::IpfsPath,
    repo::{PinKind, PinMode},
};

use libipld::multibase::{self, Base};
pub use libp2p::{
    self,
    core::transport::ListenerId,
    gossipsub::{MessageId, PublishError},
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
pub(crate) struct IpfsTask {
    pub(crate) swarm: TSwarm,
    pub(crate) repo_events: Fuse<Receiver<RepoEvent>>,
    pub(crate) from_facade: Fuse<Receiver<IpfsEvent>>,
    pub(crate) listening_addresses: HashMap<Multiaddr, ListenerId>,
    pub(crate) listeners: HashSet<ListenerId>,
    pub(crate) provider_stream: HashMap<QueryId, UnboundedSender<PeerId>>,
    pub(crate) record_stream: HashMap<QueryId, UnboundedSender<Record>>,
    pub(crate) repo: Repo,
    pub(crate) kad_subscriptions: HashMap<QueryId, Channel<KadResult>>,
    pub(crate) dht_peer_lookup: HashMap<PeerId, Vec<Channel<PeerInfo>>>,
    pub(crate) listener_subscriptions:
        HashMap<ListenerId, oneshot::Sender<Either<Multiaddr, Result<(), io::Error>>>>,
    pub(crate) bootstraps: HashSet<MultiaddrWithPeerId>,
    pub(crate) swarm_event: Option<TSwarmEventFn>,
}

impl IpfsTask {
    pub(crate) async fn run(&mut self, delay: bool, notify: Arc<Notify>) {
        let mut first_run = false;
        let mut connected_peer_timer = tokio::time::interval(Duration::from_secs(60));
        loop {
            tokio::select! {
                Some(swarm) = self.swarm.next() => {
                    if delay {
                        tokio::time::sleep(Duration::from_nanos(10)).await;
                    }
                    self.handle_swarm_event(swarm);
                },
                Some(event) = self.from_facade.next() => {
                    if matches!(event, IpfsEvent::Exit) {
                        break;
                    }
                    if delay {
                        tokio::time::sleep(Duration::from_nanos(10)).await;
                    }
                    self.handle_event(event);
                },
                Some(repo) = self.repo_events.next() => {
                    self.handle_repo_event(repo);
                },
                _ = connected_peer_timer.tick() => {
                    info!("Connected Peers: {}", self.swarm.connected_peers().count());
                }
            }
            if !first_run {
                first_run = true;
                notify.notify_one();
            }
        }
    }

    fn handle_swarm_event(&mut self, swarm_event: TSwarmEvent) {
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

                if let Some(ret) = self.listener_subscriptions.remove(&listener_id) {
                    let _ = ret.send(Either::Left(address));
                }
            }
            SwarmEvent::ExpiredListenAddr {
                listener_id,
                address,
            } => {
                self.listeners.remove(&listener_id);
                self.listening_addresses.remove(&address);
                if let Some(ret) = self.listener_subscriptions.remove(&listener_id) {
                    //TODO: Determine if we want to return the address or use the right side and return an error?
                    let _ = ret.send(Either::Left(address));
                }
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
                if let Some(ret) = self.listener_subscriptions.remove(&listener_id) {
                    let _ = ret.send(Either::Right(reason));
                }
            }
            SwarmEvent::ListenerError { listener_id, error } => {
                self.listeners.remove(&listener_id);
                if let Some(ret) = self.listener_subscriptions.remove(&listener_id) {
                    let _ = ret.send(Either::Right(Err(error)));
                }
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
                                self.swarm.behaviour_mut().remove_peer(&peer, false);
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
                    self.swarm.behaviour_mut().peerbook.set_peer_rtt(peer, rtt);
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
                    self.swarm.behaviour_mut().remove_peer(&peer, false);
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
                        .peerbook
                        .inject_peer_info(info.clone());

                    if let Some(rets) = self.dht_peer_lookup.remove(&peer_id) {
                        for ret in rets {
                            let _ = ret.send(Ok(info.clone().into()));
                        }
                    }

                    let IdentifyInfo {
                        listen_addrs,
                        protocols,
                        ..
                    } = info;

                    if let Some(kad) = self.swarm.behaviour_mut().kademlia.as_mut() {
                        if protocols
                            .iter()
                            .any(|p| p.as_bytes() == libp2p::kad::protocol::DEFAULT_PROTO_NAME)
                        {
                            for addr in &listen_addrs {
                                kad.add_address(&peer_id, addr.clone());
                            }
                        }
                    }

                    if protocols
                        .iter()
                        .any(|p| p.as_bytes() == libp2p::autonat::DEFAULT_PROTOCOL_NAME)
                    {
                        for addr in listen_addrs {
                            self.swarm
                                .behaviour_mut()
                                .autonat
                                .add_server(peer_id, Some(addr));
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

    #[allow(deprecated)]
    //TODO: Replace addresses_of_peer
    fn handle_event(&mut self, event: IpfsEvent) {
        match event {
            IpfsEvent::Connect(target, ret) => {
                let rx = self.swarm.behaviour_mut().peerbook.connect(target);
                let _ = ret.send(rx);
            }
            IpfsEvent::Protocol(ret) => {
                let info = self.swarm.behaviour().supported_protocols();
                let _ = ret.send(info);
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
                    let (tx, rx) = oneshot::channel();
                    self.listener_subscriptions.insert(id, tx);
                    let _ = ret.send(Ok(rx));
                }
                Err(e) => {
                    let _ = ret.send(Err(anyhow::anyhow!(e)));
                }
            },
            IpfsEvent::RemoveListeningAddress(addr, ret) => {
                if let Some(id) = self.listening_addresses.remove(&addr) {
                    match self.swarm.remove_listener(id) {
                        true => {
                            self.listeners.remove(&id);
                            let (tx, rx) = oneshot::channel();
                            self.listener_subscriptions.insert(id, tx);
                            let _ = ret.send(Ok(rx));
                        }
                        false => {
                            let _ = ret.send(Err(anyhow::anyhow!(
                                "Failed to remove previously added listening address: {}",
                                addr
                            )));
                        }
                    }
                } else {
                    let _ = ret.send(Err(format_err!(
                        "Address was not listened to before: {}",
                        addr
                    )));
                }
            }
            IpfsEvent::Bootstrap(ret) => {
                let future = match self
                    .swarm
                    .behaviour_mut()
                    .kademlia
                    .as_mut()
                    .map(|kad| kad.bootstrap())
                {
                    Some(Ok(id)) => {
                        let (tx, rx) = oneshot::channel();
                        self.kad_subscriptions.insert(id, tx);
                        Ok(rx)
                    }
                    Some(Err(e)) => {
                        error!("kad: can't bootstrap the node: {:?}", e);
                        Err(anyhow!("kad: can't bootstrap the node: {:?}", e))
                    }
                    None => Err(anyhow!("kad protocol is disabled")),
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
                    .as_mut()
                    .map(|kad| kad.get_closest_peers(peer_id));

                let (tx, rx) = oneshot::channel();
                let _ = ret.send(rx);
                match id {
                    Some(id) => {
                        self.kad_subscriptions.insert(id, tx);
                    }
                    None => {
                        let _ = tx.send(Err(anyhow::anyhow!("kad protocol is disabled")));
                    }
                };
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
            IpfsEvent::FindPeerIdentity(peer_id, ret) => {
                let locally_known = self.swarm.behaviour().peerbook.get_peer_info(peer_id);

                let (tx, rx) = oneshot::channel();

                match locally_known {
                    Some(info) => {
                        let _ = tx.send(Ok(info.clone()));
                    }
                    None => {
                        self.swarm
                            .behaviour_mut()
                            .kademlia
                            .as_mut()
                            .map(|kad| kad.get_closest_peers(peer_id));

                        self.dht_peer_lookup.entry(peer_id).or_default().push(tx);
                    }
                }

                let _ = ret.send(rx);
            }
            IpfsEvent::FindPeer(peer_id, local_only, ret) => {
                let swarm_addrs = self.swarm.behaviour_mut().swarm.connections_to(&peer_id);
                let locally_known_addrs = if !swarm_addrs.is_empty() {
                    swarm_addrs
                } else {
                    self.swarm
                        .behaviour_mut()
                        .kademlia
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
                            .as_mut()
                            .map(|kad| kad.get_closest_peers(peer_id));

                        let (tx, rx) = oneshot::channel();
                        if let Some(id) = id {
                            self.kad_subscriptions.insert(id, tx);
                        }
                        rx
                    })
                };
                let _ = ret.send(addrs);
            }
            IpfsEvent::WhitelistPeer(peer_id, ret) => {
                self.swarm.behaviour_mut().peerbook.add(peer_id);
                let _ = ret.send(Ok(()));
            }
            IpfsEvent::RemoveWhitelistPeer(peer_id, ret) => {
                self.swarm.behaviour_mut().peerbook.remove(peer_id);
                let _ = ret.send(Ok(()));
            }
            IpfsEvent::GetProviders(cid, ret) => {
                let key = Key::from(cid.hash().to_bytes());
                let id = self
                    .swarm
                    .behaviour_mut()
                    .kademlia
                    .as_mut()
                    .map(|kad| kad.get_providers(key));

                let mut provider_stream = None;

                let (tx, mut rx) = futures::channel::mpsc::unbounded();
                if let Some(id) = id {
                    let stream = async_stream::stream! {
                        let mut current_providers: HashSet<PeerId> = Default::default();
                        while let Some(provider) = rx.next().await {
                            if current_providers.insert(provider) {
                                yield provider;
                            }
                        }
                    };
                    self.provider_stream.insert(id, tx);
                    provider_stream = Some(ProviderStream(stream.boxed()));
                }

                let _ = ret.send(provider_stream);
            }
            IpfsEvent::Provide(cid, ret) => {
                let key = Key::from(cid.hash().to_bytes());
                let future = match self
                    .swarm
                    .behaviour_mut()
                    .kademlia
                    .as_mut()
                    .map(|kad| kad.start_providing(key))
                {
                    Some(Ok(id)) => {
                        let (tx, rx) = oneshot::channel();
                        self.kad_subscriptions.insert(id, tx);
                        Ok(rx)
                    }
                    Some(Err(e)) => {
                        error!("kad: can't provide a key: {:?}", e);
                        Err(anyhow!("kad: can't provide the key: {:?}", e))
                    }
                    None => Err(anyhow!("kad protocol is disabled")),
                };
                let _ = ret.send(future);
            }
            IpfsEvent::DhtGet(key, ret) => {
                let id = self
                    .swarm
                    .behaviour_mut()
                    .kademlia
                    .as_mut()
                    .map(|kad| kad.get_record(key));

                let (tx, mut rx) = futures::channel::mpsc::unbounded();
                let stream = async_stream::stream! {
                    while let Some(record) = rx.next().await {
                            yield record;
                    }
                };
                if let Some(id) = id {
                    self.record_stream.insert(id, tx);
                }

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
                    .as_mut()
                    .map(|kad| kad.put_record(record, quorum))
                {
                    Some(Ok(id)) => {
                        let (tx, rx) = oneshot::channel();
                        self.kad_subscriptions.insert(id, tx);
                        Ok(rx)
                    }
                    Some(Err(e)) => {
                        error!("kad: can't put a record: {:?}", e);
                        Err(anyhow!("kad: can't provide the record: {:?}", e))
                    }
                    None => Err(anyhow!("kad protocol is not enabled")),
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
                if !self.swarm.behaviour().kademlia.is_enabled() {
                    let _ = ret.send(Err(anyhow::anyhow!("kad protocol is disabled")));
                } else {
                    if self.bootstraps.insert(addr.clone()) {
                        let MultiaddrWithPeerId {
                            multiaddr: ma,
                            peer_id,
                        } = addr;

                        self.swarm
                            .behaviour_mut()
                            .kademlia
                            .as_mut()
                            .map(|kad| kad.add_address(&peer_id, ma.into()));
                        self.swarm.behaviour_mut().peerbook.add(peer_id);
                        // the return value of add_address doesn't implement Debug
                        trace!(peer_id=%peer_id, "tried to add a bootstrapper");
                    }
                    let _ = ret.send(Ok(ret_addr));
                }
            }
            IpfsEvent::RemoveBootstrapper(addr, ret) => {
                let result = addr.clone().into();
                if !self.swarm.behaviour().kademlia.is_enabled() {
                    let _ = ret.send(Err(anyhow::anyhow!("kad protocol is disabled")));
                } else {
                    if self.bootstraps.remove(&addr) {
                        let peer_id = addr.peer_id;
                        let prefix: Multiaddr = addr.multiaddr.into();

                        if let Some(Some(e)) = self
                            .swarm
                            .behaviour_mut()
                            .kademlia
                            .as_mut()
                            .map(|kad| kad.remove_address(&peer_id, &prefix))
                        {
                            info!(peer_id=%peer_id, status=?e.status, "removed bootstrapper");
                        } else {
                            warn!(peer_id=%peer_id, "attempted to remove an unknown bootstrapper");
                        }
                        self.swarm.behaviour_mut().peerbook.remove(peer_id);
                    }
                    let _ = ret.send(Ok(result));
                }
            }
            IpfsEvent::ClearBootstrappers(ret) => {
                let removed = self.bootstraps.drain().collect::<Vec<_>>();
                let mut list = Vec::with_capacity(removed.len());
                if self.swarm.behaviour().kademlia.is_enabled() {
                    for addr_with_peer_id in removed {
                        let peer_id = addr_with_peer_id.peer_id;
                        let prefix: Multiaddr = addr_with_peer_id.multiaddr.clone().into();

                        if let Some(Some(e)) = self
                            .swarm
                            .behaviour_mut()
                            .kademlia
                            .as_mut()
                            .map(|kad| kad.remove_address(&peer_id, &prefix))
                        {
                            info!(peer_id=%peer_id, status=?e.status, "cleared bootstrapper");
                            list.push(addr_with_peer_id.into());
                        } else {
                            error!(peer_id=%peer_id, "attempted to clear an unknown bootstrapper");
                        }
                        self.swarm.behaviour_mut().peerbook.remove(peer_id);
                    }
                }
                let _ = ret.send(list);
            }
            IpfsEvent::DefaultBootstrap(ret) => {
                let mut rets = Vec::new();
                if self.swarm.behaviour().kademlia.is_enabled() {
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
                                .as_mut()
                                .map(|kad| kad.add_address(&peer_id, ma.clone()));
                            trace!(peer_id=%peer_id, "tried to restore a bootstrapper");
                            self.swarm.behaviour_mut().peerbook.add(peer_id);
                            // report with the peerid
                            let reported: Multiaddr = addr.into();
                            rets.push(reported);
                        }
                    }
                }

                let _ = ret.send(Ok(rets));
            }
            IpfsEvent::Exit => {
                // FIXME: we could do a proper teardown
            }
        }
    }

    fn handle_repo_event(&mut self, event: RepoEvent) {
        match event {
            RepoEvent::WantBlock(cid, _) => self.swarm.behaviour_mut().want_block(cid, &[]),
            RepoEvent::UnwantBlock(cid) => self.swarm.behaviour_mut().bitswap().cancel_block(&cid),
            RepoEvent::NewBlock(cid, ret) => {
                // TODO: consider if cancel is applicable in cases where we provide the
                // associated Block ourselves
                self.swarm.behaviour_mut().bitswap().cancel_block(&cid);
                // currently disabled; see https://github.com/rs-ipfs/rust-ipfs/pull/281#discussion_r465583345
                // for details regarding the concerns about enabling this functionality as-is
                if false {
                    let key = Key::from(cid.hash().to_bytes());
                    let future = match self
                        .swarm
                        .behaviour_mut()
                        .kademlia
                        .as_mut()
                        .map(|kad| kad.start_providing(key))
                    {
                        Some(Ok(id)) => {
                            let (tx, rx) = oneshot::channel();
                            self.kad_subscriptions.insert(id, tx);
                            Ok(rx)
                        }
                        Some(Err(e)) => {
                            error!("kad: can't provide a key: {:?}", e);
                            Err(anyhow!("kad: can't provide the key: {:?}", e))
                        }
                        None => Err(anyhow!("kad protocol is disabled")),
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
