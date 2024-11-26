use super::gossipsub::GossipsubStream;
use super::{addressbook, protocol, request_response, rr_man};

use indexmap::IndexMap;
use libp2p_allow_block_list::BlockedPeers;

use super::peerbook::{self};
use either::Either;
use serde::{Deserialize, Serialize};

use crate::error::Error;
use crate::{IntoAddPeerOpt, IpfsOptions};

use crate::repo::Repo;

use ipld_core::cid::Cid;
use libp2p::core::Multiaddr;
use libp2p::dcutr::Behaviour as Dcutr;
use libp2p::identify::{Behaviour as Identify, Config as IdentifyConfig};
use libp2p::identity::{Keypair, PeerId};
use libp2p::kad::store::{MemoryStore, MemoryStoreConfig};
use libp2p::kad::{
    Behaviour as Kademlia, BucketInserts as KademliaBucketInserts, Config as KademliaConfig,
    Record, StoreInserts as KademliaStoreInserts,
};
#[cfg(not(target_arch = "wasm32"))]
use libp2p::mdns::tokio::Behaviour as Mdns;
use libp2p::ping::Behaviour as Ping;
use libp2p::relay::client::Behaviour as RelayClient;
use libp2p::relay::client::{self, Transport as ClientTransport};
use libp2p::relay::Behaviour as Relay;
use libp2p::swarm::behaviour::toggle::Toggle;
use libp2p::swarm::NetworkBehaviour;
use libp2p::{autonat, StreamProtocol};
use std::fmt::Debug;
use std::num::{NonZeroU32, NonZeroUsize};
use std::time::Duration;

/// Behaviour type.
#[derive(NetworkBehaviour)]
pub struct Behaviour<C>
where
    C: NetworkBehaviour,
    <C as NetworkBehaviour>::ToSwarm: Debug + Send,
{
    // connection management
    //TODO: Maybe have a optiont to enable a whitelist?
    pub block_list: libp2p_allow_block_list::Behaviour<BlockedPeers>,
    pub connection_limits: Toggle<libp2p_connection_limits::Behaviour>,
    pub addressbook: addressbook::Behaviour,

    // networking
    pub relay: Toggle<Relay>,
    pub relay_client: Toggle<RelayClient>,
    pub relay_manager: Toggle<libp2p_relay_manager::Behaviour>,
    #[cfg(not(target_arch = "wasm32"))]
    pub upnp: Toggle<libp2p::upnp::tokio::Behaviour>,
    pub dcutr: Toggle<Dcutr>,

    // discovery
    pub rendezvous_client: Toggle<libp2p::rendezvous::client::Behaviour>,
    pub rendezvous_server: Toggle<libp2p::rendezvous::server::Behaviour>,
    #[cfg(not(target_arch = "wasm32"))]
    pub mdns: Toggle<Mdns>,
    pub kademlia: Toggle<Kademlia<MemoryStore>>,

    // messaging
    pub identify: Toggle<Identify>,
    pub pubsub: Toggle<GossipsubStream>,
    pub bitswap: Toggle<super::bitswap::Behaviour>,
    pub ping: Toggle<Ping>,
    #[cfg(feature = "experimental_stream")]
    pub stream: Toggle<libp2p_stream::Behaviour>,

    pub autonat: Toggle<autonat::Behaviour>,

    // TODO: Write a macro or behaviour to support multiple request-response behaviour
    pub rr_man: Toggle<rr_man::Behaviour>,
    pub rr_1: Toggle<request_response::Behaviour>,
    pub rr_2: Toggle<request_response::Behaviour>,
    pub rr_3: Toggle<request_response::Behaviour>,
    pub rr_4: Toggle<request_response::Behaviour>,
    pub rr_5: Toggle<request_response::Behaviour>,
    pub rr_6: Toggle<request_response::Behaviour>,
    pub rr_7: Toggle<request_response::Behaviour>,
    pub rr_8: Toggle<request_response::Behaviour>,
    pub rr_9: Toggle<request_response::Behaviour>,
    pub rr_0: Toggle<request_response::Behaviour>,

    // custom behaviours
    pub custom: Toggle<C>,

    // misc
    pub peerbook: peerbook::Behaviour,
    pub protocol: protocol::Behaviour,
}

/// Represents the result of a Kademlia query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KadResult {
    /// The query has been exhausted.
    Complete,
    /// The query successfully returns `GetClosestPeers` or `GetProviders` results.
    Peers(Vec<PeerId>),
    /// The query successfully returns a `GetRecord` result.
    Records(Vec<Record>),
    Record(Record),
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RelayConfig {
    pub max_reservations: usize,
    pub max_reservations_per_peer: usize,
    pub reservation_duration: Duration,
    pub reservation_rate_limiters: Vec<RateLimit>,

    pub max_circuits: usize,
    pub max_circuits_per_peer: usize,
    pub max_circuit_duration: Duration,
    pub max_circuit_bytes: u64,
    pub circuit_src_rate_limiters: Vec<RateLimit>,
}

impl Default for RelayConfig {
    fn default() -> Self {
        Self {
            max_reservations: 128,
            max_reservations_per_peer: 4,
            reservation_duration: Duration::from_secs(60 * 60),
            reservation_rate_limiters: vec![
                RateLimit::PerPeer {
                    limit: NonZeroU32::new(30).expect("30 > 0"),
                    interval: Duration::from_secs(60 * 2),
                },
                RateLimit::PerIp {
                    limit: NonZeroU32::new(60).expect("60 > 0"),
                    interval: Duration::from_secs(60),
                },
            ],

            max_circuits: 16,
            max_circuits_per_peer: 4,
            max_circuit_duration: Duration::from_secs(2 * 60),
            max_circuit_bytes: 1 << 17,
            circuit_src_rate_limiters: vec![
                RateLimit::PerPeer {
                    limit: NonZeroU32::new(30).expect("30 > 0"),
                    interval: Duration::from_secs(60 * 2),
                },
                RateLimit::PerIp {
                    limit: NonZeroU32::new(60).expect("60 > 0"),
                    interval: Duration::from_secs(60),
                },
            ],
        }
    }
}

impl RelayConfig {
    /// Configuration to allow a connection to the relay without limits
    pub fn unbounded() -> Self {
        Self {
            max_circuits: usize::MAX,
            max_circuit_bytes: u64::MAX,
            max_circuit_duration: Duration::MAX,
            max_circuits_per_peer: usize::MAX,
            max_reservations: usize::MAX,
            reservation_duration: Duration::MAX,
            max_reservations_per_peer: usize::MAX,
            reservation_rate_limiters: vec![],
            circuit_src_rate_limiters: vec![],
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct IdentifyConfiguration {
    pub protocol_version: String,
    pub agent_version: String,
    pub interval: Duration,
    pub push_update: bool,
    pub cache: usize,
}

impl Default for IdentifyConfiguration {
    fn default() -> Self {
        Self {
            protocol_version: "/ipfs/0.1.0".into(),
            agent_version: "rust-ipfs".into(),
            interval: Duration::from_secs(5 * 60),
            push_update: true,
            cache: 100,
        }
    }
}

impl IdentifyConfiguration {
    pub fn into(self, publuc_key: libp2p::identity::PublicKey) -> IdentifyConfig {
        IdentifyConfig::new(self.protocol_version, publuc_key)
            .with_agent_version(self.agent_version)
            .with_interval(self.interval)
            .with_push_listen_addr_updates(self.push_update)
            .with_cache_size(self.cache)
    }
}

impl From<RelayConfig> for libp2p::relay::Config {
    fn from(
        RelayConfig {
            max_reservations,
            max_reservations_per_peer,
            reservation_duration,
            max_circuits,
            max_circuits_per_peer,
            max_circuit_duration,
            max_circuit_bytes,
            reservation_rate_limiters,
            circuit_src_rate_limiters,
        }: RelayConfig,
    ) -> Self {
        let reservation_duration = max_duration(reservation_duration);
        let max_circuit_duration = max_duration(max_circuit_duration);

        let mut config = libp2p::relay::Config {
            max_reservations,
            max_reservations_per_peer,
            reservation_duration,
            max_circuits,
            max_circuits_per_peer,
            max_circuit_duration,
            max_circuit_bytes,
            ..Default::default()
        };

        for rate in circuit_src_rate_limiters {
            match rate {
                RateLimit::PerPeer { limit, interval } => {
                    config = config.circuit_src_per_peer(limit, interval);
                }
                RateLimit::PerIp { limit, interval } => {
                    config = config.circuit_src_per_ip(limit, interval);
                }
            }
        }

        for rate in reservation_rate_limiters {
            match rate {
                RateLimit::PerPeer { limit, interval } => {
                    config = config.reservation_rate_per_peer(limit, interval);
                }
                RateLimit::PerIp { limit, interval } => {
                    config = config.reservation_rate_per_ip(limit, interval);
                }
            }
        }

        config
    }
}

fn max_duration(duration: Duration) -> Duration {
    let start = web_time::Instant::now();
    if start.checked_add(duration).is_none() {
        return Duration::from_secs(u32::MAX as _);
    }
    duration
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum RateLimit {
    PerPeer {
        limit: NonZeroU32,
        interval: std::time::Duration,
    },
    PerIp {
        limit: NonZeroU32,
        interval: std::time::Duration,
    },
}

#[derive(Default, Clone, Debug)]
pub struct KadStoreConfig {
    pub memory: Option<MemoryStoreConfig>,
}
#[derive(Clone, Debug)]
pub struct KadConfig {
    pub protocol: Option<String>,
    pub disjoint_query_paths: bool,
    pub query_timeout: Duration,
    pub parallelism: Option<NonZeroUsize>,
    pub publication_interval: Option<Duration>,
    pub provider_record_ttl: Option<Duration>,
    pub insert_method: KadInserts,
    pub store_filter: KadStoreInserts,
    pub automatic_bootstrap: Option<Duration>,
}

#[derive(Clone, Debug, Default, Copy)]
pub enum KadInserts {
    #[default]
    Auto,
    Manual,
}

#[derive(Clone, Debug, Default, Copy)]
pub enum KadStoreInserts {
    #[default]
    Unfiltered,
    Filtered,
}

impl From<KadStoreInserts> for KademliaStoreInserts {
    fn from(value: KadStoreInserts) -> Self {
        match value {
            KadStoreInserts::Filtered => KademliaStoreInserts::FilterBoth,
            KadStoreInserts::Unfiltered => KademliaStoreInserts::Unfiltered,
        }
    }
}

impl From<KadInserts> for KademliaBucketInserts {
    fn from(value: KadInserts) -> Self {
        match value {
            KadInserts::Auto => KademliaBucketInserts::OnConnected,
            KadInserts::Manual => KademliaBucketInserts::Manual,
        }
    }
}

impl From<KadConfig> for KademliaConfig {
    fn from(config: KadConfig) -> Self {
        let protocol = config.protocol.unwrap_or("/ipfs/kad/1.0.0".to_string());
        let protocol = StreamProtocol::try_from_owned(protocol).expect("protocol to be valid");

        let mut kad_config = KademliaConfig::new(protocol);
        kad_config.disjoint_query_paths(config.disjoint_query_paths);
        kad_config.set_query_timeout(config.query_timeout);
        if let Some(p) = config.parallelism {
            kad_config.set_parallelism(p);
        }
        kad_config.set_publication_interval(config.publication_interval);
        kad_config.set_provider_record_ttl(config.provider_record_ttl);
        kad_config.set_kbucket_inserts(config.insert_method.into());
        kad_config.set_record_filtering(config.store_filter.into());
        kad_config.set_periodic_bootstrap_interval(config.automatic_bootstrap);
        kad_config
    }
}

impl Default for KadConfig {
    fn default() -> Self {
        Self {
            protocol: None,
            disjoint_query_paths: false,
            query_timeout: Duration::from_secs(120),
            parallelism: Some(2.try_into().unwrap()),
            provider_record_ttl: None,
            publication_interval: None,
            insert_method: Default::default(),
            store_filter: Default::default(),
            automatic_bootstrap: None,
        }
    }
}

impl<C> Behaviour<C>
where
    C: NetworkBehaviour,
    <C as NetworkBehaviour>::ToSwarm: Debug + Send,
{
    pub(crate) fn new(
        keypair: &Keypair,
        options: &IpfsOptions,
        repo: &Repo,
        custom: Option<C>,
    ) -> Result<(Self, Option<ClientTransport>), Error> {
        let bootstrap = options.bootstrap.clone();

        let protocols = options.protocols;

        let peer_id = keypair.public().to_peer_id();

        info!("net: starting with peer id {}", peer_id);

        // TODO: Do we want to ignore the protocol if there is an error from Mdns::new?
        #[cfg(not(target_arch = "wasm32"))]
        let mdns = protocols
            .mdns
            .then(|| Mdns::new(Default::default(), peer_id).ok())
            .flatten()
            .into();

        let store = {
            //TODO: Make customizable
            //TODO: Use persistent store for kad
            let config = options.kad_store_config.memory.clone().unwrap_or_default();

            MemoryStore::with_config(peer_id, config)
        };

        let kad_config = match options.kad_configuration.clone() {
            Either::Left(kad) => kad.into(),
            Either::Right(kad) => kad,
        };

        let kademlia: Toggle<Kademlia<MemoryStore>> = (protocols.kad)
            .then(|| Kademlia::with_config(peer_id, store, kad_config))
            .into();

        let autonat = protocols
            .autonat
            .then(|| autonat::Behaviour::new(peer_id, Default::default()))
            .into();

        let bitswap = protocols
            .bitswap
            .then(|| super::bitswap::Behaviour::new(repo))
            .into();

        let ping = protocols
            .ping
            .then(|| Ping::new(options.ping_configuration.clone()))
            .into();

        let identify = protocols
            .identify
            .then(|| {
                Identify::new(
                    options
                        .identify_configuration
                        .clone()
                        .into(keypair.public()),
                )
            })
            .into();

        let pubsub = {
            let pubsub_config = options.pubsub_config.clone();
            let mut builder = libp2p::gossipsub::ConfigBuilder::default();

            if let Some(protocol) = pubsub_config.custom_protocol_id {
                builder.protocol_id(protocol, libp2p::gossipsub::Version::V1_1);
            }

            builder.max_transmit_size(pubsub_config.max_transmit_size);

            if pubsub_config.floodsub_compat {
                builder.support_floodsub();
            }

            builder.validation_mode(pubsub_config.validate.into());

            let config = builder.build().map_err(anyhow::Error::from)?;

            let gossipsub = libp2p::gossipsub::Behaviour::new(
                libp2p::gossipsub::MessageAuthenticity::Signed(keypair.clone()),
                config,
            )
            .map_err(|e| anyhow::anyhow!("{}", e))?;

            protocols
                .pubsub
                .then(|| GossipsubStream::from(gossipsub))
                .into()
        };

        // Maybe have this enable in conjunction with RelayClient?
        let dcutr = protocols.dcutr.then(|| Dcutr::new(peer_id)).into();
        let relay_config = options.relay_server_config.clone().into();

        let relay = protocols
            .relay_server
            .then(|| Relay::new(peer_id, relay_config))
            .into();

        #[cfg(not(target_arch = "wasm32"))]
        let upnp = protocols
            .upnp
            .then(libp2p::upnp::tokio::Behaviour::default)
            .into();

        let (transport, relay_client, relay_manager) = match protocols.relay_client {
            true => {
                let (transport, client) = client::new(peer_id);
                let manager = libp2p_relay_manager::Behaviour::new(Default::default());
                (Some(transport), Some(client).into(), Some(manager).into())
            }
            false => (None, None.into(), None.into()),
        };

        let peerbook = peerbook::Behaviour::default();

        let addressbook = addressbook::Behaviour::with_config(options.addr_config);

        let block_list = libp2p_allow_block_list::Behaviour::default();
        let protocol = protocol::Behaviour::default();
        let custom = Toggle::from(custom);

        let rendezvous_client = protocols
            .rendezvous_client
            .then(|| libp2p::rendezvous::client::Behaviour::new(keypair.clone()))
            .into();

        let rendezvous_server = protocols
            .rendezvous_server
            .then(|| libp2p::rendezvous::server::Behaviour::new(Default::default()))
            .into();

        #[cfg(feature = "experimental_stream")]
        let stream = protocols.streams.then(libp2p_stream::Behaviour::new).into();

        let connection_limits = options
            .connection_limits
            .clone()
            .map(libp2p_connection_limits::Behaviour::new)
            .into();

        let mut behaviour = Behaviour {
            connection_limits,
            #[cfg(not(target_arch = "wasm32"))]
            mdns,
            kademlia,
            bitswap,
            ping,
            identify,
            autonat,
            pubsub,
            dcutr,
            relay,
            relay_client,
            relay_manager,
            block_list,
            #[cfg(feature = "experimental_stream")]
            stream,
            #[cfg(not(target_arch = "wasm32"))]
            upnp,
            peerbook,
            addressbook,
            protocol,
            custom,
            rendezvous_client,
            rendezvous_server,
            rr_man: Toggle::from(None),
            rr_0: Toggle::from(None),
            rr_1: Toggle::from(None),
            rr_2: Toggle::from(None),
            rr_3: Toggle::from(None),
            rr_4: Toggle::from(None),
            rr_5: Toggle::from(None),
            rr_6: Toggle::from(None),
            rr_7: Toggle::from(None),
            rr_8: Toggle::from(None),
            rr_9: Toggle::from(None),
        };

        let mut existing_protocol: IndexMap<StreamProtocol, _> = IndexMap::new();

        match options.request_response_config {
            Either::Left(ref config) => {
                let protocol = StreamProtocol::try_from_owned(config.protocol.clone())
                    .expect("valid protocol");
                existing_protocol.insert(protocol, 0);
                behaviour.rr_0 = protocols
                    .request_response
                    .then(|| request_response::Behaviour::new(config.clone()))
                    .into();
            }
            Either::Right(ref configs) => {
                for (index, config) in configs.iter().enumerate() {
                    let protocol = StreamProtocol::try_from_owned(config.protocol.clone())
                        .expect("valid protocol");
                    if existing_protocol.contains_key(&protocol) {
                        tracing::warn!(%protocol, "request-response protocol is already registered");
                        continue;
                    };

                    match index {
                        0 => {
                            if behaviour.rr_0.is_enabled() {
                                continue;
                            }
                            behaviour.rr_0 = protocols
                                .request_response
                                .then(|| request_response::Behaviour::new(config.clone()))
                                .into();
                        }
                        1 => {
                            if behaviour.rr_1.is_enabled() {
                                continue;
                            }
                            behaviour.rr_1 = protocols
                                .request_response
                                .then(|| request_response::Behaviour::new(config.clone()))
                                .into();
                        }
                        2 => {
                            if behaviour.rr_2.is_enabled() {
                                continue;
                            }
                            behaviour.rr_2 = protocols
                                .request_response
                                .then(|| request_response::Behaviour::new(config.clone()))
                                .into();
                        }
                        3 => {
                            if !behaviour.rr_3.is_enabled() {
                                continue;
                            }
                            behaviour.rr_3 = protocols
                                .request_response
                                .then(|| request_response::Behaviour::new(config.clone()))
                                .into();
                        }
                        4 => {
                            if !behaviour.rr_4.is_enabled() {
                                continue;
                            }
                            behaviour.rr_4 = protocols
                                .request_response
                                .then(|| request_response::Behaviour::new(config.clone()))
                                .into();
                        }
                        5 => {
                            if !behaviour.rr_5.is_enabled() {
                                continue;
                            }
                            behaviour.rr_5 = protocols
                                .request_response
                                .then(|| request_response::Behaviour::new(config.clone()))
                                .into();
                        }
                        6 => {
                            if !behaviour.rr_6.is_enabled() {
                                continue;
                            }
                            behaviour.rr_6 = protocols
                                .request_response
                                .then(|| request_response::Behaviour::new(config.clone()))
                                .into();
                        }
                        7 => {
                            if !behaviour.rr_7.is_enabled() {
                                continue;
                            }
                            behaviour.rr_7 = protocols
                                .request_response
                                .then(|| request_response::Behaviour::new(config.clone()))
                                .into();
                        }
                        8 => {
                            if !behaviour.rr_8.is_enabled() {
                                continue;
                            }
                            behaviour.rr_8 = protocols
                                .request_response
                                .then(|| request_response::Behaviour::new(config.clone()))
                                .into();
                        }
                        9 => {
                            if !behaviour.rr_9.is_enabled() {
                                continue;
                            }
                            behaviour.rr_9 = protocols
                                .request_response
                                .then(|| request_response::Behaviour::new(config.clone()))
                                .into();
                        }
                        _ => break,
                    }

                    existing_protocol.insert(protocol, index);
                }
            }
        }

        if !existing_protocol.is_empty() {
            behaviour.rr_man = Toggle::from(Some(rr_man::Behaviour::new(existing_protocol)))
        }

        for addr in bootstrap {
            let Ok(mut opt) = IntoAddPeerOpt::into_opt(addr) else {
                continue;
            };

            // explicitly dial the bootstrap peer. If the peer will be bootstrapped via kad, the additional dial will be cancelled
            opt = opt.set_dial(true);

            _ = behaviour.add_peer(opt);
        }

        Ok((behaviour, transport))
    }

    pub fn add_peer<I: IntoAddPeerOpt>(&mut self, opt: I) -> bool {
        let opt = opt.into_opt().expect("valid entries");
        if let Some(kad) = self.kademlia.as_mut() {
            let peer_id = opt.peer_id();
            let addrs = opt.addresses().to_vec();
            for addr in addrs {
                kad.add_address(peer_id, addr);
            }
        }

        self.addressbook.add_address(opt);

        true
    }

    pub fn remove_peer(&mut self, peer: &PeerId) {
        self.addressbook.remove_peer(peer);
    }

    pub fn addrs(&self) -> Vec<(PeerId, Vec<Multiaddr>)> {
        self.peerbook.connected_peers_addrs().collect()
    }

    pub fn stop_providing_block(&mut self, cid: &Cid) {
        info!("Finished providing block {}", cid.to_string());
        let key = cid.hash().to_bytes();
        if let Some(kad) = self.kademlia.as_mut() {
            kad.stop_providing(&key.into());
        }
    }

    pub fn supported_protocols(&self) -> Vec<String> {
        self.protocol.iter().collect::<Vec<_>>()
    }

    pub fn pubsub(&mut self) -> Option<&mut GossipsubStream> {
        self.pubsub.as_mut()
    }

    pub fn request_response(
        &mut self,
        protocol: Option<StreamProtocol>,
    ) -> Option<&mut request_response::Behaviour> {
        let Some(protocol) = protocol else {
            return self.rr_0.as_mut();
        };

        let manager = self.rr_man.as_ref()?;
        let index = manager.get_protocol(protocol)?;
        match index {
            0 => self.rr_0.as_mut(),
            1 => self.rr_1.as_mut(),
            2 => self.rr_2.as_mut(),
            3 => self.rr_3.as_mut(),
            4 => self.rr_4.as_mut(),
            5 => self.rr_5.as_mut(),
            6 => self.rr_6.as_mut(),
            7 => self.rr_7.as_mut(),
            8 => self.rr_8.as_mut(),
            9 => self.rr_9.as_mut(),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn max_duration_test() {
        let base = Duration::from_secs(1);
        let dur = max_duration(base);
        assert_eq!(dur, base);

        let base = Duration::MAX;
        let dur = max_duration(base);
        assert_ne!(dur, base);
        assert_eq!(dur, Duration::from_secs(u32::MAX as _))
    }
}
