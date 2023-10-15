use super::gossipsub::GossipsubStream;
use super::{addressbook, protocol};
use bytes::Bytes;
use libp2p_allow_block_list::BlockedPeers;

use super::peerbook::{self, ConnectionLimits};
use either::Either;
use serde::{Deserialize, Serialize};

use crate::error::Error;
use crate::IpfsOptions;

use crate::p2p::MultiaddrExt;
use crate::repo::Repo;

use beetle_bitswap_next::{Bitswap, ProtocolId};
use libipld::Cid;
use libp2p::core::Multiaddr;
use libp2p::dcutr::Behaviour as Dcutr;
use libp2p::identify::{Behaviour as Identify, Config as IdentifyConfig};
use libp2p::identity::{Keypair, PeerId};
use libp2p::kad::record::{
    store::{MemoryStore, MemoryStoreConfig},
    Record,
};
use libp2p::kad::{Kademlia, KademliaBucketInserts, KademliaConfig, KademliaStoreInserts};
use libp2p::mdns::{tokio::Behaviour as Mdns, Config as MdnsConfig};
use libp2p::ping::Behaviour as Ping;
use libp2p::relay::client::Behaviour as RelayClient;
use libp2p::relay::client::{self, Transport as ClientTransport};
use libp2p::relay::Behaviour as Relay;
use libp2p::swarm::behaviour::toggle::Toggle;
use libp2p::swarm::NetworkBehaviour;
use libp2p::{autonat, StreamProtocol};
use std::borrow::Cow;
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
    pub mdns: Toggle<Mdns>,
    pub bitswap: Toggle<Bitswap<Repo>>,
    pub kademlia: Toggle<Kademlia<MemoryStore>>,
    pub ping: Toggle<Ping>,
    pub identify: Toggle<Identify>,
    pub pubsub: Toggle<GossipsubStream>,
    pub autonat: Toggle<autonat::Behaviour>,
    pub upnp: Toggle<libp2p_nat::Behaviour>,
    pub block_list: libp2p_allow_block_list::Behaviour<BlockedPeers>,
    pub relay: Toggle<Relay>,
    pub relay_client: Toggle<RelayClient>,
    pub relay_manager: Toggle<libp2p_relay_manager::Behaviour>,
    pub rendezvous_client: Toggle<libp2p::rendezvous::client::Behaviour>,
    pub rendezvous_server: Toggle<libp2p::rendezvous::server::Behaviour>,
    pub dcutr: Toggle<Dcutr>,
    pub addressbook: addressbook::Behaviour,
    pub peerbook: peerbook::Behaviour,
    pub protocol: protocol::Behaviour,
    pub custom: Toggle<C>,
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
    ///
    Record(Record),
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RelayConfig {
    pub max_reservations: usize,
    pub max_reservations_per_peer: usize,
    pub reservation_duration: std::time::Duration,
    pub reservation_rate_limiters: Vec<RateLimit>,

    pub max_circuits: usize,
    pub max_circuits_per_peer: usize,
    pub max_circuit_duration: std::time::Duration,
    pub max_circuit_bytes: u64,
    pub circuit_src_rate_limiters: Vec<RateLimit>,
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
    pub protocol: Option<Vec<Cow<'static, str>>>,
    pub disjoint_query_paths: bool,
    pub query_timeout: Duration,
    pub parallelism: Option<NonZeroUsize>,
    pub publication_interval: Option<Duration>,
    pub provider_record_ttl: Option<Duration>,
    pub insert_method: KadInserts,
    pub store_filter: KadStoreInserts,
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
        let mut kad_config = KademliaConfig::default();
        if let Some(protocol) = config.protocol.map(|list| {
            list.iter()
                .filter_map(|p| StreamProtocol::try_from_owned(p.to_string()).ok())
                .collect()
        }) {
            kad_config.set_protocol_names(protocol);
        }
        kad_config.disjoint_query_paths(config.disjoint_query_paths);
        kad_config.set_query_timeout(config.query_timeout);
        if let Some(p) = config.parallelism {
            kad_config.set_parallelism(p);
        }
        kad_config.set_publication_interval(config.publication_interval);
        kad_config.set_provider_record_ttl(config.provider_record_ttl);
        kad_config.set_kbucket_inserts(config.insert_method.into());
        kad_config.set_record_filtering(config.store_filter.into());
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
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BitswapConfig {
    protocol: Vec<BitswapProtocol>,
    max_buf_size: Option<usize>,
    server: bool,
}

impl Default for BitswapConfig {
    fn default() -> Self {
        Self {
            protocol: vec![
                BitswapProtocol::ProtocolLegacy,
                BitswapProtocol::Protocol100,
                BitswapProtocol::Protocol110,
                BitswapProtocol::Protocol120,
            ],
            max_buf_size: None,
            server: true,
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Default, Hash, PartialOrd, Ord)]
pub enum BitswapProtocol {
    ProtocolLegacy,
    Protocol100,
    Protocol110,
    #[default]
    Protocol120,
}

impl From<BitswapProtocol> for ProtocolId {
    fn from(value: BitswapProtocol) -> Self {
        match value {
            BitswapProtocol::ProtocolLegacy => ProtocolId::Legacy,
            BitswapProtocol::Protocol100 => ProtocolId::Bitswap100,
            BitswapProtocol::Protocol110 => ProtocolId::Bitswap110,
            BitswapProtocol::Protocol120 => ProtocolId::Bitswap120,
        }
    }
}

impl From<BitswapConfig> for beetle_bitswap_next::Config {
    fn from(value: BitswapConfig) -> Self {
        beetle_bitswap_next::Config {
            client: Default::default(),
            server: value.server.then_some(Default::default()),
            protocol: beetle_bitswap_next::ProtocolConfig {
                protocol_ids: value.protocol.iter().map(|proto| (*proto).into()).collect(),
                max_transmit_size: value.max_buf_size.unwrap_or(1024 * 1024 * 2),
            },
            ..Default::default()
        }
    }
}

impl<C> Behaviour<C>
where
    C: NetworkBehaviour,
    <C as NetworkBehaviour>::ToSwarm: Debug + Send,
{
    pub async fn new(
        keypair: &Keypair,
        options: &IpfsOptions,
        repo: Repo,
        limits: ConnectionLimits,
        custom: Option<C>,
    ) -> Result<(Self, Option<ClientTransport>), Error> {
        let protocols = options.protocols;

        let peer_id = keypair.public().to_peer_id();

        info!("net: starting with peer id {}", peer_id);

        let mdns = if protocols.mdns {
            let config = MdnsConfig {
                ..Default::default()
            };
            Mdns::new(config, peer_id).ok()
        } else {
            None
        }
        .into();

        let store = {
            //TODO: Make customizable
            //TODO: Use persistent store for kad
            let config = options.kad_store_config.memory.clone().unwrap_or_default();

            MemoryStore::with_config(peer_id, config)
        };

        let kad_config = match options
            .kad_configuration
            .clone()
            .unwrap_or(Either::Left(KadConfig::default()))
        {
            Either::Left(kad) => kad.into(),
            Either::Right(kad) => kad,
        };

        let mut kademlia: Toggle<Kademlia<MemoryStore>> = Toggle::from(
            (protocols.kad).then_some(Kademlia::with_config(peer_id, store, kad_config)),
        );

        if let Some(kad) = kademlia.as_mut() {
            for mut addr in options.bootstrap.clone() {
                let Some(peer_id) = addr.extract_peer_id() else {
                    continue;
                };
                kad.add_address(&peer_id, addr);
            }
        }

        let autonat = protocols
            .autonat
            .then_some(autonat::Behaviour::new(peer_id, Default::default()))
            .into();

        let bitswap = (protocols.bitswap)
            .then_some(Bitswap::new(peer_id, repo, Default::default()).await)
            .into();

        let ping = protocols
            .ping
            .then_some(Ping::new(
                options.ping_configuration.clone().unwrap_or_default(),
            ))
            .into();

        let identify = protocols
            .identify
            .then_some(Identify::new(
                options
                    .identify_configuration
                    .clone()
                    .unwrap_or_default()
                    .into(keypair.public()),
            ))
            .into();

        let pubsub = {
            let pubsub_config = options.pubsub_config.clone().unwrap_or_default();
            let mut builder = libp2p::gossipsub::ConfigBuilder::default();

            if let Some(protocol) = pubsub_config.custom_protocol_id {
                builder.protocol_id(protocol, libp2p::gossipsub::Version::V1_1);
            }

            builder.max_transmit_size(pubsub_config.max_transmit_size);

            if pubsub_config.floodsub_compat {
                builder.support_floodsub();
            }

            builder.validation_mode(pubsub_config.validate.into());

            let config = builder.build().map_err(|e| anyhow::anyhow!("{}", e))?;

            let gossipsub = libp2p::gossipsub::Behaviour::new(
                libp2p::gossipsub::MessageAuthenticity::Signed(keypair.clone()),
                config,
            )
            .map_err(|e| anyhow::anyhow!("{}", e))?;

            protocols
                .pubsub
                .then_some(GossipsubStream::from(gossipsub))
                .into()
        };

        // Maybe have this enable in conjunction with RelayClient?
        let dcutr = Toggle::from(protocols.dcutr.then_some(Dcutr::new(peer_id)));
        let relay_config = options
            .relay_server_config
            .clone()
            .map(|rc| rc.into())
            .unwrap_or_default();

        let relay = Toggle::from(
            protocols
                .relay_server
                .then(|| Relay::new(peer_id, relay_config)),
        );

        let upnp = Toggle::from(protocols.upnp.then_some(libp2p_nat::Behaviour::default()));

        let (transport, relay_client, relay_manager) = match protocols.relay_client {
            true => {
                let (transport, client) = client::new(peer_id);
                let manager = libp2p_relay_manager::Behaviour::new(Default::default());
                (Some(transport), Some(client).into(), Some(manager).into())
            }
            false => (None, None.into(), None.into()),
        };

        let mut peerbook = peerbook::Behaviour::default();
        peerbook.set_connection_limit(limits);

        let addressbook =
            addressbook::Behaviour::with_config(options.addr_config.unwrap_or_default());

        let block_list = libp2p_allow_block_list::Behaviour::default();
        let protocol = protocol::Behaviour::default();
        let custom = Toggle::from(custom);

        let rendezvous_client = protocols
            .rendezvous_client
            .then_some(libp2p::rendezvous::client::Behaviour::new(keypair.clone()))
            .into();

        let rendezvous_server = protocols
            .rendezvous_server
            .then_some(libp2p::rendezvous::server::Behaviour::new(
                Default::default(),
            ))
            .into();
        Ok((
            Behaviour {
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
                upnp,
                peerbook,
                addressbook,
                protocol,
                custom,
                rendezvous_client,
                rendezvous_server,
            },
            transport,
        ))
    }

    pub fn add_peer(&mut self, peer: PeerId, addr: Multiaddr) {
        if !self.addressbook.contains(&peer, &addr) {
            self.addressbook.add_address(peer, addr.clone());
        }

        if let Some(kad) = self.kademlia.as_mut() {
            kad.add_address(&peer, addr);
        }
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

    pub fn notify_new_blocks(&self, blocks: Vec<crate::Block>) {
        if let Some(bitswap) = self.bitswap.as_ref() {
            let client = bitswap.client().clone();
            tokio::task::spawn(async move {
                let blocks = blocks
                    .iter()
                    .map(|block| beetle_bitswap_next::Block {
                        cid: *block.cid(),
                        data: Bytes::copy_from_slice(block.data()),
                    })
                    .collect::<Vec<_>>();
                if let Err(err) = client.notify_new_blocks(&blocks).await {
                    warn!("failed to notify bitswap about blocks: {:?}", err);
                }
            });
        }
    }

    pub fn pubsub(&mut self) -> Option<&mut GossipsubStream> {
        self.pubsub.as_mut()
    }

    pub fn bitswap(&mut self) -> Option<&mut Bitswap<Repo>> {
        self.bitswap.as_mut()
    }
}
