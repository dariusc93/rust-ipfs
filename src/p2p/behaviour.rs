use super::gossipsub::GossipsubStream;
use serde::{Deserialize, Serialize};

use super::swarm::{Connection, SwarmApi};
use crate::error::Error;
use crate::p2p::{MultiaddrWithPeerId, SwarmOptions};
use crate::subscription::SubscriptionFuture;

// use cid::Cid;
use ipfs_bitswap::{Bitswap, BitswapEvent};
use libipld::Cid;
use libp2p::autonat;
use libp2p::core::{Multiaddr, PeerId};
use libp2p::dcutr::behaviour::{Behaviour as Dcutr, Event as DcutrEvent};
use libp2p::gossipsub::GossipsubEvent;
use libp2p::identify::{Behaviour as Identify, Config as IdentifyConfig, Event as IdentifyEvent};
use libp2p::kad::record::{
    store::{MemoryStore, MemoryStoreConfig},
    Record,
};
use libp2p::kad::{Kademlia, KademliaConfig, KademliaEvent};
use libp2p::mdns::{tokio::Behaviour as Mdns, Config as MdnsConfig, Event as MdnsEvent};
use libp2p::ping::{Behaviour as Ping, Event as PingEvent};
use libp2p::relay::v2::client::transport::ClientTransport;
use libp2p::relay::v2::client::{Client as RelayClient, Event as RelayClientEvent};
use libp2p::relay::v2::relay::{rate_limiter, Event as RelayEvent, Relay};
use libp2p::swarm::behaviour::toggle::Toggle;
use libp2p::swarm::keep_alive::Behaviour as KeepAliveBehaviour;
use libp2p::swarm::NetworkBehaviour;
use std::convert::TryFrom;
use std::num::NonZeroU32;
use std::time::Duration;

/// Behaviour type.
#[derive(NetworkBehaviour)]
#[behaviour(out_event = "BehaviourEvent", event_process = false)]
pub struct Behaviour {
    pub mdns: Toggle<Mdns>,
    pub kademlia: Kademlia<MemoryStore>,
    pub bitswap: Bitswap,
    pub ping: Ping,
    pub identify: Identify,
    pub keepalive: Toggle<KeepAliveBehaviour>,
    pub pubsub: GossipsubStream,
    pub autonat: autonat::Behaviour,
    pub upnp: Toggle<libp2p_nat::Behaviour>,
    pub relay: Toggle<Relay>,
    pub relay_client: Toggle<RelayClient>,
    pub dcutr: Toggle<Dcutr>,
    pub swarm: SwarmApi,
}

#[derive(Debug)]
pub enum BehaviourEvent {
    Mdns(MdnsEvent),
    Kad(KademliaEvent),
    Bitswap(BitswapEvent),
    Ping(PingEvent),
    Identify(IdentifyEvent),
    Gossipsub(GossipsubEvent),
    Autonat(autonat::Event),
    Relay(RelayEvent),
    RelayClient(RelayClientEvent),
    Dcutr(DcutrEvent),
    Void(void::Void),
}

impl From<MdnsEvent> for BehaviourEvent {
    fn from(event: MdnsEvent) -> Self {
        BehaviourEvent::Mdns(event)
    }
}

impl From<KademliaEvent> for BehaviourEvent {
    fn from(event: KademliaEvent) -> Self {
        BehaviourEvent::Kad(event)
    }
}

impl From<BitswapEvent> for BehaviourEvent {
    fn from(event: BitswapEvent) -> Self {
        BehaviourEvent::Bitswap(event)
    }
}

impl From<PingEvent> for BehaviourEvent {
    fn from(event: PingEvent) -> Self {
        BehaviourEvent::Ping(event)
    }
}

impl From<IdentifyEvent> for BehaviourEvent {
    fn from(event: IdentifyEvent) -> Self {
        BehaviourEvent::Identify(event)
    }
}

impl From<GossipsubEvent> for BehaviourEvent {
    fn from(event: GossipsubEvent) -> Self {
        BehaviourEvent::Gossipsub(event)
    }
}

impl From<autonat::Event> for BehaviourEvent {
    fn from(event: autonat::Event) -> Self {
        BehaviourEvent::Autonat(event)
    }
}

impl From<RelayEvent> for BehaviourEvent {
    fn from(event: RelayEvent) -> Self {
        BehaviourEvent::Relay(event)
    }
}

impl From<RelayClientEvent> for BehaviourEvent {
    fn from(event: RelayClientEvent) -> Self {
        BehaviourEvent::RelayClient(event)
    }
}

impl From<DcutrEvent> for BehaviourEvent {
    fn from(event: DcutrEvent) -> Self {
        BehaviourEvent::Dcutr(event)
    }
}

impl From<void::Void> for BehaviourEvent {
    fn from(event: void::Void) -> Self {
        BehaviourEvent::Void(event)
    }
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
    pub initial_delay: Duration,
    pub interval: Duration,
    pub push_update: bool,
    pub cache: usize,
}

impl Default for IdentifyConfiguration {
    fn default() -> Self {
        Self {
            protocol_version: "/ipfs/0.1.0".into(),
            agent_version: "rust-ipfs".into(),
            initial_delay: Duration::from_millis(500),
            interval: Duration::from_secs(5 * 60),
            push_update: false,
            cache: 0,
        }
    }
}

impl IdentifyConfiguration {
    pub fn into(self, publuc_key: libp2p::identity::PublicKey) -> IdentifyConfig {
        IdentifyConfig::new(self.protocol_version, publuc_key)
            .with_agent_version(self.agent_version)
            .with_initial_delay(self.initial_delay)
            .with_interval(self.interval)
            .with_push_listen_addr_updates(self.push_update)
            .with_cache_size(self.cache)
    }
}

impl From<RelayConfig> for libp2p::relay::v2::relay::Config {
    fn from(
        RelayConfig {
            max_reservations,
            max_reservations_per_peer,
            reservation_duration,
            reservation_rate_limiters,
            max_circuits,
            max_circuits_per_peer,
            max_circuit_duration,
            max_circuit_bytes,
            circuit_src_rate_limiters,
        }: RelayConfig,
    ) -> Self {
        let reservation_rate_limiters = reservation_rate_limiters
            .iter()
            .map(|rate| match rate {
                RateLimit::PerPeer { limit, interval } => {
                    rate_limiter::new_per_peer(rate_limiter::GenericRateLimiterConfig {
                        limit: *limit,
                        interval: *interval,
                    })
                }
                RateLimit::PerIp { limit, interval } => {
                    rate_limiter::new_per_ip(rate_limiter::GenericRateLimiterConfig {
                        limit: *limit,
                        interval: *interval,
                    })
                }
            })
            .collect::<Vec<_>>();

        let circuit_src_rate_limiters = circuit_src_rate_limiters
            .iter()
            .map(|rate| match rate {
                RateLimit::PerPeer { limit, interval } => {
                    rate_limiter::new_per_peer(rate_limiter::GenericRateLimiterConfig {
                        limit: *limit,
                        interval: *interval,
                    })
                }
                RateLimit::PerIp { limit, interval } => {
                    rate_limiter::new_per_ip(rate_limiter::GenericRateLimiterConfig {
                        limit: *limit,
                        interval: *interval,
                    })
                }
            })
            .collect::<Vec<_>>();

        libp2p::relay::v2::relay::Config {
            max_reservations,
            max_reservations_per_peer,
            reservation_duration,
            reservation_rate_limiters,
            max_circuits,
            max_circuits_per_peer,
            max_circuit_duration,
            max_circuit_bytes,
            circuit_src_rate_limiters,
        }
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

impl Behaviour {
    /// Create a Kademlia behaviour with the IPFS bootstrap nodes.
    pub async fn new(options: SwarmOptions) -> Result<(Self, Option<ClientTransport>), Error> {
        let peer_id = options.peer_id;

        info!("net: starting with peer id {}", peer_id);

        let mdns = if options.mdns {
            let config = MdnsConfig {
                enable_ipv6: options.mdns_ipv6,
                ..Default::default()
            };
            Mdns::new(config).ok()
        } else {
            None
        }
        .into();

        let store = {
            //TODO: Make customizable
            //TODO: Use persistent store for kad
            let config = options
                .kad_store_config
                .unwrap_or_default()
                .memory
                .unwrap_or_default();

            MemoryStore::with_config(peer_id, config)
        };

        let kad_config = match options.kad_config.clone() {
            Some(config) => config,
            None => {
                let mut kad_config = KademliaConfig::default();
                kad_config.disjoint_query_paths(true);
                kad_config.set_query_timeout(std::time::Duration::from_secs(300));
                kad_config
            }
        };

        let mut kademlia = Kademlia::with_config(peer_id, store, kad_config);

        for addr in &options.bootstrap {
            let addr = MultiaddrWithPeerId::try_from(addr.clone())?;
            kademlia.add_address(&addr.peer_id, addr.multiaddr.as_ref().clone());
        }

        let autonat = autonat::Behaviour::new(options.peer_id.to_owned(), Default::default());
        let bitswap = Bitswap::default();
        let keepalive = options.keep_alive.then(KeepAliveBehaviour::default).into();

        let ping = Ping::new(options.ping_config.unwrap_or_default());

        let identify = Identify::new(
            options
                .identify_config
                .unwrap_or_default()
                .into(options.keypair.public()),
        );

        let pubsub = {
            let config = libp2p::gossipsub::GossipsubConfigBuilder::default()
                .max_transmit_size(512 * 1024)
                .build()
                .map_err(|e| anyhow::anyhow!("{}", e))?;
            let gossipsub = libp2p::gossipsub::Gossipsub::new(
                libp2p::gossipsub::MessageAuthenticity::Signed(options.keypair),
                config,
            )
            .map_err(|e| anyhow::anyhow!("{}", e))?;
            GossipsubStream::from(gossipsub)
        };

        let swarm = SwarmApi::default();

        // Maybe have this enable in conjunction with RelayClient?
        let dcutr = Toggle::from(options.dcutr.then(Dcutr::new));
        let relay_config = options
            .relay_server_config
            .map(|rc| rc.into())
            .unwrap_or_default();

        let relay = Toggle::from(
            options
                .relay_server
                .then(|| Relay::new(peer_id, relay_config)),
        );

        let upnp = Toggle::from(options.portmapping.then_some(libp2p_nat::Behaviour::new().await?));

        let (transport, relay_client) = match options.relay {
            true => {
                let (transport, client) = RelayClient::new_transport_and_behaviour(peer_id);
                (Some(transport), Some(client).into())
            }
            false => (None, None.into()),
        };

        Ok((
            Behaviour {
                mdns,
                kademlia,
                bitswap,
                keepalive,
                ping,
                identify,
                autonat,
                pubsub,
                swarm,
                dcutr,
                relay,
                relay_client,
                upnp,
            },
            transport,
        ))
    }

    pub fn add_peer(&mut self, peer: PeerId, addr: Option<Multiaddr>) {
        if let Some(addr) = addr {
            self.kademlia.add_address(&peer, addr);
        }
        self.swarm.add_peer(peer);
        self.pubsub.add_explicit_peer(&peer);
        self.bitswap.connect(peer);
    }

    pub fn remove_peer(&mut self, peer: &PeerId) {
        self.swarm.remove_peer(peer);
        self.pubsub.remove_explicit_peer(peer);
        self.kademlia.remove_peer(peer);
        // TODO self.bitswap.remove_peer(&peer);
    }

    pub fn addrs(&mut self) -> Vec<(PeerId, Vec<Multiaddr>)> {
        let peers = self
            .swarm
            .peers()
            .map(|(k, _)| k)
            .cloned()
            .collect::<Vec<_>>();
        let mut addrs = Vec::with_capacity(peers.len());
        for peer_id in peers.into_iter() {
            let peer_addrs = self.addresses_of_peer(&peer_id);
            addrs.push((peer_id, peer_addrs));
        }
        addrs
    }

    pub fn connections(&self) -> impl Iterator<Item = Connection> + '_ {
        self.swarm.connections()
    }

    pub fn connect(
        &mut self,
        addr: MultiaddrWithPeerId,
    ) -> Option<Option<SubscriptionFuture<(), String>>> {
        self.swarm.connect(addr)
    }

    // FIXME: it would be best if get_providers is called only in case the already connected
    // peers don't have it
    pub fn want_block(&mut self, cid: Cid) {
        // TODO: Restructure this to utilize provider propertly
        let key = cid.hash().to_bytes();
        self.kademlia.get_providers(key.into());
        self.bitswap.want_block(cid, 1);
    }

    pub fn stop_providing_block(&mut self, cid: &Cid) {
        info!("Finished providing block {}", cid.to_string());
        let key = cid.hash().to_bytes();
        self.kademlia.stop_providing(&key.into());
    }

    pub fn supported_protocols(&self) -> Vec<String> {
        self.swarm.protocols().collect::<Vec<_>>()
    }

    pub fn pubsub(&mut self) -> &mut GossipsubStream {
        &mut self.pubsub
    }

    pub fn bitswap(&mut self) -> &mut Bitswap {
        &mut self.bitswap
    }

    pub fn kademlia(&mut self) -> &mut Kademlia<MemoryStore> {
        &mut self.kademlia
    }
}

/// Create a IPFS behaviour with the IPFS bootstrap nodes.
pub async fn build_behaviour(
    options: SwarmOptions,
) -> Result<(Behaviour, Option<ClientTransport>), Error> {
    Behaviour::new(options).await
}
