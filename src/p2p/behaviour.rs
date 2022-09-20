#[cfg(not(feature = "external-gossipsub-stream"))]
use super::pubsub::Pubsub;

#[cfg(feature = "external-gossipsub-stream")]
use libp2p_helper::gossipsub::GossipsubStream;
use serde::{Deserialize, Serialize};

use super::swarm::{Connection, SwarmApi};
use crate::config::BOOTSTRAP_NODES;
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
use libp2p::identify::{Identify, IdentifyConfig, IdentifyEvent};
use libp2p::kad::record::{store::MemoryStore, Record};
use libp2p::kad::{Kademlia, KademliaConfig, KademliaEvent};
use libp2p::mdns::{MdnsConfig, MdnsEvent, TokioMdns as Mdns};
use libp2p::ping::{Ping, PingEvent};
use libp2p::relay::v2::client::transport::ClientTransport;
use libp2p::relay::v2::client::{Client as RelayClient, Event as RelayClientEvent};
use libp2p::relay::v2::relay::{rate_limiter, Event as RelayEvent, Relay};
use libp2p::swarm::behaviour::toggle::Toggle;
use libp2p::swarm::NetworkBehaviour;
use std::convert::TryFrom;
use std::convert::TryInto;
use std::num::NonZeroU32;

/// Behaviour type.
#[derive(libp2p::NetworkBehaviour)]
#[behaviour(out_event = "BehaviourEvent", event_process = false)]
pub struct Behaviour {
    pub mdns: Toggle<Mdns>,
    pub kademlia: Kademlia<MemoryStore>,
    pub bitswap: Bitswap,
    pub ping: Ping,
    pub identify: Identify,
    #[cfg(not(feature = "external-gossipsub-stream"))]
    pub pubsub: Pubsub,
    #[cfg(feature = "external-gossipsub-stream")]
    pub pubsub: GossipsubStream,
    pub autonat: autonat::Behaviour,
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

impl Behaviour {
    /// Create a Kademlia behaviour with the IPFS bootstrap nodes.
    pub async fn new(options: SwarmOptions) -> Result<(Self, Option<ClientTransport>), Error> {
        info!("net: starting with peer id {}", options.peer_id);

        let mdns = if options.mdns {
            let config = MdnsConfig {
                enable_ipv6: options.mdns_ipv6,
                ..Default::default()
            };
            Mdns::new(config).await.ok()
        } else {
            None
        }
        .into();

        let store = MemoryStore::new(options.peer_id.to_owned());

        let mut kad_config = match options.kad_config.clone() {
            Some(config) => config,
            None => {
                let mut kad_config = KademliaConfig::default();
                kad_config.disjoint_query_paths(true);
                kad_config.set_query_timeout(std::time::Duration::from_secs(300));
                kad_config
            }
        };

        if let Some(protocol) = options.kad_protocol {
            kad_config.set_protocol_names(std::iter::once(protocol.into_bytes().into()).collect());
        }

        let mut kademlia = Kademlia::with_config(options.peer_id.to_owned(), store, kad_config);

        for addr in &options.bootstrap {
            let addr = MultiaddrWithPeerId::try_from(addr.clone())?;
            kademlia.add_address(&addr.peer_id, addr.multiaddr.as_ref().clone());
        }
        let autonat = autonat::Behaviour::new(options.peer_id.to_owned(), Default::default());
        let bitswap = Bitswap::default();
        let ping = Ping::default();
        let peer_id = options.keypair.public().into();

        //TODO: Provide custom protocol and agent name via IpfsOptions
        let identify = Identify::new(
            IdentifyConfig::new("/ipfs/0.1.0".into(), options.keypair.public())
                .with_agent_version("rust-ipfs".into()),
        );

        #[cfg(not(feature = "external-gossipsub-stream"))]
        let pubsub = Pubsub::new(options.keypair)?;

        #[cfg(feature = "external-gossipsub-stream")]
        let pubsub = {
            let config = gossipsub::GossipsubConfigBuilder::default()
                .max_transmit_size(256 * 1024)
                .build()
                .map_err(|e| anyhow::anyhow!("{}", e))?;
            let gossipsub = Gossipsub::new(MessageAuthenticity::Signed(option.keypair), config)
                .map_err(|e| anyhow::anyhow!("{}", e))?;
            GossipsubStream::from(gossipsub)
        };

        let mut swarm = SwarmApi::default();

        for addr in &options.bootstrap {
            if let Ok(addr) = addr.to_owned().try_into() {
                swarm.bootstrappers.insert(addr);
            }
        }

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
                ping,
                identify,
                autonat,
                pubsub,
                swarm,
                dcutr,
                relay,
                relay_client,
            },
            transport,
        ))
    }

    pub fn add_peer(&mut self, peer: PeerId, addr: Multiaddr) {
        self.kademlia.add_address(&peer, addr);
        self.swarm.add_peer(peer);
        // FIXME: the call below automatically performs a dial attempt
        // to the given peer; it is unsure that we want it done within
        // add_peer, especially since that peer might not belong to the
        // expected identify protocol
        self.pubsub.add_explicit_peer(&peer);
        // TODO self.bitswap.add_node_to_partial_view(peer);
    }

    pub fn remove_peer(&mut self, peer: &PeerId) {
        self.swarm.remove_peer(peer);
        self.pubsub.remove_explicit_peer(peer);
        // TODO self.bitswap.remove_peer(&peer);
    }

    pub fn addrs(&mut self) -> Vec<(PeerId, Vec<Multiaddr>)> {
        let peers = self.swarm.peers().cloned().collect::<Vec<_>>();
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

    pub fn connect(&mut self, addr: MultiaddrWithPeerId) -> Option<SubscriptionFuture<(), String>> {
        self.swarm.connect(addr)
    }

    // FIXME: it would be best if get_providers is called only in case the already connected
    // peers don't have it
    pub fn want_block(&mut self, cid: Cid) {
        let key = cid.hash().to_bytes();
        self.kademlia.get_providers(key.into());
        self.bitswap.want_block(cid, 1);
    }

    pub fn stop_providing_block(&mut self, cid: &Cid) {
        info!("Finished providing block {}", cid.to_string());
        //let hash = Multihash::from_bytes(cid.to_bytes()).unwrap();
        //self.kademlia.remove_providing(&hash);
    }

    #[cfg(not(feature = "external-gossipsub-stream"))]
    pub fn pubsub(&mut self) -> &mut Pubsub {
        &mut self.pubsub
    }

    #[cfg(feature = "external-gossipsub-stream")]
    pub fn pubsub(&mut self) -> &mut GossipsubStream {
        &mut self.pubsub
    }

    pub fn bitswap(&mut self) -> &mut Bitswap {
        &mut self.bitswap
    }

    pub fn kademlia(&mut self) -> &mut Kademlia<MemoryStore> {
        &mut self.kademlia
    }

    pub fn get_bootstrappers(&self) -> Vec<Multiaddr> {
        self.swarm
            .bootstrappers
            .iter()
            .cloned()
            .map(|a| a.into())
            .collect()
    }

    pub fn add_bootstrapper(
        &mut self,
        addr: MultiaddrWithPeerId,
    ) -> Result<Multiaddr, anyhow::Error> {
        let ret = addr.clone().into();
        if self.swarm.bootstrappers.insert(addr.clone()) {
            let MultiaddrWithPeerId {
                multiaddr: ma,
                peer_id,
            } = addr;
            self.kademlia.add_address(&peer_id, ma.into());
            // the return value of add_address doesn't implement Debug
            trace!(peer_id=%peer_id, "tried to add a bootstrapper");
        }
        Ok(ret)
    }

    pub fn remove_bootstrapper(
        &mut self,
        addr: MultiaddrWithPeerId,
    ) -> Result<Multiaddr, anyhow::Error> {
        let ret = addr.clone().into();
        if self.swarm.bootstrappers.remove(&addr) {
            let peer_id = addr.peer_id;
            let prefix: Multiaddr = addr.multiaddr.into();

            if let Some(e) = self.kademlia.remove_address(&peer_id, &prefix) {
                info!(peer_id=%peer_id, status=?e.status, "removed bootstrapper");
            } else {
                warn!(peer_id=%peer_id, "attempted to remove an unknown bootstrapper");
            }
        }
        Ok(ret)
    }

    pub fn clear_bootstrappers(&mut self) -> Vec<Multiaddr> {
        let removed = self.swarm.bootstrappers.drain();
        let mut ret = Vec::with_capacity(removed.len());

        for addr_with_peer_id in removed {
            let peer_id = &addr_with_peer_id.peer_id;
            let prefix: Multiaddr = addr_with_peer_id.multiaddr.clone().into();

            if let Some(e) = self.kademlia.remove_address(peer_id, &prefix) {
                info!(peer_id=%peer_id, status=?e.status, "cleared bootstrapper");
                ret.push(addr_with_peer_id.into());
            } else {
                error!(peer_id=%peer_id, "attempted to clear an unknown bootstrapper");
            }
        }

        ret
    }

    pub fn restore_bootstrappers(&mut self) -> Result<Vec<Multiaddr>, anyhow::Error> {
        let mut ret = Vec::new();

        for addr in BOOTSTRAP_NODES {
            let addr = addr
                .parse::<MultiaddrWithPeerId>()
                .expect("see test bootstrap_nodes_are_multiaddr_with_peerid");
            if self.swarm.bootstrappers.insert(addr.clone()) {
                let MultiaddrWithPeerId {
                    multiaddr: ma,
                    peer_id,
                } = addr.clone();

                // this is intentionally the multiaddr without peerid turned into plain multiaddr:
                // libp2p cannot dial addresses which include peerids.
                let ma: Multiaddr = ma.into();

                // same as with add_bootstrapper: the return value from kademlia.add_address
                // doesn't implement Debug
                self.kademlia.add_address(&peer_id, ma.clone());
                trace!(peer_id=%peer_id, "tried to restore a bootstrapper");

                // report with the peerid
                let reported: Multiaddr = addr.into();
                ret.push(reported);
            }
        }

        Ok(ret)
    }
}

/// Create a IPFS behaviour with the IPFS bootstrap nodes.
pub async fn build_behaviour(
    options: SwarmOptions,
) -> Result<(Behaviour, Option<ClientTransport>), Error> {
    Behaviour::new(options).await
}
