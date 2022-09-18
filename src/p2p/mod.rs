//! P2P handling for IPFS nodes.
use std::convert::TryInto;
use std::num::{NonZeroU8, NonZeroUsize};

use crate::error::Error;
use crate::IpfsOptions;

use libp2p::identify::IdentifyInfo;
use libp2p::identity::{Keypair, PublicKey};
use libp2p::kad::KademliaConfig;
use libp2p::swarm::ConnectionLimits;
use libp2p::Swarm;
use libp2p::{Multiaddr, PeerId};
use tracing::Span;

pub(crate) mod addr;
mod behaviour;
pub use self::behaviour::BehaviourEvent;
pub use self::behaviour::{RelayConfig, RateLimit};
pub use self::transport::TransportConfig;
pub(crate) mod pubsub;
mod swarm;
mod transport;

pub use addr::{MultiaddrWithPeerId, MultiaddrWithoutPeerId};
pub use {behaviour::KadResult, swarm::Connection};

/// Type alias for [`libp2p::Swarm`] running the [`behaviour::Behaviour`] with the given [`IpfsTypes`].
pub type TSwarm = Swarm<behaviour::Behaviour>;

/// Abstraction of IdentifyInfo but includes PeerId
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeerInfo {
    /// The peer id of the user
    pub peer_id: PeerId,
    /// The public key of the local peer.
    pub public_key: PublicKey,
    /// Application-specific version of the protocol family used by the peer,
    /// e.g. `ipfs/1.0.0` or `polkadot/1.0.0`.
    pub protocol_version: String,
    /// Name and version of the peer, similar to the `User-Agent` header in
    /// the HTTP protocol.
    pub agent_version: String,
    /// The addresses that the peer is listening on.
    pub listen_addrs: Vec<Multiaddr>,
    /// The list of protocols supported by the peer, e.g. `/ipfs/ping/1.0.0`.
    pub protocols: Vec<String>,
    /// Address observed by or for the remote.
    pub observed_addr: Multiaddr,
}

impl From<IdentifyInfo> for PeerInfo {
    fn from(info: IdentifyInfo) -> Self {
        let IdentifyInfo {
            public_key,
            protocol_version,
            agent_version,
            listen_addrs,
            protocols,
            observed_addr,
        } = info;
        let peer_id = public_key.clone().into();
        Self {
            peer_id,
            public_key,
            protocol_version,
            agent_version,
            listen_addrs,
            protocols,
            observed_addr,
        }
    }
}

/// Defines the configuration for an IPFS swarm.
pub struct SwarmOptions {
    /// The keypair for the PKI based identity of the local node.
    pub keypair: Keypair,
    /// The peer address of the local node created from the keypair.
    pub peer_id: PeerId,
    /// The peers to connect to on startup.
    pub bootstrap: Vec<Multiaddr>,
    /// Enables mdns for peer discovery and announcement when true.
    pub mdns: bool,
    /// enables ipv6 for mdns
    pub mdns_ipv6: bool,
    /// Custom Kademlia protocol name, see [`IpfsOptions::kad_protocol`].
    pub kad_protocol: Option<String>,
    /// Relay Server
    pub relay_server: bool,
    /// Relay Server Configuration
    pub relay_server_config: Option<RelayConfig>,
    /// Kademlia 
    pub kad_config: Option<KademliaConfig>,
    /// Relay client
    pub relay: bool,
    /// Enables dcutr
    pub dcutr: bool,
}

impl From<&IpfsOptions> for SwarmOptions {
    fn from(options: &IpfsOptions) -> Self {
        let keypair = options.keypair.clone();
        let peer_id = keypair.public().to_peer_id();
        let bootstrap = options.bootstrap.clone();
        let mdns = options.mdns;
        let mdns_ipv6 = options.mdns_ipv6;
        let kad_protocol = options.kad_protocol.clone();
        let dcutr = options.dcutr;
        let relay_server = options.relay_server;
        let relay_server_config = options.relay_server_config.clone();
        let relay = options.relay;
        let kad_config = options.kad_configuration.clone();
        SwarmOptions {
            keypair,
            peer_id,
            bootstrap,
            mdns,
            mdns_ipv6,
            kad_protocol,
            relay_server,
            relay_server_config,
            relay,
            dcutr,
            kad_config
        }
    }
}

#[derive(Clone)]
pub struct SwarmConfig {
    pub connection: ConnectionLimits,
    pub dial_concurrency_factor: NonZeroU8,
    pub notify_handler_buffer_size: NonZeroUsize,
    pub connection_event_buffer_size: usize,
}

impl Default for SwarmConfig {
    fn default() -> Self {
        Self {
            connection: ConnectionLimits::default()
                .with_max_pending_incoming(Some(128))
                .with_max_pending_outgoing(Some(128))
                .with_max_established_incoming(Some(128))
                .with_max_established_outgoing(Some(128))
                .with_max_established_per_peer(Some(8)),
            dial_concurrency_factor: 8.try_into().expect("8 > 0"),
            notify_handler_buffer_size: 256.try_into().expect("256 > 0"),
            connection_event_buffer_size: 256,
        }
    }
}

/// Creates a new IPFS swarm.
pub async fn create_swarm(
    options: SwarmOptions,
    swarm_config: SwarmConfig,
    transport_config: TransportConfig,
    span: Span,
) -> Result<TSwarm, Error> {
    let peer_id = options.peer_id;

    let keypair = options.keypair.clone();

    // Create a Kademlia behaviour
    let (behaviour, relay_transport) = behaviour::build_behaviour(options).await?;

    // Set up an encrypted TCP transport over the Yamux and Mplex protocol. If relay transport is supplied, that will be apart
    let transport = transport::build_transport(keypair, relay_transport, transport_config)?;

    // Create a Swarm
    let swarm = libp2p::swarm::SwarmBuilder::new(transport, behaviour, peer_id)
        .connection_limits(swarm_config.connection)
        .notify_handler_buffer_size(swarm_config.notify_handler_buffer_size)
        .connection_event_buffer_size(swarm_config.connection_event_buffer_size)
        .dial_concurrency_factor(swarm_config.dial_concurrency_factor)
        .executor(Box::new(SpannedExecutor(span)))
        .build();

    Ok(swarm)
}

struct SpannedExecutor(Span);

impl libp2p::core::Executor for SpannedExecutor {
    fn exec(
        &self,
        future: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + 'static + Send>>,
    ) {
        use tracing_futures::Instrument;
        tokio::task::spawn(future.instrument(self.0.clone()));
    }
}
