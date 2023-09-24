//! P2P handling for IPFS nodes.
use std::convert::TryInto;
use std::num::{NonZeroU8, NonZeroUsize};
use std::time::Duration;

use crate::error::Error;
use crate::repo::Repo;
use crate::{IpfsOptions, TTransportFn};

use either::Either;
use libp2p::gossipsub::ValidationMode;
use libp2p::identify::Info as IdentifyInfo;
use libp2p::identity::{Keypair, PublicKey};
use libp2p::kad::KademliaConfig;
use libp2p::ping::Config as PingConfig;
use libp2p::swarm::NetworkBehaviour;
use libp2p::{Multiaddr, PeerId};
use libp2p::{StreamProtocol, Swarm};
use tracing::Span;

pub(crate) mod addr;
pub(crate) mod addressbook;
pub(crate) mod peerbook;
pub mod protocol;

mod behaviour;
pub use self::addressbook::Config as AddressBookConfig;
pub use self::behaviour::BehaviourEvent;
pub use self::behaviour::IdentifyConfiguration;
pub use self::behaviour::{BitswapConfig, BitswapProtocol};
pub use self::behaviour::{KadConfig, KadInserts, KadStoreConfig};
pub use self::behaviour::{RateLimit, RelayConfig};
pub use self::peerbook::ConnectionLimits;
pub use self::transport::{
    DnsResolver, MultiPlexOption, TransportConfig, UpdateMode, UpgradeVersion,
};
pub(crate) mod gossipsub;
mod transport;

pub use addr::MultiaddrExt;
pub use behaviour::KadResult;

/// Type alias for [`libp2p::Swarm`] running the [`behaviour::Behaviour`] with the given [`IpfsTypes`].
pub type TSwarm<C> = Swarm<behaviour::Behaviour<C>>;

/// Abstraction of IdentifyInfo but includes PeerId
#[derive(Clone, Debug, Eq)]
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
    pub protocols: Vec<StreamProtocol>,

    /// Address observed by or for the remote.
    pub observed_addr: Option<Multiaddr>,
}

impl core::hash::Hash for PeerInfo {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.peer_id.hash(state);
        self.public_key.hash(state);
    }
}

impl PartialEq for PeerInfo {
    fn eq(&self, other: &Self) -> bool {
        self.peer_id == other.peer_id && self.public_key == other.public_key
    }
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
        let observed_addr = Some(observed_addr);
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
    /// The peers to connect to on startup.
    pub bootstrap: Vec<Multiaddr>,
    /// Enables mdns for peer discovery and announcement when true.
    pub mdns: bool,
    /// enables ipv6 for mdns
    pub mdns_ipv6: bool,
    /// disable kad
    pub disable_kad: bool,
    /// disable bitswap
    pub disable_bitswap: bool,
    /// Relay Server
    pub relay_server: bool,
    /// Relay Server Configuration
    pub relay_server_config: Option<RelayConfig>,
    /// Kademlia Configuration
    pub kad_config: Option<Either<KadConfig, KademliaConfig>>,
    /// Ping Configuration
    pub ping_config: Option<PingConfig>,
    /// bitswap config
    pub bitswap_config: Option<BitswapConfig>,
    /// identify configuration
    pub identify_config: Option<IdentifyConfiguration>,
    /// Kad store config
    /// Note: Only supports MemoryStoreConfig at this time
    pub kad_store_config: KadStoreConfig,
    /// Pubsub configuration,
    pub pubsub_config: Option<PubsubConfig>,
    /// addressbook config
    pub addrbook_config: Option<AddressBookConfig>,
    /// UPnP/PortMapping
    pub portmapping: bool,
    /// Relay client
    pub relay: bool,
    /// Enables dcutr
    pub dcutr: bool,
    /// Connection idle
    pub connection_idle: Duration,
    pub rendezvous_client: bool,
    pub rendezvous_server: bool,
}

impl From<&IpfsOptions> for SwarmOptions {
    fn from(options: &IpfsOptions) -> Self {
        let bootstrap = options.bootstrap.clone();
        let mdns = options.mdns;
        let mdns_ipv6 = options.mdns_ipv6;
        let dcutr = options.dcutr;
        let relay_server = options.relay_server;
        let relay_server_config = options.relay_server_config.clone();
        let relay = options.relay;
        let kad_config = options.kad_configuration.clone();
        let ping_config = options.ping_configuration.clone();
        let kad_store_config = options.kad_store_config.clone();
        let disable_kad = options.disable_kad;
        let disable_bitswap = options.disable_bitswap;
        let bitswap_config = options.bitswap_config.clone();

        let identify_config = options.identify_configuration.clone();
        let portmapping = options.port_mapping;
        let pubsub_config = options.pubsub_config.clone();
        let addrbook_config = options.addr_config;

        let connection_idle = options.connection_idle;
        let rendezvous_client = options.rendezvous_client;
        let rendezvous_server = options.rendezvous_server;

        SwarmOptions {
            bootstrap,
            mdns,
            disable_kad,
            disable_bitswap,
            bitswap_config,
            mdns_ipv6,
            relay_server,
            relay_server_config,
            relay,
            dcutr,
            kad_config,
            kad_store_config,
            ping_config,
            identify_config,
            portmapping,
            addrbook_config,
            pubsub_config,
            connection_idle,
            rendezvous_client,
            rendezvous_server,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubsubConfig {
    /// Custom protocol name
    pub custom_protocol_id: Option<String>,

    /// Max size that can be transmitted over gossipsub
    pub max_transmit_size: usize,

    /// Floodsub compatibility
    pub floodsub_compat: bool,

    /// Validation
    pub validate: PubsubValidation,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum PubsubValidation {
    /// See [`ValidationMode::Strict`]
    Strict,

    /// See [`ValidationMode::Permissive`]
    Permissive,

    /// See [`ValidationMode::Anonymous`]
    Anonymous,

    /// See [`ValidationMode::None`]
    Relaxed,
}

impl From<PubsubValidation> for ValidationMode {
    fn from(validation: PubsubValidation) -> Self {
        match validation {
            PubsubValidation::Strict => ValidationMode::Strict,
            PubsubValidation::Permissive => ValidationMode::Permissive,
            PubsubValidation::Anonymous => ValidationMode::Anonymous,
            PubsubValidation::Relaxed => ValidationMode::None,
        }
    }
}

impl Default for PubsubConfig {
    fn default() -> Self {
        Self {
            custom_protocol_id: None,
            max_transmit_size: 2 * 1024 * 1024,
            validate: PubsubValidation::Strict,
            floodsub_compat: false,
        }
    }
}

#[derive(Clone)]
pub struct SwarmConfig {
    pub connection: ConnectionLimits,
    pub dial_concurrency_factor: NonZeroU8,
    pub notify_handler_buffer_size: NonZeroUsize,
    pub connection_event_buffer_size: usize,
    pub max_inbound_stream: usize,
}

impl Default for SwarmConfig {
    fn default() -> Self {
        Self {
            connection: ConnectionLimits::default(),
            dial_concurrency_factor: 8.try_into().expect("8 > 0"),
            notify_handler_buffer_size: 32.try_into().expect("256 > 0"),
            connection_event_buffer_size: 7,
            max_inbound_stream: 10_000,
        }
    }
}

#[allow(clippy::type_complexity)]
/// Creates a new IPFS swarm.
pub async fn create_swarm<C>(
    keypair: &Keypair,
    options: SwarmOptions,
    swarm_config: SwarmConfig,
    transport_config: TransportConfig,
    repo: Repo,
    span: Span,
    (custom, custom_transport): (Option<C>, Option<TTransportFn>),
) -> Result<TSwarm<C>, Error>
where
    C: NetworkBehaviour,
    <C as NetworkBehaviour>::ToSwarm: std::fmt::Debug + Send,
{
    let keypair = keypair.clone();
    let peer_id = keypair.public().to_peer_id();

    let idle = options.connection_idle;

    let (behaviour, relay_transport) =
        behaviour::build_behaviour(&keypair, options, repo, swarm_config.connection, custom)
            .await?;

    // Set up an encrypted TCP transport over the Yamux and Mplex protocol. If relay transport is supplied, that will be apart
    let transport = match custom_transport {
        Some(transport) => transport(&keypair, relay_transport)?,
        None => transport::build_transport(keypair, relay_transport, transport_config)?,
    };

    // Create a Swarm
    let swarm = libp2p::swarm::SwarmBuilder::with_executor(
        transport,
        behaviour,
        peer_id,
        SpannedExecutor(span),
    )
    .notify_handler_buffer_size(swarm_config.notify_handler_buffer_size)
    .per_connection_event_buffer_size(swarm_config.connection_event_buffer_size)
    .dial_concurrency_factor(swarm_config.dial_concurrency_factor)
    .max_negotiating_inbound_streams(swarm_config.max_inbound_stream)
    .idle_connection_timeout(idle)
    .build();

    Ok(swarm)
}

struct SpannedExecutor(Span);

impl libp2p::swarm::Executor for SpannedExecutor {
    fn exec(
        &self,
        future: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + 'static + Send>>,
    ) {
        use tracing_futures::Instrument;
        tokio::task::spawn(future.instrument(self.0.clone()));
    }
}
