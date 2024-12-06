//! P2P handling for IPFS nodes.
use crate::error::Error;
use crate::repo::Repo;
use crate::rt::{Executor, ExecutorSwitch};
use crate::{IpfsOptions, TTransportFn};
use std::convert::TryInto;
use std::num::{NonZeroU8, NonZeroUsize};
use std::time::Duration;

use libp2p::gossipsub::ValidationMode;
use libp2p::identify::Info as IdentifyInfo;
use libp2p::identity::{Keypair, PublicKey};
use libp2p::request_response::ProtocolSupport;
use libp2p::swarm::NetworkBehaviour;
use libp2p::{Multiaddr, PeerId};
use libp2p::{StreamProtocol, Swarm};
use tracing::Span;

pub(crate) mod addr;
pub(crate) mod addressbook;
pub mod bitswap;
pub(crate) mod peerbook;
pub mod protocol;
pub(crate) mod rr_man;

mod behaviour;
pub use self::addressbook::Config as AddressBookConfig;
pub use self::behaviour::BehaviourEvent;
pub use self::behaviour::IdentifyConfiguration;

pub use self::behaviour::{KadConfig, KadInserts, KadStoreConfig};
pub use self::behaviour::{RateLimit, RelayConfig};
#[cfg(not(target_arch = "wasm32"))]
pub use self::transport::generate_cert;
pub use self::transport::{DnsResolver, TransportConfig, UpgradeVersion};
pub(crate) mod gossipsub;
mod request_response;
mod transport;

pub use addr::MultiaddrExt;
pub use behaviour::KadResult;

pub(crate) type TSwarm<C> = Swarm<behaviour::Behaviour<C>>;

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
        let peer_id = public_key.to_peer_id();
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

#[derive(Debug, Clone)]
pub struct RequestResponseConfig {
    pub protocol: String,
    pub timeout: Option<Duration>,
    pub max_request_size: usize,
    pub max_response_size: usize,
    pub concurrent_streams: Option<usize>,
    pub channel_buffer: usize,
    pub protocol_direction: RequestResponseDirection,
}

#[derive(Debug, Clone, Default)]
pub enum RequestResponseDirection {
    In,
    Out,
    #[default]
    Both,
}

impl From<RequestResponseDirection> for ProtocolSupport {
    fn from(direction: RequestResponseDirection) -> Self {
        match direction {
            RequestResponseDirection::In => ProtocolSupport::Inbound,
            RequestResponseDirection::Out => ProtocolSupport::Outbound,
            RequestResponseDirection::Both => ProtocolSupport::Full,
        }
    }
}

impl Default for RequestResponseConfig {
    fn default() -> Self {
        Self {
            protocol: "/ipfs/request-response".into(),
            timeout: None,
            max_request_size: 512 * 1024,
            max_response_size: 2 * 1024 * 1024,
            concurrent_streams: None,
            channel_buffer: 128,
            protocol_direction: RequestResponseDirection::default(),
        }
    }
}

#[derive(Clone)]
pub struct SwarmConfig {
    pub dial_concurrency_factor: NonZeroU8,
    pub notify_handler_buffer_size: NonZeroUsize,
    pub connection_event_buffer_size: usize,
    pub max_inbound_stream: usize,
}

impl Default for SwarmConfig {
    fn default() -> Self {
        Self {
            dial_concurrency_factor: 8.try_into().expect("8 > 0"),
            notify_handler_buffer_size: 32.try_into().expect("256 > 0"),
            connection_event_buffer_size: 7,
            max_inbound_stream: 10_000,
        }
    }
}

#[allow(clippy::type_complexity)]
#[allow(deprecated)]
//TODO: use libp2p::SwarmBuilder
/// Creates a new IPFS swarm.
pub(crate) fn create_swarm<C>(
    keypair: &Keypair,
    options: &IpfsOptions,
    executor: ExecutorSwitch,
    repo: &Repo,
    span: Span,
    (custom, custom_transport): (Option<C>, Option<TTransportFn>),
) -> Result<TSwarm<C>, Error>
where
    C: NetworkBehaviour,
    <C as NetworkBehaviour>::ToSwarm: std::fmt::Debug + Send,
{
    let keypair = keypair.clone();
    let peer_id = keypair.public().to_peer_id();

    let swarm_config = options.swarm_configuration.clone();
    let transport_config = options.transport_configuration.clone();

    let idle = options.connection_idle;

    let (behaviour, relay_transport) = behaviour::Behaviour::new(&keypair, options, repo, custom)?;

    // Set up an encrypted TCP transport over the Yamux. If relay transport is supplied, that will be apart
    let transport = match custom_transport {
        Some(transport) => transport(&keypair, relay_transport)?,
        None => transport::build_transport(keypair, relay_transport, transport_config)?,
    };

    let swarm = libp2p::Swarm::new(
        transport,
        behaviour,
        peer_id,
        libp2p::swarm::Config::with_executor(SpannedExecutor { executor, span })
            .with_notify_handler_buffer_size(swarm_config.notify_handler_buffer_size)
            .with_per_connection_event_buffer_size(swarm_config.connection_event_buffer_size)
            .with_dial_concurrency_factor(swarm_config.dial_concurrency_factor)
            .with_max_negotiating_inbound_streams(swarm_config.max_inbound_stream)
            .with_idle_connection_timeout(idle),
    );

    Ok(swarm)
}

struct SpannedExecutor {
    executor: ExecutorSwitch,
    span: Span,
}

impl libp2p::swarm::Executor for SpannedExecutor {
    fn exec(
        &self,
        future: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + 'static + Send>>,
    ) {
        use tracing_futures::Instrument;
        self.executor.dispatch(future.instrument(self.span.clone()));
    }
}
