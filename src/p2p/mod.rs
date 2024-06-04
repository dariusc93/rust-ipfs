//! P2P handling for IPFS nodes.
use std::convert::TryInto;
use std::num::{NonZeroU8, NonZeroUsize};

use crate::error::Error;
use crate::repo::Repo;
use crate::{IpfsOptions, TTransportFn};

use libp2p::gossipsub::ValidationMode;
use libp2p::identify::Info as IdentifyInfo;
use libp2p::identity::{Keypair, PublicKey};
use libp2p::swarm::NetworkBehaviour;
use libp2p::{Multiaddr, PeerId};
use libp2p::{StreamProtocol, Swarm};
use tracing::Span;

pub(crate) mod addr;
pub(crate) mod addressbook;
pub mod bitswap;
pub(crate) mod peerbook;
pub mod protocol;

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
pub async fn create_swarm<C>(
    keypair: &Keypair,
    options: &IpfsOptions,
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

    let (behaviour, relay_transport) =
        behaviour::Behaviour::new(&keypair, options, repo, custom).await?;

    // Set up an encrypted TCP transport over the Yamux. If relay transport is supplied, that will be apart
    let transport = match custom_transport {
        Some(transport) => transport(&keypair, relay_transport)?,
        None => transport::build_transport(keypair, relay_transport, transport_config)?,
    };

    let swarm = libp2p::Swarm::new(
        transport,
        behaviour,
        peer_id,
        libp2p::swarm::Config::with_executor(SpannedExecutor(span))
            .with_notify_handler_buffer_size(swarm_config.notify_handler_buffer_size)
            .with_per_connection_event_buffer_size(swarm_config.connection_event_buffer_size)
            .with_dial_concurrency_factor(swarm_config.dial_concurrency_factor)
            .with_max_negotiating_inbound_streams(swarm_config.max_inbound_stream)
            .with_idle_connection_timeout(idle),
    );

    Ok(swarm)
}

struct SpannedExecutor(Span);

impl libp2p::swarm::Executor for SpannedExecutor {
    fn exec(
        &self,
        future: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + 'static + Send>>,
    ) {
        use tracing_futures::Instrument;
        crate::rt::spawn(future.instrument(self.0.clone()));
    }
}
