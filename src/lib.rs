//! IPFS node implementation
//!
//! [Ipfs](https://ipfs.io) is a peer-to-peer system with content addressed functionality. The main
//! entry point for users of this crate is the [`Ipfs`] facade, which allows access to most of the
//! implemented functionality.
//!
//! This crate passes a lot of the [interface-ipfs-core] test suite; most of that functionality is
//! in `ipfs-http` crate. The crate has some interoperability with the [go-ipfs] and [js-ipfs]
//! implementations.
//!
//! `ipfs` is an early alpha level crate: APIs and their implementation are subject to change in
//! any upcoming release at least for now. The aim of the crate is to become a library-first
//! production ready implementation of an Ipfs node.
//!
//! [interface-ipfs-core]: https://www.npmjs.com/package/interface-ipfs-core
//! [go-ipfs]: https://github.com/ipfs/go-ipfs/
//! [js-ipfs]: https://github.com/ipfs/js-ipfs/
// We are not done yet, but uncommenting this makes it easier to hunt down for missing docs.
//#![deny(missing_docs)]
//
// This isn't recognized in stable yet, but we should disregard any nags on these to keep making
// the docs better.
//#![allow(private_intra_doc_links)]

pub mod config;
pub mod dag;
pub mod error;
pub mod ipns;
mod keystore;
pub mod p2p;
pub mod path;
pub mod refs;
pub mod repo;
mod task;
pub mod unixfs;

#[macro_use]
extern crate tracing;

use anyhow::{anyhow, format_err};
use either::Either;
use futures::{
    channel::{
        mpsc::{channel, Sender, UnboundedReceiver},
        oneshot::{self, channel as oneshot_channel, Sender as OneshotSender},
    },
    future::BoxFuture,
    sink::SinkExt,
    stream::{BoxStream, Stream},
    StreamExt,
};

use keystore::Keystore;
use p2p::{
    BitswapConfig, IdentifyConfiguration, KadConfig, KadStoreConfig, PeerInfo, PubsubConfig,
    RelayConfig,
};
use repo::{BlockStore, DataStore, Lock};
use tokio::task::JoinHandle;
use tracing::Span;
use tracing_futures::Instrument;
use unixfs::{IpfsUnixfs, NodeItem, UnixfsStatus};

use std::{
    collections::{HashMap, HashSet},
    fmt, io,
    ops::{Deref, DerefMut, Range},
    path::{Path, PathBuf},
    sync::atomic::AtomicU64,
    sync::Arc,
    time::Duration,
};

use self::{
    dag::IpldDag,
    ipns::Ipns,
    p2p::{create_swarm, SwarmOptions, TSwarm},
    repo::Repo,
};

pub use self::p2p::gossipsub::SubscriptionStream;

pub use self::{
    error::Error,
    p2p::BehaviourEvent,
    p2p::KadResult,
    path::IpfsPath,
    repo::{PinKind, PinMode},
};

pub type Block = libipld::Block<libipld::DefaultParams>;

use libipld::{Cid, Ipld, IpldCodec};

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
    core::{muxing::StreamMuxerBox, transport::Boxed},
    kad::{store::MemoryStoreConfig, KademliaConfig, Mode, Record},
    ping::Config as PingConfig,
    swarm::dial_opts::DialOpts,
    StreamProtocol,
};

pub(crate) static BITSWAP_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone)]
pub enum StoragePath {
    Disk(PathBuf),
    Memory,
    Custom {
        blockstore: Arc<dyn BlockStore>,
        datastore: Arc<dyn DataStore>,
        lock: Arc<dyn Lock>,
    },
}

impl PartialEq for StoragePath {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (StoragePath::Disk(left_path), StoragePath::Disk(right_path)) => {
                left_path.eq(right_path)
            }
            (StoragePath::Memory, StoragePath::Memory) => true,
            (StoragePath::Custom { .. }, StoragePath::Custom { .. }) => {
                //Do we really care if they equal?
                //TODO: Possibly implement PartialEq/Eq for the traits so we could make sure
                //      that they do or dont eq each other. For now this will always be true
                true
            }
            _ => false,
        }
    }
}

impl Eq for StoragePath {}

/// Ipfs node options used to configure the node to be created with [`UninitializedIpfs`].
// TODO: Refactor
#[derive(Clone)]
pub struct IpfsOptions {
    /// The path of the ipfs repo (blockstore and datastore).
    ///
    /// This is always required but can be any path with in-memory backends. The filesystem backend
    /// creates a directory structure alike but not compatible to other ipfs implementations.
    ///
    /// # Incompatiblity and interop warning
    ///
    /// It is **not** recommended to set this to IPFS_PATH without first at least backing up your
    /// existing repository.
    pub ipfs_path: StoragePath,

    /// Nodes used as bootstrap peers.
    pub bootstrap: Vec<Multiaddr>,

    /// Enables mdns for peer discovery and announcement when true.
    pub mdns: bool,

    /// Enables ipv6 for mdns
    pub mdns_ipv6: bool,

    /// Keep connection alive
    pub keep_alive: bool,

    /// Enables dcutr
    pub dcutr: bool,

    /// Enables relay client.
    pub relay: bool,

    /// Disables kademlia protocol
    pub disable_kad: bool,

    /// Disables bitswap protocol
    pub disable_bitswap: bool,

    /// Bitswap configuration
    pub bitswap_config: Option<BitswapConfig>,

    /// Enables relay server
    pub relay_server: bool,

    /// Relay server config
    pub relay_server_config: Option<RelayConfig>,

    /// Bound listening addresses; by default the node will not listen on any address.
    pub listening_addrs: Vec<Multiaddr>,

    /// Transport configuration
    pub transport_configuration: Option<crate::p2p::TransportConfig>,

    /// Swarm configuration
    pub swarm_configuration: Option<crate::p2p::SwarmConfig>,

    /// Identify configuration
    pub identify_configuration: Option<crate::p2p::IdentifyConfiguration>,

    /// Pubsub configuration
    pub pubsub_config: Option<crate::p2p::PubsubConfig>,

    /// Kad configuration
    pub kad_configuration: Option<Either<KadConfig, KademliaConfig>>,

    /// Kad Store Config
    /// Note: Only supports MemoryStoreConfig at this time
    pub kad_store_config: KadStoreConfig,

    /// Ping Configuration
    pub ping_configuration: Option<PingConfig>,

    /// Enables port mapping (aka UPnP)
    pub port_mapping: bool,

    /// Address book configuration
    pub addr_config: Option<AddressBookConfig>,

    pub keystore: Keystore,

    /// Repo Provider option
    pub provider: RepoProvider,
    /// The span for tracing purposes, `None` value is converted to `tracing::trace_span!("ipfs")`.
    ///
    /// All futures returned by `Ipfs`, background task actions and swarm actions are instrumented
    /// with this span or spans referring to this as their parent. Setting this other than `None`
    /// default is useful when running multiple nodes.
    pub span: Option<Span>,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub enum RepoProvider {
    /// Dont provide any blocks automatically
    #[default]
    None,

    /// Provide all blocks stored automatically
    All,

    /// Provide pinned blocks
    Pinned,

    /// Provide root blocks only
    Roots,
}

impl Default for IpfsOptions {
    fn default() -> Self {
        Self {
            ipfs_path: StoragePath::Memory,
            mdns: Default::default(),
            mdns_ipv6: Default::default(),
            dcutr: Default::default(),
            bootstrap: Default::default(),
            relay: Default::default(),
            disable_kad: Default::default(),
            disable_bitswap: Default::default(),
            bitswap_config: Default::default(),
            keep_alive: Default::default(),
            relay_server: Default::default(),
            relay_server_config: Default::default(),
            kad_configuration: Default::default(),
            kad_store_config: Default::default(),
            ping_configuration: Default::default(),
            identify_configuration: Default::default(),
            addr_config: Default::default(),
            provider: Default::default(),
            keystore: Keystore::in_memory(),
            listening_addrs: vec![],
            port_mapping: false,
            transport_configuration: None,
            pubsub_config: None,
            swarm_configuration: None,
            span: None,
        }
    }
}

impl fmt::Debug for IpfsOptions {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        // needed since libp2p::identity::Keypair does not have a Debug impl, and the IpfsOptions
        // is a struct with all public fields, don't enforce users to use this wrapper.
        fmt.debug_struct("IpfsOptions")
            .field("ipfs_path", &self.ipfs_path)
            .field("bootstrap", &self.bootstrap)
            .field("mdns", &self.mdns)
            .field("dcutr", &self.dcutr)
            .field("listening_addrs", &self.listening_addrs)
            .field("span", &self.span)
            .finish()
    }
}

impl IpfsOptions {
    /// Creates an in-memory store backed configuration useful for any testing purposes.
    ///
    /// Also used from examples.
    pub fn inmemory_with_generated_keys() -> Self {
        IpfsOptions {
            listening_addrs: vec!["/ip4/127.0.0.1/tcp/0".parse().unwrap()],
            ..Default::default()
        }
    }
}

/// The facade for the Ipfs node.
///
/// The facade has most of the functionality either directly as a method or the functionality can
/// be implemented using the provided methods. For more information, see examples or the HTTP
/// endpoint implementations in `ipfs-http`.
///
/// The facade is created through [`UninitializedIpfs`] which is configured with [`IpfsOptions`].
#[derive(Clone)]
#[allow(clippy::type_complexity)]
pub struct Ipfs {
    span: Span,
    repo: Repo,
    key: Keypair,
    keystore: Keystore,
    identify_conf: IdentifyConfiguration,
    to_task: Sender<IpfsEvent>,
    record_key_validator: HashMap<String, Arc<dyn Fn(&str) -> anyhow::Result<Key> + Sync + Send>>,
}

impl std::fmt::Debug for Ipfs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Ipfs").finish()
    }
}

type Channel<T> = OneshotSender<Result<T, Error>>;
type ReceiverChannel<T> = oneshot::Receiver<Result<T, Error>>;
/// Events used internally to communicate with the swarm, which is executed in the the background
/// task.
#[derive(Debug)]
#[allow(clippy::type_complexity)]
enum IpfsEvent {
    /// Connect
    Connect(DialOpts, OneshotSender<ReceiverChannel<()>>),
    /// Node supported protocol
    Protocol(OneshotSender<Vec<String>>),
    /// Addresses
    Addresses(Channel<Vec<(PeerId, Vec<Multiaddr>)>>),
    /// Local addresses
    Listeners(Channel<Either<Vec<Multiaddr>, BoxFuture<'static, Vec<Multiaddr>>>>),
    /// Local addresses
    ExternalAddresses(Channel<Either<Vec<Multiaddr>, BoxFuture<'static, Vec<Multiaddr>>>>),
    /// Connected peers
    Connected(Channel<Vec<PeerId>>),
    /// Is Connected
    IsConnected(PeerId, Channel<bool>),
    /// Disconnect
    Disconnect(
        PeerId,
        OneshotSender<(ReceiverChannel<()>, ReceiverChannel<()>)>,
    ),
    /// Ban Peer
    Ban(PeerId, Channel<()>),
    /// Unban peer
    Unban(PeerId, Channel<()>),
    PubsubSubscribe(String, OneshotSender<Option<SubscriptionStream>>),
    PubsubUnsubscribe(String, OneshotSender<Result<bool, Error>>),
    PubsubPublish(
        String,
        Vec<u8>,
        OneshotSender<Result<MessageId, PublishError>>,
    ),
    PubsubPeers(Option<String>, OneshotSender<Vec<PeerId>>),
    GetBitswapPeers(OneshotSender<BoxFuture<'static, Vec<PeerId>>>),
    WantList(Option<PeerId>, OneshotSender<BoxFuture<'static, Vec<Cid>>>),
    PubsubSubscribed(OneshotSender<Vec<String>>),
    AddListeningAddress(
        Multiaddr,
        OneshotSender<anyhow::Result<oneshot::Receiver<Either<Multiaddr, Result<(), io::Error>>>>>,
    ),
    RemoveListeningAddress(
        Multiaddr,
        OneshotSender<anyhow::Result<oneshot::Receiver<Either<Multiaddr, Result<(), io::Error>>>>>,
    ),
    Bootstrap(Channel<ReceiverChannel<KadResult>>),
    AddPeer(PeerId, Multiaddr, Channel<()>),
    RemovePeer(PeerId, Option<Multiaddr>, Channel<bool>),
    GetClosestPeers(PeerId, OneshotSender<ReceiverChannel<KadResult>>),
    FindPeerIdentity(
        PeerId,
        OneshotSender<ReceiverChannel<libp2p::identify::Info>>,
    ),
    FindPeer(
        PeerId,
        bool,
        OneshotSender<Either<Vec<Multiaddr>, ReceiverChannel<KadResult>>>,
    ),
    WhitelistPeer(PeerId, Channel<()>),
    RemoveWhitelistPeer(PeerId, Channel<()>),
    GetProviders(Cid, OneshotSender<Option<BoxStream<'static, PeerId>>>),
    Provide(Cid, Channel<ReceiverChannel<KadResult>>),
    DhtMode(DhtMode, Channel<()>),
    DhtGet(Key, OneshotSender<BoxStream<'static, Record>>),
    DhtPut(Key, Vec<u8>, Quorum, Channel<ReceiverChannel<KadResult>>),
    GetBootstrappers(OneshotSender<Vec<Multiaddr>>),
    AddBootstrapper(Multiaddr, Channel<Multiaddr>),
    RemoveBootstrapper(Multiaddr, Channel<Multiaddr>),
    ClearBootstrappers(OneshotSender<Vec<Multiaddr>>),
    DefaultBootstrap(Channel<Vec<Multiaddr>>),

    //event streams
    PubsubEventStream(OneshotSender<UnboundedReceiver<InnerPubsubEvent>>),

    Exit,
}

#[derive(Debug, Copy, Clone)]
pub enum DhtMode {
    Auto,
    Client,
    Server,
}

impl From<DhtMode> for Option<Mode> {
    fn from(mode: DhtMode) -> Self {
        match mode {
            DhtMode::Auto => None,
            DhtMode::Client => Some(Mode::Client),
            DhtMode::Server => Some(Mode::Server),
        }
    }
}

#[derive(Debug, Clone)]
pub enum PubsubEvent {
    /// Subscription event to a given topic
    Subscribe { peer_id: PeerId },

    /// Unsubscribing event to a given topic
    Unsubscribe { peer_id: PeerId },
}

#[derive(Debug, Clone)]
pub(crate) enum InnerPubsubEvent {
    /// Subscription event to a given topic
    Subscribe { topic: String, peer_id: PeerId },

    /// Unsubscribing event to a given topic
    Unsubscribe { topic: String, peer_id: PeerId },
}

impl From<InnerPubsubEvent> for PubsubEvent {
    fn from(event: InnerPubsubEvent) -> Self {
        match event {
            InnerPubsubEvent::Subscribe { peer_id, .. } => PubsubEvent::Subscribe { peer_id },
            InnerPubsubEvent::Unsubscribe { peer_id, .. } => PubsubEvent::Unsubscribe { peer_id },
        }
    }
}

type TSwarmEvent<C> = <TSwarm<C> as Stream>::Item;
type TSwarmEventFn<C> = Arc<dyn Fn(&mut TSwarm<C>, &TSwarmEvent<C>) + Sync + Send>;
type TTransportFn = Box<
    dyn Fn(
            &Keypair,
            Option<libp2p::relay::client::Transport>,
        ) -> std::io::Result<Boxed<(PeerId, StreamMuxerBox)>>
        + Sync
        + Send
        + 'static,
>;

#[derive(Debug, Copy, Clone)]
pub enum FDLimit {
    Max,
    Custom(u64),
}

/// Configured Ipfs which can only be started.
#[allow(clippy::type_complexity)]
pub struct UninitializedIpfs<C: NetworkBehaviour<ToSwarm = void::Void> + Send> {
    keys: Keypair,
    options: IpfsOptions,
    fdlimit: Option<FDLimit>,
    delay: bool,
    repo_handle: Option<Repo>,
    local_external_addr: bool,
    swarm_event: Option<TSwarmEventFn<C>>,
    // record_validators: HashMap<String, Arc<dyn Fn(&str, &Record) -> bool + Sync + Send>>,
    record_key_validator: HashMap<String, Arc<dyn Fn(&str) -> anyhow::Result<Key> + Sync + Send>>,
    custom_behaviour: Option<C>,
    custom_transport: Option<TTransportFn>,
}

pub type UninitializedIpfsNoop = UninitializedIpfs<libp2p::swarm::dummy::Behaviour>;

impl<C: NetworkBehaviour<ToSwarm = void::Void> + Send> Default for UninitializedIpfs<C> {
    fn default() -> Self {
        Self::new()
    }
}

impl<C: NetworkBehaviour<ToSwarm = void::Void> + Send> UninitializedIpfs<C> {
    /// New uninitualized instance
    pub fn new() -> Self {
        Self::with_opt(IpfsOptions {
            listening_addrs: vec![
                "/ip4/0.0.0.0/tcp/0".parse().unwrap(),
                "/ip4/0.0.0.0/udp/0/quic-v1".parse().unwrap(),
                "/ip6/::/tcp/0".parse().unwrap(),
                "/ip6/::/udp/0/quic-v1".parse().unwrap(),
            ],
            ..Default::default()
        })
    }

    /// New uninitualized instance without any listener addresses
    pub fn empty() -> Self {
        Self::with_opt(Default::default())
    }

    /// Configures a new UninitializedIpfs with from the given options and optionally a span.
    /// If the span is not given, it is defaulted to `tracing::trace_span!("ipfs")`.
    ///
    /// The span is attached to all operations called on the later created `Ipfs` along with all
    /// operations done in the background task as well as tasks spawned by the underlying
    /// `libp2p::Swarm`.
    pub fn with_opt(options: IpfsOptions) -> Self {
        let keys = Keypair::generate_ed25519();
        let fdlimit = None;
        let delay = true;
        UninitializedIpfs {
            keys,
            options,
            fdlimit,
            delay,
            repo_handle: None,
            // record_validators: Default::default(),
            record_key_validator: Default::default(),
            local_external_addr: false,
            swarm_event: None,
            custom_behaviour: None,
            custom_transport: None,
        }
    }

    /// Adds a listening address
    pub fn add_listening_addr(mut self, addr: Multiaddr) -> Self {
        if !self.options.listening_addrs.contains(&addr) {
            self.options.listening_addrs.push(addr)
        }
        self
    }

    /// Adds a listening addresses
    pub fn add_listening_addrs(mut self, addrs: Vec<Multiaddr>) -> Self {
        if !addrs.is_empty() {
            self.options.listening_addrs = addrs;
        }

        self
    }

    /// Set a list of listening addresses
    pub fn set_listening_addrs(mut self, addrs: Vec<Multiaddr>) -> Self {
        self.options.listening_addrs = addrs;
        self
    }

    /// Adds a bootstrap node
    pub fn add_bootstrap(mut self, addr: Multiaddr) -> Self {
        if !self.options.bootstrap.contains(&addr) {
            self.options.bootstrap.push(addr)
        }
        self
    }

    /// Sets a path
    pub fn set_path<P: AsRef<Path>>(mut self, path: P) -> Self {
        let path = path.as_ref().to_path_buf();
        self.options.ipfs_path = StoragePath::Disk(path);
        self
    }

    /// Set identify configuration
    pub fn set_identify_configuration(mut self, config: crate::p2p::IdentifyConfiguration) -> Self {
        self.options.identify_configuration = Some(config);
        self
    }

    /// Set transport configuration
    pub fn set_transport_configuration(mut self, config: crate::p2p::TransportConfig) -> Self {
        self.options.transport_configuration = Some(config);
        self
    }

    /// Set swarm configuration
    pub fn set_swarm_configuration(mut self, config: crate::p2p::SwarmConfig) -> Self {
        self.options.swarm_configuration = Some(config);
        self
    }

    /// Set kad configuration
    pub fn set_kad_configuration(mut self, config: KadConfig, store: KadStoreConfig) -> Self {
        self.options.kad_configuration = Some(Either::Left(config));
        self.options.kad_store_config = store;
        self
    }

    /// Set ping configuration
    pub fn set_ping_configuration(mut self, config: PingConfig) -> Self {
        self.options.ping_configuration = Some(config);
        self
    }

    /// Set default record validator for IPFS
    /// Note: This will override any keys set for `ipns` prefix
    pub fn default_record_key_validator(mut self) -> Self {
        self.record_key_validator.insert(
            "ipns".into(),
            Arc::new(|key| to_dht_key(("ipns", |key| ipns_to_dht_key(key)), key)),
        );
        self
    }

    #[allow(clippy::type_complexity)]
    pub fn set_record_prefix_validator(
        mut self,
        key: &str,
        callback: Arc<dyn Fn(&str) -> anyhow::Result<Key> + Sync + Send>,
    ) -> Self {
        self.record_key_validator.insert(key.to_string(), callback);
        self
    }

    /// Set address book configuration
    pub fn set_addrbook_configuration(mut self, config: AddressBookConfig) -> Self {
        self.options.addr_config = Some(config);
        self
    }

    /// Set pubsub configuration
    pub fn set_pubsub_configuration(mut self, config: PubsubConfig) -> Self {
        self.options.pubsub_config = Some(config);
        self
    }

    /// Set RepoProvider option to provide blocks automatically
    pub fn set_provider(mut self, opt: RepoProvider) -> Self {
        self.options.provider = opt;
        self
    }

    /// Set keypair
    pub fn set_keypair(mut self, keypair: Keypair) -> Self {
        self.keys = keypair;
        self
    }

    /// Set block and data repo
    pub fn set_repo(mut self, repo: Repo) -> Self {
        self.repo_handle = Some(repo);
        self
    }

    /// Enable keep alive
    pub fn enable_keepalive(mut self) -> Self {
        self.options.keep_alive = true;
        self
    }

    /// Disables kademlia
    pub fn disable_kad(mut self) -> Self {
        self.options.disable_kad = true;
        self
    }

    /// Disable bitswap
    pub fn disable_bitswap(mut self) -> Self {
        self.options.disable_bitswap = true;
        self
    }

    /// Set Bitswap configuration
    pub fn set_bitswap_configuration(mut self, config: BitswapConfig) -> Self {
        self.options.bitswap_config = Some(config);
        self
    }

    /// Set a keystore
    pub fn set_keystore(mut self, keystore: Keystore) -> Self {
        self.options.keystore = keystore;
        self
    }

    /// Enable mdns
    pub fn enable_mdns(mut self) -> Self {
        self.options.mdns = true;
        self
    }

    /// Enable relay client
    pub fn enable_relay(mut self, with_dcutr: bool) -> Self {
        self.options.relay = true;
        self.options.dcutr = with_dcutr;
        self
    }

    /// Enable relay server
    pub fn enable_relay_server(mut self, config: Option<RelayConfig>) -> Self {
        self.options.relay_server = true;
        self.options.relay_server_config = config;
        self
    }

    /// Enable port mapping (AKA UPnP)
    pub fn enable_upnp(mut self) -> Self {
        self.options.port_mapping = true;
        self
    }

    /// Automatically add any listened address as an external address
    pub fn listen_as_external_addr(mut self) -> Self {
        self.local_external_addr = true;
        self
    }

    /// Set a custom behaviour
    pub fn set_custom_behaviour(mut self, behaviour: C) -> Self {
        self.custom_behaviour = Some(behaviour);
        self
    }

    #[allow(clippy::type_complexity)]
    /// Set a transport
    pub fn set_custom_transport(mut self, transport: TTransportFn) -> Self {
        self.custom_transport = Some(transport);
        self
    }

    /// Set file desc limit
    pub fn fd_limit(mut self, limit: FDLimit) -> Self {
        self.fdlimit = Some(limit);
        self
    }

    /// Used to delay the loop
    /// Note: This may be removed in future
    pub fn disable_delay(mut self) -> Self {
        self.delay = false;
        self
    }

    /// Handle libp2p swarm events
    pub fn swarm_events<F>(mut self, func: F) -> Self
    where
        F: Fn(&mut TSwarm<C>, &TSwarmEvent<C>) + Sync + Send + 'static,
    {
        self.swarm_event = Some(Arc::new(func));
        self
    }

    /// Initialize the ipfs node. The returned `Ipfs` value is cloneable, send and sync.
    pub async fn start(self) -> Result<Ipfs, Error> {
        let UninitializedIpfs {
            keys,
            fdlimit,
            delay,
            mut options,
            swarm_event,
            custom_behaviour,
            custom_transport,
            record_key_validator,
            local_external_addr,
            repo_handle,
            ..
        } = self;

        let root_span = Option::take(&mut options.span)
            // not sure what would be the best practice with tracing and spans
            .unwrap_or_else(|| tracing::trace_span!(parent: &Span::current(), "ipfs"));

        // the "current" span which is not entered but the awaited futures are instrumented with it
        let init_span = tracing::trace_span!(parent: &root_span, "init");

        // stored in the Ipfs, instrumenting every method call
        let facade_span = tracing::trace_span!("facade");

        // stored in the executor given to libp2p, used to spawn at least the connections,
        // instrumenting each of those.
        let exec_span = tracing::trace_span!(parent: &root_span, "exec");

        // instruments the IpfsFuture, the background task.
        let swarm_span = tracing::trace_span!(parent: &root_span, "swarm");

        let repo = match repo_handle {
            Some(repo) => {
                if repo.is_online() {
                    anyhow::bail!("Repo is already initialized");
                }
                repo
            }
            None => {
                if let StoragePath::Disk(path) = &options.ipfs_path {
                    if !path.is_dir() {
                        tokio::fs::create_dir_all(path).await?;
                    }
                }
                Repo::new(options.ipfs_path.clone())
            }
        };

        repo.init().instrument(init_span.clone()).await?;

        let repo_events = repo.initialize_channel();

        if let Some(limit) = fdlimit {
            #[cfg(unix)]
            {
                let (_, hard) = rlimit::Resource::NOFILE.get()?;
                let limit = match limit {
                    FDLimit::Max => hard,
                    FDLimit::Custom(limit) => limit,
                };

                let target = std::cmp::min(hard, limit);
                rlimit::Resource::NOFILE.set(target, hard)?;
                let (soft, _) = rlimit::Resource::NOFILE.get()?;
                if soft < 2048 {
                    error!("Limit is too low: {soft}");
                }
            }
            #[cfg(not(unix))]
            {
                warn!("Cannot set {limit:?}. Can only set a fd limit on unix systems. Ignoring...")
            }
        }

        let (to_task, receiver) = channel::<IpfsEvent>(1);
        let id_conf = options.identify_configuration.clone().unwrap_or_default();

        let keystore = options.keystore.clone();

        let ipfs = Ipfs {
            span: facade_span,
            repo: repo.clone(),
            identify_conf: id_conf,
            key: keys.clone(),
            keystore,
            to_task,
            record_key_validator,
        };

        //Note: If `All` or `Pinned` are used, we would have to auto adjust the amount of
        //      provider records by adding the amount of blocks to the config.
        //TODO: Add persistent layer for kad store
        let blocks = match options.provider {
            RepoProvider::None => vec![],
            RepoProvider::All => repo.list_blocks().await.unwrap_or_default(),
            RepoProvider::Pinned => {
                repo.list_pins(None)
                    .await
                    .filter_map(|result| async move { result.map(|(cid, _)| cid).ok() })
                    .collect()
                    .await
            }
            RepoProvider::Roots => {
                //TODO: Scan blockstore for root unixfs blocks
                warn!("RepoProvider::Roots is not implemented... ignoring...");
                vec![]
            }
        };

        let count = blocks.len();

        let store_config = &mut options.kad_store_config;

        match store_config.memory.as_mut() {
            Some(memory_config) => {
                memory_config.max_provided_keys += count;
            }
            None => {
                store_config.memory = Some(MemoryStoreConfig {
                    //Provide a buffer to the max amount of provided keys
                    max_provided_keys: (50 * 1024) + count,
                    ..Default::default()
                })
            }
        }

        // FIXME: mutating options above is an unfortunate side-effect of this call, which could be
        // reordered for less error prone code.
        let swarm_options = SwarmOptions::from(&options);

        let swarm_config = options.swarm_configuration.unwrap_or_default();
        let transport_config = options.transport_configuration.unwrap_or_default();
        let swarm = create_swarm(
            &keys,
            swarm_options,
            swarm_config,
            transport_config,
            repo.clone(),
            exec_span,
            (custom_behaviour, custom_transport),
        )
        .instrument(tracing::trace_span!(parent: &init_span, "swarm"))
        .await?;

        let kad_subscriptions = Default::default();
        let listener_subscriptions = Default::default();
        let listeners = Default::default();
        let bootstraps = Default::default();

        let IpfsOptions {
            listening_addrs, ..
        } = options;

        let mut fut = task::IpfsTask {
            repo_events: repo_events.fuse(),
            from_facade: receiver.fuse(),
            swarm,
            listening_addresses: HashMap::with_capacity(listening_addrs.len()),
            listeners,
            provider_stream: HashMap::new(),
            bitswap_provider_stream: Default::default(),
            record_stream: HashMap::new(),
            dht_peer_lookup: Default::default(),
            bitswap_sessions: Default::default(),
            disconnect_confirmation: Default::default(),
            pubsub_event_stream: Default::default(),
            kad_subscriptions,
            listener_subscriptions,
            repo,
            bootstraps,
            swarm_event,
            external_listener: Default::default(),
            local_listener: Default::default(),
            timer: Default::default(),
            local_external_addr,
        };

        for addr in listening_addrs.into_iter() {
            match fut.swarm.listen_on(addr) {
                Ok(id) => fut.listeners.insert(id),
                _ => continue,
            };
        }

        for block in blocks {
            if let Some(kad) = fut.swarm.behaviour_mut().kademlia.as_mut() {
                let key = Key::from(block.hash().to_bytes());
                match kad.start_providing(key) {
                    Ok(id) => {
                        let (tx, _rx) = oneshot_channel();
                        fut.kad_subscriptions.insert(id, tx);
                    }
                    Err(e) => match e {
                        libp2p::kad::store::Error::MaxProvidedKeys => break,
                        _ => unreachable!(),
                    },
                };
            }
        }

        tokio::spawn({
            async move {
                //Note: For now this is not configurable as its meant for internal testing purposes but may change in the future
                let as_fut = false;

                if as_fut {
                    fut.instrument(swarm_span).await;
                } else {
                    fut.run(delay).instrument(swarm_span).await;
                }
            }
        });
        Ok(ipfs)
    }
}

impl Ipfs {
    /// Return an [`IpldDag`] for DAG operations
    pub fn dag(&self) -> IpldDag {
        IpldDag::new(self.clone())
    }

    /// Return an [`Repo`] to access the internal repo of the node
    pub fn repo(&self) -> &Repo {
        &self.repo
    }

    /// Returns an [`IpfsFiles`] for files operations
    pub fn unixfs(&self) -> IpfsUnixfs {
        IpfsUnixfs::new(self.clone())
    }

    pub fn ipns(&self) -> Ipns {
        Ipns::new(self.clone())
    }

    /// Puts a block into the ipfs repo.
    ///
    /// # Forget safety
    ///
    /// Forgetting the returned future will not result in memory unsafety, but it can
    /// deadlock other tasks.
    pub async fn put_block(&self, block: Block) -> Result<Cid, Error> {
        self.repo
            .put_block(block)
            .instrument(self.span.clone())
            .await
            .map(|(cid, _put_status)| cid)
    }

    /// Retrieves a block from the local blockstore, or starts fetching from the network or join an
    /// already started fetch.
    pub async fn get_block(&self, cid: &Cid) -> Result<Block, Error> {
        self.repo
            .get_block(cid, &[], false)
            .instrument(self.span.clone())
            .await
    }

    /// Remove block from the ipfs repo. A pinned block cannot be removed.
    pub async fn remove_block(&self, cid: Cid) -> Result<Cid, Error> {
        self.repo
            .remove_block(&cid)
            .instrument(self.span.clone())
            .await
    }

    /// Cleans up of all unpinned blocks
    /// Note: This is extremely basic and should not be relied on completely
    ///       until there is additional or extended implementation for a gc
    pub async fn gc(&self) -> Result<Vec<Cid>, Error> {
        self.repo.cleanup().instrument(self.span.clone()).await
    }

    /// Pins a given Cid recursively or directly (non-recursively).
    ///
    /// Pins on a block are additive in sense that a previously directly (non-recursively) pinned
    /// can be made recursive, but removing the recursive pin on the block removes also the direct
    /// pin as well.
    ///
    /// Pinning a Cid recursively (for supported dag-protobuf and dag-cbor) will walk its
    /// references and pin the references indirectly. When a Cid is pinned indirectly it will keep
    /// its previous direct or recursive pin and be indirect in addition.
    ///
    /// Recursively pinned Cids cannot be re-pinned non-recursively but non-recursively pinned Cids
    /// can be "upgraded to" being recursively pinned.
    ///
    /// # Crash unsafety
    ///
    /// If a recursive `insert_pin` operation is interrupted because of a crash or the crash
    /// prevents from synchronizing the data store to disk, this will leave the system in an inconsistent
    /// state. The remedy is to re-pin recursive pins.
    pub async fn insert_pin(&self, cid: &Cid, recursive: bool) -> Result<(), Error> {
        let span = debug_span!(parent: &self.span, "insert_pin", cid = %cid, recursive);

        self.repo()
            .insert_pin(cid, recursive, false)
            .instrument(span)
            .await
    }

    /// Unpins a given Cid recursively or only directly.
    ///
    /// Recursively unpinning a previously only directly pinned Cid will remove the direct pin.
    ///
    /// Unpinning an indirectly pinned Cid is not possible other than through its recursively
    /// pinned tree roots.
    pub async fn remove_pin(&self, cid: &Cid, recursive: bool) -> Result<(), Error> {
        use futures::stream::TryStreamExt;
        let span = debug_span!(parent: &self.span, "remove_pin", cid = %cid, recursive);
        async move {
            if !recursive {
                self.repo.remove_direct_pin(cid).await
            } else {
                // start walking refs of the root after loading it

                let block = match self.repo.get_block_now(cid).await? {
                    Some(b) => b,
                    None => {
                        return Err(anyhow::anyhow!("pinned root not found: {}", cid));
                    }
                };

                let ipld = block.decode::<IpldCodec, Ipld>()?;
                let st = crate::refs::IpldRefs::default()
                    .with_only_unique()
                    .with_existing_blocks()
                    .refs_of_resolved(self.repo(), vec![(*cid, ipld.clone())].into_iter())
                    .map_ok(|crate::refs::Edge { destination, .. }| destination)
                    .into_stream()
                    .boxed();

                self.repo.remove_recursive_pin(cid, st).await
            }
        }
        .instrument(span)
        .await
    }

    /// Checks whether a given block is pinned.
    ///
    /// Returns true if the block is pinned, false if not. See Crash unsafety notes for the false
    /// response.
    ///
    /// # Crash unsafety
    ///
    /// Cannot currently detect partially written recursive pins. Those can happen if
    /// `Ipfs::insert_pin(cid, true)` is interrupted by a crash for example.
    ///
    /// Works correctly only under no-crash situations. Workaround for hitting a crash is to re-pin
    /// any existing recursive pins.
    ///
    // TODO: This operation could be provided as a `Ipfs::fix_pins()`.
    pub async fn is_pinned(&self, cid: &Cid) -> Result<bool, Error> {
        let span = debug_span!(parent: &self.span, "is_pinned", cid = %cid);
        self.repo.is_pinned(cid).instrument(span).await
    }

    /// Lists all pins, or the specific kind thereof.
    ///
    /// # Crash unsafety
    ///
    /// Does not currently recover from partial recursive pin insertions.
    pub async fn list_pins(
        &self,
        filter: Option<PinMode>,
    ) -> futures::stream::BoxStream<'static, Result<(Cid, PinMode), Error>> {
        let span = debug_span!(parent: &self.span, "list_pins", ?filter);
        self.repo.list_pins(filter).instrument(span).await
    }

    /// Read specific pins. When `requirement` is `Some`, all pins are required to be of the given
    /// [`PinMode`].
    ///
    /// # Crash unsafety
    ///
    /// Does not currently recover from partial recursive pin insertions.
    pub async fn query_pins(
        &self,
        cids: Vec<Cid>,
        requirement: Option<PinMode>,
    ) -> Result<Vec<(Cid, PinKind<Cid>)>, Error> {
        let span = debug_span!(parent: &self.span, "query_pins", ids = cids.len(), ?requirement);
        self.repo
            .query_pins(cids, requirement)
            .instrument(span)
            .await
    }

    /// Puts an ipld node into the ipfs repo using `dag-cbor` codec and Sha2_256 hash.
    ///
    /// Returns Cid version 1 for the document
    pub async fn put_dag(&self, ipld: Ipld) -> Result<Cid, Error> {
        self.dag()
            .put(IpldCodec::DagCbor, ipld, None)
            .instrument(self.span.clone())
            .await
    }

    /// Gets an ipld node from the ipfs, fetching the block if necessary.
    ///
    /// See [`IpldDag::get`] for more information.
    pub async fn get_dag(&self, path: IpfsPath) -> Result<Ipld, Error> {
        self.dag()
            .get(path, &[], false)
            .instrument(self.span.clone())
            .await
            .map_err(Error::new)
    }

    /// Get an ipld path from the datastore.
    /// Note: This will be replaced in the future and shouldnt be depended on completely
    pub async fn get_ipns(&self, peer_id: &PeerId) -> Result<Option<IpfsPath>, Error> {
        self.repo
            .get_ipns(peer_id)
            .instrument(self.span.clone())
            .await
    }

    /// Put an ipld path into the datastore.
    /// Note: This will be replaced in the future and shouldnt be depended on completely
    pub async fn put_ipns(&self, peer_id: &PeerId, path: &IpfsPath) -> Result<(), Error> {
        self.repo
            .put_ipns(peer_id, path)
            .instrument(self.span.clone())
            .await
    }

    /// Remove an ipld path from the datastore.
    /// Note: This will be replaced in the future and shouldnt be depended on completely
    pub async fn remove_ipns(&self, peer_id: &PeerId) -> Result<(), Error> {
        self.repo
            .remove_ipns(peer_id)
            .instrument(self.span.clone())
            .await
    }

    /// Creates a stream which will yield the bytes of an UnixFS file from the root Cid, with the
    /// optional file byte range. If the range is specified and is outside of the file, the stream
    /// will end without producing any bytes.
    ///
    /// To create an owned version of the stream, please use `ipfs::unixfs::cat` directly.
    pub async fn cat_unixfs(
        &self,
        starting_point: impl Into<unixfs::StartingPoint>,
        range: Option<Range<u64>>,
    ) -> Result<
        impl Stream<Item = Result<Vec<u8>, unixfs::TraversalFailed>> + Send + '_,
        unixfs::TraversalFailed,
    > {
        self.unixfs()
            .cat(starting_point, range, &[], false)
            .instrument(self.span.clone())
            .await
    }

    /// Add a file from a path to the blockstore
    ///
    /// To create an owned version of the stream, please use `ipfs::unixfs::add_file` directly.
    pub async fn add_file_unixfs<P: AsRef<std::path::Path>>(
        &self,
        path: P,
    ) -> Result<BoxStream<'_, UnixfsStatus>, Error> {
        let path = path.as_ref();
        self.unixfs()
            .add(path, None)
            .instrument(self.span.clone())
            .await
    }

    /// Add a file through a stream of data to the blockstore
    ///
    /// To create an owned version of the stream, please use `ipfs::unixfs::add` directly.
    pub async fn add_unixfs<'a>(
        &self,
        stream: BoxStream<'a, std::io::Result<Vec<u8>>>,
    ) -> Result<BoxStream<'a, UnixfsStatus>, Error> {
        self.unixfs()
            .add(stream, None)
            .instrument(self.span.clone())
            .await
    }

    /// Retreive a file and saving it to a path.
    ///
    /// To create an owned version of the stream, please use `ipfs::unixfs::get` directly.
    pub async fn get_unixfs<P: AsRef<Path>>(
        &self,
        path: IpfsPath,
        dest: P,
    ) -> Result<BoxStream<'_, UnixfsStatus>, Error> {
        self.unixfs()
            .get(path, dest, &[], false)
            .instrument(self.span.clone())
            .await
    }

    /// List directory contents
    pub async fn ls_unixfs(&self, path: IpfsPath) -> Result<BoxStream<'_, NodeItem>, Error> {
        self.unixfs()
            .ls(path, &[], false)
            .instrument(self.span.clone())
            .await
    }

    /// Resolves a ipns path to an ipld path; currently only supports dht and dnslink resolution.
    pub async fn resolve_ipns(&self, path: &IpfsPath, recursive: bool) -> Result<IpfsPath, Error> {
        async move {
            let ipns = self.ipns();
            let mut resolved = ipns.resolve(p2p::DnsResolver::Cloudflare, path).await;

            if recursive {
                let mut seen = HashSet::with_capacity(1);
                while let Ok(ref res) = resolved {
                    if !seen.insert(res.clone()) {
                        break;
                    }
                    resolved = ipns.resolve(p2p::DnsResolver::Cloudflare, res).await;
                }
            }
            resolved
        }
        .instrument(self.span.clone())
        .await
    }

    /// Publish ipns record to DHT
    #[cfg(feature = "experimental")]
    pub async fn publish_ipns(&self, path: &IpfsPath) -> Result<IpfsPath, Error> {
        async move {
            let ipns = self.ipns();
            ipns.publish(None, path, None).await
        }
        .instrument(self.span.clone())
        .await
    }

    /// Connects to the peer
    pub async fn connect(&self, target: impl Into<DialOpts>) -> Result<(), Error> {
        async move {
            let target = target.into();
            let (tx, rx) = oneshot_channel();
            self.to_task
                .clone()
                .send(IpfsEvent::Connect(target, tx))
                .await?;

            let subscription = rx.await?;

            subscription.await?
        }
        .instrument(self.span.clone())
        .await
    }

    /// Whitelist a peer
    pub async fn whitelist(&self, peer_id: PeerId) -> Result<(), Error> {
        async move {
            let (tx, rx) = oneshot_channel();
            self.to_task
                .clone()
                .send(IpfsEvent::WhitelistPeer(peer_id, tx))
                .await?;

            rx.await?.map_err(anyhow::Error::from)
        }
        .instrument(self.span.clone())
        .await
    }

    /// Remove peer from whitelist
    pub async fn remove_whitelisted_peer(&self, peer_id: PeerId) -> Result<(), Error> {
        async move {
            let (tx, rx) = oneshot_channel();
            self.to_task
                .clone()
                .send(IpfsEvent::RemoveWhitelistPeer(peer_id, tx))
                .await?;

            rx.await?.map_err(anyhow::Error::from)
        }
        .instrument(self.span.clone())
        .await
    }

    /// Returns known peer addresses
    pub async fn addrs(&self) -> Result<Vec<(PeerId, Vec<Multiaddr>)>, Error> {
        async move {
            let (tx, rx) = oneshot_channel();
            self.to_task.clone().send(IpfsEvent::Addresses(tx)).await?;
            rx.await?
        }
        .instrument(self.span.clone())
        .await
    }

    /// Checks whether there is an established connection to a peer.
    pub async fn is_connected(&self, peer_id: PeerId) -> Result<bool, Error> {
        async move {
            let (tx, rx) = oneshot_channel();
            self.to_task
                .clone()
                .send(IpfsEvent::IsConnected(peer_id, tx))
                .await?;
            rx.await?
        }
        .instrument(self.span.clone())
        .await
    }

    /// Returns the connected peers
    pub async fn connected(&self) -> Result<Vec<PeerId>, Error> {
        async move {
            let (tx, rx) = oneshot_channel();
            self.to_task.clone().send(IpfsEvent::Connected(tx)).await?;
            rx.await?
        }
        .instrument(self.span.clone())
        .await
    }

    /// Disconnects a given peer.
    pub async fn disconnect(&self, target: PeerId) -> Result<(), Error> {
        async move {
            let (tx, rx) = oneshot_channel();
            self.to_task
                .clone()
                .send(IpfsEvent::Disconnect(target, tx))
                .await?;
            let (rx_nb, rx_swarm) = rx.await?;
            let (result_nb, result_swarm) = futures::join!(rx_nb, rx_swarm);

            result_nb??;
            result_swarm?
        }
        .instrument(self.span.clone())
        .await
    }

    /// Bans a peer.
    pub async fn ban_peer(&self, target: PeerId) -> Result<(), Error> {
        async move {
            let (tx, rx) = oneshot_channel();
            self.to_task
                .clone()
                .send(IpfsEvent::Ban(target, tx))
                .await?;
            rx.await?
        }
        .instrument(self.span.clone())
        .await
    }

    /// Unbans a peer.
    pub async fn unban_peer(&self, target: PeerId) -> Result<(), Error> {
        async move {
            let (tx, rx) = oneshot_channel();
            self.to_task
                .clone()
                .send(IpfsEvent::Unban(target, tx))
                .await?;
            rx.await?
        }
        .instrument(self.span.clone())
        .await
    }

    /// Returns the peer identity information. If no peer id is supplied the local node identity is used.
    pub async fn identity(&self, peer_id: Option<PeerId>) -> Result<PeerInfo, Error> {
        async move {
            match peer_id {
                Some(peer_id) => {
                    let (tx, rx) = oneshot_channel();

                    self.to_task
                        .clone()
                        .send(IpfsEvent::FindPeerIdentity(peer_id, tx))
                        .await?;

                    rx.await?.await?.map(PeerInfo::from)
                }
                None => {
                    let (local_result, external_result) =
                        futures::join!(self.listening_addresses(), self.external_addresses());

                    let external = external_result.unwrap_or_default();
                    let local = local_result.unwrap_or_default();

                    let mut addresses = local
                        .iter()
                        .chain(external.iter())
                        .cloned()
                        .collect::<Vec<_>>();

                    let (tx, rx) = oneshot_channel();
                    self.to_task.clone().send(IpfsEvent::Protocol(tx)).await?;

                    let protocols = rx
                        .await?
                        .iter()
                        .filter_map(|s| StreamProtocol::try_from_owned(s.clone()).ok())
                        .collect();

                    let public_key = self.key.public();
                    let peer_id = public_key.to_peer_id();

                    for addr in &mut addresses {
                        if !matches!(addr.iter().last(), Some(Protocol::P2p(_))) {
                            addr.push(Protocol::P2p(peer_id))
                        }
                    }

                    let info = PeerInfo {
                        peer_id,
                        public_key,
                        protocol_version: self.identify_conf.protocol_version.clone(),
                        agent_version: self.identify_conf.agent_version.clone(),
                        listen_addrs: addresses,
                        protocols,
                        observed_addr: None,
                    };

                    Ok(info)
                }
            }
        }
        .instrument(self.span.clone())
        .await
    }

    /// Subscribes to a given topic. Can be done at most once without unsubscribing in the between.
    /// The subscription can be unsubscribed by dropping the stream or calling
    /// [`Ipfs::pubsub_unsubscribe`].
    pub async fn pubsub_subscribe(&self, topic: String) -> Result<SubscriptionStream, Error> {
        async move {
            let (tx, rx) = oneshot_channel();

            self.to_task
                .clone()
                .send(IpfsEvent::PubsubSubscribe(topic.clone(), tx))
                .await?;

            rx.await?
                .ok_or_else(|| format_err!("already subscribed to {:?}", topic))
        }
        .instrument(self.span.clone())
        .await
    }

    /// Stream that returns [`PubsubEvent`] for a given topic
    pub async fn pubsub_events(
        &self,
        topic: &str,
    ) -> Result<BoxStream<'static, PubsubEvent>, Error> {
        async move {
            let (tx, rx) = oneshot_channel();

            self.to_task
                .clone()
                .send(IpfsEvent::PubsubEventStream(tx))
                .await?;

            let mut receiver = rx
                .await?;

            let defined_topic = topic.to_string();

            let peers = self.pubsub_peers(Some(defined_topic.clone())).await?;
            
            let stream = async_stream::stream! {
                for peer_id in peers {
                    yield PubsubEvent::Subscribe { peer_id };
                }
                while let Some(event) = receiver.next().await {
                    match &event {
                        InnerPubsubEvent::Subscribe { topic, .. } | InnerPubsubEvent::Unsubscribe { topic, .. } if topic.eq(&defined_topic) => yield event.into(),
                        _ => {}
                    }
                }
            };

            Ok(stream.boxed())
        }
        .instrument(self.span.clone())
        .await
    }

    /// Publishes to the topic which may have been subscribed to earlier
    pub async fn pubsub_publish(&self, topic: String, data: Vec<u8>) -> Result<MessageId, Error> {
        async move {
            let (tx, rx) = oneshot_channel();

            self.to_task
                .clone()
                .send(IpfsEvent::PubsubPublish(topic, data, tx))
                .await?;
            Ok(rx.await??)
        }
        .instrument(self.span.clone())
        .await
    }

    /// Forcibly unsubscribes a previously made [`SubscriptionStream`], which could also be
    /// unsubscribed by dropping the stream.
    ///
    /// Returns true if unsubscription was successful
    pub async fn pubsub_unsubscribe(&self, topic: &str) -> Result<bool, Error> {
        async move {
            let (tx, rx) = oneshot_channel();

            self.to_task
                .clone()
                .send(IpfsEvent::PubsubUnsubscribe(topic.into(), tx))
                .await?;

            rx.await?
        }
        .instrument(self.span.clone())
        .await
    }

    /// Returns all known pubsub peers with the optional topic filter
    pub async fn pubsub_peers(&self, topic: Option<String>) -> Result<Vec<PeerId>, Error> {
        async move {
            let (tx, rx) = oneshot_channel();

            self.to_task
                .clone()
                .send(IpfsEvent::PubsubPeers(topic, tx))
                .await?;

            Ok(rx.await?)
        }
        .instrument(self.span.clone())
        .await
    }

    /// Returns all currently subscribed topics
    pub async fn pubsub_subscribed(&self) -> Result<Vec<String>, Error> {
        async move {
            let (tx, rx) = oneshot_channel();

            self.to_task
                .clone()
                .send(IpfsEvent::PubsubSubscribed(tx))
                .await?;

            Ok(rx.await?)
        }
        .instrument(self.span.clone())
        .await
    }

    /// Returns the known wantlist for the local node when the `peer` is `None` or the wantlist of the given `peer`
    pub async fn bitswap_wantlist(&self, peer: Option<PeerId>) -> Result<Vec<Cid>, Error> {
        async move {
            let (tx, rx) = oneshot_channel();

            self.to_task
                .clone()
                .send(IpfsEvent::WantList(peer, tx))
                .await?;

            Ok(rx.await?.await)
        }
        .instrument(self.span.clone())
        .await
    }

    /// Returns a list of local blocks
    ///
    /// This implementation is subject to change into a stream, which might only include the pinned
    /// blocks.
    pub async fn refs_local(&self) -> Result<Vec<Cid>, Error> {
        self.repo.list_blocks().instrument(self.span.clone()).await
    }

    /// Returns local listening addresses
    pub async fn listening_addresses(&self) -> Result<Vec<Multiaddr>, Error> {
        async move {
            let (tx, rx) = oneshot_channel();

            self.to_task.clone().send(IpfsEvent::Listeners(tx)).await?;

            match rx.await?? {
                Either::Left(list) => Ok(list),
                Either::Right(fut) => {
                    let list = tokio::time::timeout(Duration::from_secs(5), fut).await?;
                    Ok(list)
                }
            }
        }
        .instrument(self.span.clone())
        .await
    }

    /// Returns external addresses
    pub async fn external_addresses(&self) -> Result<Vec<Multiaddr>, Error> {
        async move {
            let (tx, rx) = oneshot_channel();

            self.to_task
                .clone()
                .send(IpfsEvent::ExternalAddresses(tx))
                .await?;

            match rx.await?? {
                Either::Left(list) => Ok(list),
                Either::Right(fut) => {
                    let list = tokio::time::timeout(Duration::from_secs(5), fut).await?;
                    Ok(list)
                }
            }
        }
        .instrument(self.span.clone())
        .await
    }

    /// Add a given multiaddr as a listening address. Will fail if the address is unsupported, or
    /// if it is already being listened on. Currently will invoke `Swarm::listen_on` internally,
    /// keep the ListenerId for later `remove_listening_address` use in a HashMap.
    ///
    /// The returned future will resolve on the first bound listening address when this is called
    /// with `/ip4/0.0.0.0/...` or anything similar which will bound through multiple concrete
    /// listening addresses.
    ///
    /// Trying to add an unspecified listening address while any other listening address adding is
    /// in progress will result in error.
    ///
    /// Returns the bound multiaddress, which in the case of original containing an ephemeral port
    /// has now been changed.
    pub async fn add_listening_address(&self, addr: Multiaddr) -> Result<Multiaddr, Error> {
        async move {
            //Note: This is due to a possible race when doing an initial dial out to a relay
            //      Without this delay, the listener may close, resulting in an error here
            if addr.iter().any(|p| matches!(p, Protocol::P2pCircuit)) {
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            let (tx, rx) = oneshot_channel();

            self.to_task
                .clone()
                .send(IpfsEvent::AddListeningAddress(addr, tx))
                .await?;
            let rx = rx.await??;
            match rx.await? {
                Either::Left(addr) => Ok(addr),
                Either::Right(result) => {
                    result?;
                    Err(anyhow::anyhow!("No multiaddr provided"))
                }
            }
        }
        .instrument(self.span.clone())
        .await
    }

    /// Stop listening on a previously added listening address. Fails if the address is not being
    /// listened to.
    ///
    /// The removal of all listening addresses added through unspecified addresses is not supported.
    pub async fn remove_listening_address(&self, addr: Multiaddr) -> Result<(), Error> {
        async move {
            let (tx, rx) = oneshot_channel();

            self.to_task
                .clone()
                .send(IpfsEvent::RemoveListeningAddress(addr, tx))
                .await?;
            let rx = rx.await??;
            match rx.await? {
                Either::Left(addr) => Err(anyhow::anyhow!(
                    "Error: Address {addr} was returned while removing listener"
                )),
                Either::Right(result) => result.map_err(anyhow::Error::from),
            }
        }
        .instrument(self.span.clone())
        .await
    }

    /// Obtain the addresses associated with the given `PeerId`; they are first searched for locally
    /// and the DHT is used as a fallback: a `Kademlia::get_closest_peers(peer_id)` query is run and
    /// when it's finished, the newly added DHT records are checked for the existence of the desired
    /// `peer_id` and if it's there, the list of its known addresses is returned.
    pub async fn find_peer(&self, peer_id: PeerId) -> Result<Vec<Multiaddr>, Error> {
        async move {
            let (tx, rx) = oneshot_channel();

            self.to_task
                .clone()
                .send(IpfsEvent::FindPeer(peer_id, false, tx))
                .await?;

            match rx.await? {
                Either::Left(addrs) if !addrs.is_empty() => Ok(addrs),
                Either::Left(_) => unreachable!(),
                Either::Right(future) => {
                    future.await??;

                    let (tx, rx) = oneshot_channel();

                    self.to_task
                        .clone()
                        .send(IpfsEvent::FindPeer(peer_id, true, tx))
                        .await?;

                    match rx.await? {
                        Either::Left(addrs) if !addrs.is_empty() => Ok(addrs),
                        _ => Err(anyhow!("couldn't find peer {}", peer_id)),
                    }
                }
            }
        }
        .instrument(self.span.clone())
        .await
    }

    /// Performs a DHT lookup for providers of a value to the given key.
    ///
    /// Returns a list of peers found providing the Cid.
    pub async fn get_providers(&self, cid: Cid) -> Result<BoxStream<'static, PeerId>, Error> {
        async move {
            let (tx, rx) = oneshot_channel();

            self.to_task
                .clone()
                .send(IpfsEvent::GetProviders(cid, tx))
                .await?;

            rx.await?.ok_or_else(|| anyhow!("Provider already exist"))
        }
        .instrument(self.span.clone())
        .await
    }

    /// Establishes the node as a provider of a block with the given Cid: it publishes a provider
    /// record with the given key (Cid) and the node's PeerId to the peers closest to the key. The
    /// publication of provider records is periodically repeated as per the interval specified in
    /// `libp2p`'s  `KademliaConfig`.
    pub async fn provide(&self, cid: Cid) -> Result<(), Error> {
        // don't provide things we don't actually have
        if self.repo.get_block_now(&cid).await?.is_none() {
            return Err(anyhow!(
                "Error: block {} not found locally, cannot provide",
                cid
            ));
        }

        let kad_result = async move {
            let (tx, rx) = oneshot_channel();

            self.to_task
                .clone()
                .send(IpfsEvent::Provide(cid, tx))
                .await?;

            rx.await?
        }
        .instrument(self.span.clone())
        .await?
        .await;

        match kad_result? {
            Ok(KadResult::Complete) => Ok(()),
            Ok(_) => unreachable!(),
            Err(e) => Err(anyhow!(e)),
        }
    }

    /// Returns a list of peers closest to the given `PeerId`, as suggested by the DHT. The
    /// node must have at least one known peer in its routing table in order for the query
    /// to return any values.
    pub async fn get_closest_peers(&self, peer_id: PeerId) -> Result<Vec<PeerId>, Error> {
        let kad_result = async move {
            let (tx, rx) = oneshot_channel();

            self.to_task
                .clone()
                .send(IpfsEvent::GetClosestPeers(peer_id, tx))
                .await?;

            Ok(rx.await?).map_err(|e: String| anyhow!(e))
        }
        .instrument(self.span.clone())
        .await?
        .await;

        match kad_result? {
            Ok(KadResult::Peers(closest)) => Ok(closest),
            Ok(_) => unreachable!(),
            Err(e) => Err(anyhow!(e)),
        }
    }

    /// Change the DHT mode
    pub async fn dht_mode(&self, mode: DhtMode) -> Result<(), Error> {
        async move {
            let (tx, rx) = oneshot_channel();

            self.to_task
                .clone()
                .send(IpfsEvent::DhtMode(mode, tx))
                .await?;

            rx.await?
        }
        .instrument(self.span.clone())
        .await
    }

    /// Attempts to look a key up in the DHT and returns the values found in the records
    /// containing that key.
    pub async fn dht_get<T: AsRef<[u8]>>(
        &self,
        key: T,
    ) -> Result<BoxStream<'static, Record>, Error> {
        async move {
            let key = key.as_ref();

            let key_str = String::from_utf8_lossy(key);

            let key = if let Ok((prefix, _)) = split_dht_key(&key_str) {
                if let Some(key_fn) = self.record_key_validator.get(prefix) {
                    key_fn(&key_str)?
                } else {
                    Key::from(key.to_vec())
                }
            } else {
                Key::from(key.to_vec())
            };

            let (tx, rx) = oneshot_channel();

            self.to_task
                .clone()
                .send(IpfsEvent::DhtGet(key, tx))
                .await?;

            Ok(rx.await?).map_err(|e: String| anyhow!(e))
        }
        .instrument(self.span.clone())
        .await
    }

    /// Stores the given key + value record locally and replicates it in the DHT. It doesn't
    /// expire locally and is periodically replicated in the DHT, as per the `KademliaConfig`
    /// setup.
    pub async fn dht_put<T: AsRef<[u8]>>(
        &self,
        key: T,
        value: Vec<u8>,
        quorum: Quorum,
    ) -> Result<(), Error> {
        let kad_result = async move {
            let key = key.as_ref();

            let key_str = String::from_utf8_lossy(key);

            let key = if let Ok((prefix, _)) = split_dht_key(&key_str) {
                if let Some(key_fn) = self.record_key_validator.get(prefix) {
                    key_fn(&key_str)?
                } else {
                    Key::from(key.to_vec())
                }
            } else {
                Key::from(key.to_vec())
            };

            let (tx, rx) = oneshot_channel();

            self.to_task
                .clone()
                .send(IpfsEvent::DhtPut(key, value, quorum, tx))
                .await?;

            Ok(rx.await?).map_err(|e: String| anyhow!(e))
        }
        .instrument(self.span.clone())
        .await??
        .await;

        match kad_result? {
            Ok(KadResult::Complete) => Ok(()),
            Ok(_) => unreachable!(),
            Err(e) => Err(anyhow!(e)),
        }
    }

    // TBD
    pub async fn add_relay(&self, _: Multiaddr) -> Result<(), Error> {
        Err(anyhow::anyhow!("Unimplemented"))
    }

    // TBD
    pub async fn remove_relay(&self, _: Vec<Multiaddr>) -> Result<(), Error> {
        Err(anyhow::anyhow!("Unimplemented"))
    }

    // TBD
    pub async fn default_relay(&self) -> Result<(), Error> {
        Err(anyhow::anyhow!("Unimplemented"))
    }

    // TBD
    pub async fn relay_status(&self, _: Option<PeerId>) -> Result<(), Error> {
        Err(anyhow::anyhow!("Unimplemented"))
    }

    // TBD
    pub async fn set_relay(&self, _: Multiaddr) -> Result<(), Error> {
        Err(anyhow::anyhow!("Unimplemented"))
    }

    // TBD
    pub async fn auto_relay(&self) -> Result<(), Error> {
        Err(anyhow::anyhow!("Unimplemented"))
    }

    /// Walk the given Iplds' links up to `max_depth` (or indefinitely for `None`). Will return
    /// any duplicate trees unless `unique` is `true`.
    ///
    /// More information and a `'static` lifetime version available at [`refs::iplds_refs`].
    pub fn refs<'a, Iter>(
        &'a self,
        iplds: Iter,
        max_depth: Option<u64>,
        unique: bool,
    ) -> impl Stream<Item = Result<refs::Edge, libipld::error::Error>> + Send + 'a
    where
        Iter: IntoIterator<Item = (Cid, Ipld)> + Send + 'a,
    {
        refs::iplds_refs(self.repo(), iplds, max_depth, unique)
    }

    /// Obtain the list of addresses of bootstrapper nodes that are currently used.
    pub async fn get_bootstraps(&self) -> Result<Vec<Multiaddr>, Error> {
        async move {
            let (tx, rx) = oneshot_channel();

            self.to_task
                .clone()
                .send(IpfsEvent::GetBootstrappers(tx))
                .await?;

            Ok(rx.await?)
        }
        .instrument(self.span.clone())
        .await
    }

    /// Extend the list of used bootstrapper nodes with an additional address.
    /// Return value cannot be used to determine if the `addr` was a new bootstrapper, subject to
    /// change.
    pub async fn add_bootstrap(&self, addr: Multiaddr) -> Result<Multiaddr, Error> {
        async move {
            let (tx, rx) = oneshot_channel();

            self.to_task
                .clone()
                .send(IpfsEvent::AddBootstrapper(addr, tx))
                .await?;

            rx.await?
        }
        .instrument(self.span.clone())
        .await
    }

    /// Remove an address from the currently used list of bootstrapper nodes.
    /// Return value cannot be used to determine if the `addr` was an actual bootstrapper, subject to
    /// change.
    pub async fn remove_bootstrap(&self, addr: Multiaddr) -> Result<Multiaddr, Error> {
        async move {
            let (tx, rx) = oneshot_channel();

            self.to_task
                .clone()
                .send(IpfsEvent::RemoveBootstrapper(addr, tx))
                .await?;

            rx.await?
        }
        .instrument(self.span.clone())
        .await
    }

    /// Clear the currently used list of bootstrapper nodes, returning the removed addresses.
    pub async fn clear_bootstrap(&self) -> Result<Vec<Multiaddr>, Error> {
        async move {
            let (tx, rx) = oneshot_channel();

            self.to_task
                .clone()
                .send(IpfsEvent::ClearBootstrappers(tx))
                .await?;

            Ok(rx.await?)
        }
        .instrument(self.span.clone())
        .await
    }

    /// Restore the originally configured bootstrapper node list by adding them to the list of the
    /// currently used bootstrapper node address list; returns the restored addresses.
    pub async fn default_bootstrap(&self) -> Result<Vec<Multiaddr>, Error> {
        async move {
            let (tx, rx) = oneshot_channel();

            self.to_task
                .clone()
                .send(IpfsEvent::DefaultBootstrap(tx))
                .await?;

            rx.await?
        }
        .instrument(self.span.clone())
        .await
    }

    /// Bootstraps the local node to join the DHT: it looks up the node's own ID in the
    /// DHT and introduces it to the other nodes in it; at least one other node must be
    /// known in order for the process to succeed. Subsequently, additional queries are
    /// ran with random keys so that the buckets farther from the closest neighbor also
    /// get refreshed.
    pub async fn bootstrap(&self) -> Result<JoinHandle<Result<KadResult, Error>>, Error> {
        let (tx, rx) = oneshot_channel();

        self.to_task.clone().send(IpfsEvent::Bootstrap(tx)).await?;
        let fut = rx.await??;

        let bootstrap_task =
            tokio::spawn(async move { fut.await.map_err(|e| anyhow!(e)).and_then(|res| res) });

        Ok(bootstrap_task)
    }

    /// Add address of a peer to the address book
    pub async fn add_peer(&self, peer_id: PeerId, mut addr: Multiaddr) -> Result<(), Error> {
        if matches!(addr.iter().last(), Some(Protocol::P2p(_))) {
            addr.pop();
        }

        let (tx, rx) = oneshot::channel();

        self.to_task
            .clone()
            .send(IpfsEvent::AddPeer(peer_id, addr, tx))
            .await?;

        rx.await??;

        Ok(())
    }

    /// Remove peer from the address book
    pub async fn remove_peer(&self, peer_id: PeerId) -> Result<bool, Error> {
        let (tx, rx) = oneshot::channel();

        self.to_task
            .clone()
            .send(IpfsEvent::RemovePeer(peer_id, None, tx))
            .await?;

        rx.await.map_err(anyhow::Error::from)?
    }

    /// Remove peer address from the address book
    pub async fn remove_peer_address(
        &self,
        peer_id: PeerId,
        addr: Multiaddr,
    ) -> Result<bool, Error> {
        let (tx, rx) = oneshot::channel();

        self.to_task
            .clone()
            .send(IpfsEvent::RemovePeer(peer_id, Some(addr), tx))
            .await?;

        rx.await.map_err(anyhow::Error::from)?
    }

    /// Returns the Bitswap peers for the a `Node`.
    pub async fn get_bitswap_peers(&self) -> Result<Vec<PeerId>, Error> {
        let (tx, rx) = oneshot_channel();

        self.to_task
            .clone()
            .send(IpfsEvent::GetBitswapPeers(tx))
            .await?;

        Ok(rx.await.map_err(|e| anyhow!(e))?.await)
    }

    /// Returns the keypair to the node
    pub fn keypair(&self) -> Result<&Keypair, Error> {
        Ok(&self.key)
    }

    /// Returns the keystore
    pub fn keystore(&self) -> &Keystore {
        &self.keystore
    }

    /// Exit daemon.
    pub async fn exit_daemon(mut self) {
        // FIXME: this is a stopgap measure needed while repo is part of the struct Ipfs instead of
        // the background task or stream. After that this could be handled by dropping.
        self.repo.shutdown();

        // ignoring the error because it'd mean that the background task had already been dropped
        let _ = self.to_task.try_send(IpfsEvent::Exit);
    }
}

#[inline]
pub(crate) fn split_dht_key(key: &str) -> anyhow::Result<(&str, &str)> {
    anyhow::ensure!(!key.is_empty(), "Key cannot be empty");

    let (key, val) = {
        let data = key
            .split('/')
            .filter(|s| !s.trim().is_empty())
            .collect::<Vec<_>>();

        anyhow::ensure!(
            !data.is_empty() && data.len() == 2,
            "split dats cannot be empty"
        );

        (data[0], data[1])
    };

    Ok((key, val))
}

#[inline]
pub(crate) fn ipns_to_dht_key<B: AsRef<str>>(key: B) -> anyhow::Result<Key> {
    use libipld::multibase;

    let default_ipns_prefix = b"/ipns/";

    let mut key = key.as_ref().trim().to_string();

    anyhow::ensure!(!key.is_empty(), "Key cannot be empty");

    if key.starts_with('1') || key.starts_with('Q') {
        key.insert(0, 'z');
    }

    let mut data = multibase::decode(key).map(|(_, data)| data)?;

    if data[0] != 0x01 && data[1] != 0x72 {
        data = [vec![0x01, 0x72], data].concat();
    }

    data = [default_ipns_prefix.to_vec(), data[2..].to_vec()].concat();

    Ok(data.into())
}

#[inline]
pub(crate) fn to_dht_key<B: AsRef<str>, F: Fn(&str) -> anyhow::Result<Key>>(
    (prefix, func): (&str, F),
    key: B,
) -> anyhow::Result<Key> {
    let key = key.as_ref().trim();

    let (key, val) = split_dht_key(key)?;

    anyhow::ensure!(!key.is_empty(), "Key cannot be empty");
    anyhow::ensure!(!val.is_empty(), "Value cannot be empty");

    if key == prefix {
        return func(val);
    }

    anyhow::bail!("Invalid prefix")
}

#[allow(dead_code)]
pub(crate) fn peerid_from_multiaddr(addr: &Multiaddr) -> anyhow::Result<PeerId> {
    let mut addr = addr.clone();
    let peer_id = match addr.pop() {
        Some(Protocol::P2p(peer_id)) => peer_id,
        _ => anyhow::bail!("Invalid PeerId"),
    };
    Ok(peer_id)
}

use crate::p2p::AddressBookConfig;
#[doc(hidden)]
pub use node::Node;

/// Node module provides an easy to use interface used in `tests/`.
mod node {
    use futures::TryFutureExt;

    use super::*;

    /// Node encapsulates everything to setup a testing instance so that multi-node tests become
    /// easier.
    pub struct Node {
        /// The Ipfs facade.
        pub ipfs: Ipfs,
        /// The peer identifier on the network.
        pub id: PeerId,
        /// The listened to and externally visible addresses. The addresses are suffixed with the
        /// P2p protocol containing the node's PeerID.
        pub addrs: Vec<Multiaddr>,
    }

    impl Node {
        /// Initialises a new `Node` with an in-memory store backed configuration.
        ///
        /// This will use the testing defaults for the `IpfsOptions`. If `IpfsOptions` has been
        /// initialised manually, use `Node::with_options` instead.
        pub async fn new<T: AsRef<str>>(name: T) -> Self {
            let mut opts = IpfsOptions::inmemory_with_generated_keys();
            opts.span = Some(trace_span!("ipfs", node = name.as_ref()));
            Self::with_options(opts).await
        }

        /// Connects to a peer at the given address.
        pub async fn connect<D: Into<DialOpts>>(&self, opt: D) -> Result<(), Error> {
            let opts = opt.into();
            if let Some(peer_id) = opts.get_peer_id() {
                if self.ipfs.is_connected(peer_id).await? {
                    return Ok(());
                }
            }
            self.ipfs.connect(opts).await
        }

        /// Returns a new `Node` based on `IpfsOptions`.
        pub async fn with_options(opts: IpfsOptions) -> Self {
            // for future: assume UninitializedIpfs handles instrumenting any futures with the
            // given span
            let ipfs: Ipfs = UninitializedIpfsNoop::with_opt(opts)
                .disable_delay()
                .start()
                .await
                .unwrap();
            let id = ipfs.keypair().map(|kp| kp.public().to_peer_id()).unwrap();
            let mut addrs = ipfs.listening_addresses().await.unwrap();

            for addr in &mut addrs {
                if let Some(proto) = addr.iter().last() {
                    if !matches!(proto, Protocol::P2p(_)) {
                        addr.push(Protocol::P2p(id));
                    }
                }
            }

            Node { ipfs, id, addrs }
        }

        /// Returns the subscriptions for a `Node`.
        #[allow(clippy::type_complexity)]
        pub fn get_subscriptions(
            &self,
        ) -> &parking_lot::Mutex<HashMap<Cid, Vec<oneshot::Sender<Result<Block, String>>>>>
        {
            &self.ipfs.repo.subscriptions
        }

        /// Bootstraps the local node to join the DHT: it looks up the node's own ID in the
        /// DHT and introduces it to the other nodes in it; at least one other node must be
        /// known in order for the process to succeed. Subsequently, additional queries are
        /// ran with random keys so that the buckets farther from the closest neighbor also
        /// get refreshed.
        pub async fn bootstrap(&self) -> Result<KadResult, Error> {
            self.ipfs
                .bootstrap()
                .and_then(|fut| async { fut.await.map_err(anyhow::Error::from) })
                .await?
        }

        pub async fn add_node(&self, node: &Self) -> Result<(), Error> {
            for addr in &node.addrs {
                self.add_peer(node.id, addr.to_owned()).await?;
            }

            Ok(())
        }

        /// Shuts down the `Node`.
        pub async fn shutdown(self) {
            self.ipfs.exit_daemon().await;
        }
    }

    impl Deref for Node {
        type Target = Ipfs;

        fn deref(&self) -> &Self::Target {
            &self.ipfs
        }
    }

    impl DerefMut for Node {
        fn deref_mut(&mut self) -> &mut <Self as Deref>::Target {
            &mut self.ipfs
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use libipld::{
        ipld,
        multihash::{Code, MultihashDigest},
        IpldCodec,
    };

    #[tokio::test]
    async fn test_put_and_get_block() {
        let ipfs = Node::new("test_node").await;

        let data = b"hello block\n".to_vec();
        let cid = Cid::new_v1(IpldCodec::Raw.into(), Code::Sha2_256.digest(&data));
        let block = Block::new(cid, data).unwrap();

        let cid: Cid = ipfs.put_block(block.clone()).await.unwrap();
        let new_block = ipfs.get_block(&cid).await.unwrap();
        assert_eq!(block, new_block);
    }

    #[tokio::test]
    async fn test_put_and_get_dag() {
        let ipfs = Node::new("test_node").await;

        let data = ipld!([-1, -2, -3]);
        let cid = ipfs.put_dag(data.clone()).await.unwrap();
        let new_data = ipfs.get_dag(cid.into()).await.unwrap();
        assert_eq!(data, new_data);
    }

    #[tokio::test]
    async fn test_pin_and_unpin() {
        let ipfs = Node::new("test_node").await;

        let data = ipld!([-1, -2, -3]);
        let cid = ipfs.put_dag(data.clone()).await.unwrap();

        ipfs.insert_pin(&cid, false).await.unwrap();
        assert!(ipfs.is_pinned(&cid).await.unwrap());
        ipfs.remove_pin(&cid, false).await.unwrap();
        assert!(!ipfs.is_pinned(&cid).await.unwrap());
    }
}
