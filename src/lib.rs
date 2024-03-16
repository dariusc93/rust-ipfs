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
use bytes::Bytes;
use dag::{DagGet, DagPut};
use either::Either;
use futures::{
    channel::{
        mpsc::{channel, Sender, UnboundedReceiver},
        oneshot::{self, channel as oneshot_channel, Sender as OneshotSender},
    },
    future::BoxFuture,
    sink::SinkExt,
    stream::{BoxStream, Stream},
    FutureExt, StreamExt, TryStreamExt,
};

use keystore::Keystore;

#[cfg(feature = "beetle_bitswap")]
use p2p::BitswapConfig;

use p2p::{
    IdentifyConfiguration, KadConfig, KadStoreConfig, PeerInfo, PubsubConfig, RelayConfig,
    SwarmConfig, TransportConfig,
};
use repo::{BlockStore, DataStore, GCConfig, GCTrigger, Lock, RepoInsertPin, RepoRemovePin};
use tokio::task::JoinHandle;
use tokio_util::sync::{CancellationToken, DropGuard};
use tracing::Span;
use tracing_futures::Instrument;
use unixfs::{AddOpt, IpfsUnixfs, UnixfsAdd, UnixfsCat, UnixfsGet, UnixfsLs};

use std::{
    collections::{BTreeSet, HashMap, HashSet},
    fmt,
    ops::{Deref, DerefMut},
    path::{Path, PathBuf},
    sync::atomic::AtomicU64,
    sync::Arc,
    time::Duration,
};

use self::{
    dag::IpldDag,
    ipns::Ipns,
    p2p::{create_swarm, TSwarm},
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

use libipld::{Cid, Ipld};

pub use libp2p::{
    self,
    core::transport::ListenerId,
    gossipsub::{MessageId, PublishError},
    identity::Keypair,
    identity::PublicKey,
    kad::{Quorum, RecordKey as Key},
    multiaddr::multiaddr,
    multiaddr::Protocol,
    swarm::NetworkBehaviour,
    Multiaddr, PeerId,
};

use libp2p::{
    core::{muxing::StreamMuxerBox, transport::Boxed},
    kad::{store::MemoryStoreConfig, Mode, Record},
    ping::Config as PingConfig,
    rendezvous::Namespace,
    swarm::dial_opts::DialOpts,
    StreamProtocol,
};

pub(crate) static BITSWAP_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Default, Debug)]
pub enum StoragePath {
    Disk(PathBuf),
    #[default]
    Memory,
    Custom {
        blockstore: Option<Box<dyn BlockStore>>,
        datastore: Option<Box<dyn DataStore>>,
        lock: Option<Box<dyn Lock>>,
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

    #[cfg(feature = "beetle_bitswap")]
    /// Bitswap configuration
    pub bitswap_config: BitswapConfig,

    /// Relay server config
    pub relay_server_config: RelayConfig,

    /// Bound listening addresses; by default the node will not listen on any address.
    pub listening_addrs: Vec<Multiaddr>,

    /// Transport configuration
    pub transport_configuration: crate::p2p::TransportConfig,

    /// Swarm configuration
    pub swarm_configuration: crate::p2p::SwarmConfig,

    /// Identify configuration
    pub identify_configuration: crate::p2p::IdentifyConfiguration,

    /// Pubsub configuration
    pub pubsub_config: crate::p2p::PubsubConfig,

    /// Kad configuration
    pub kad_configuration: Either<KadConfig, libp2p::kad::Config>,

    /// Kad Store Config
    /// Note: Only supports MemoryStoreConfig at this time
    pub kad_store_config: KadStoreConfig,

    /// Ping Configuration
    pub ping_configuration: PingConfig,

    /// Address book configuration
    pub addr_config: AddressBookConfig,

    pub keystore: Keystore,

    /// Connection idle
    pub connection_idle: Duration,

    /// Repo Provider option
    pub provider: RepoProvider,

    /// The span for tracing purposes, `None` value is converted to `tracing::trace_span!("ipfs")`.
    ///
    /// All futures returned by `Ipfs`, background task actions and swarm actions are instrumented
    /// with this span or spans referring to this as their parent. Setting this other than `None`
    /// default is useful when running multiple nodes.
    pub span: Option<Span>,

    pub(crate) protocols: Libp2pProtocol,
}

#[derive(Default, Clone, Copy)]
pub(crate) struct Libp2pProtocol {
    pub(crate) pubsub: bool,
    pub(crate) kad: bool,
    pub(crate) bitswap: bool,
    pub(crate) relay_client: bool,
    pub(crate) relay_server: bool,
    pub(crate) dcutr: bool,
    pub(crate) mdns: bool,
    pub(crate) identify: bool,
    pub(crate) autonat: bool,
    pub(crate) rendezvous_client: bool,
    pub(crate) rendezvous_server: bool,
    pub(crate) upnp: bool,
    pub(crate) ping: bool,
    #[cfg(feature = "experimental_stream")]
    pub(crate) streams: bool,
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
            bootstrap: Default::default(),
            #[cfg(feature = "beetle_bitswap")]
            bitswap_config: Default::default(),
            relay_server_config: Default::default(),
            kad_configuration: Either::Left(Default::default()),
            kad_store_config: Default::default(),
            ping_configuration: Default::default(),
            identify_configuration: Default::default(),
            addr_config: Default::default(),
            provider: Default::default(),
            keystore: Keystore::in_memory(),
            connection_idle: Duration::from_secs(30),
            listening_addrs: vec![],
            transport_configuration: TransportConfig::default(),
            pubsub_config: PubsubConfig::default(),
            swarm_configuration: SwarmConfig::default(),
            span: None,
            protocols: Default::default(),
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
            .field("listening_addrs", &self.listening_addrs)
            .field("span", &self.span)
            .finish()
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
    _guard: Arc<DropGuard>,
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
    Connect(DialOpts, Channel<()>),
    /// Node supported protocol
    Protocol(OneshotSender<Vec<String>>),
    /// Addresses
    Addresses(Channel<Vec<(PeerId, Vec<Multiaddr>)>>),
    /// Local addresses
    Listeners(Channel<Vec<Multiaddr>>),
    /// Local addresses
    ExternalAddresses(Channel<Vec<Multiaddr>>),
    /// Connected peers
    Connected(Channel<Vec<PeerId>>),
    /// Is Connected
    IsConnected(PeerId, Channel<bool>),
    /// Disconnect
    Disconnect(PeerId, Channel<()>),
    /// Ban Peer
    Ban(PeerId, Channel<()>),
    /// Unban peer
    Unban(PeerId, Channel<()>),
    PubsubSubscribe(String, Channel<Option<SubscriptionStream>>),
    PubsubUnsubscribe(String, Channel<Result<bool, Error>>),
    PubsubPublish(String, Bytes, Channel<Result<MessageId, PublishError>>),
    PubsubPeers(Option<String>, Channel<Vec<PeerId>>),
    GetBitswapPeers(Channel<BoxFuture<'static, Vec<PeerId>>>),
    WantList(Option<PeerId>, Channel<BoxFuture<'static, Vec<Cid>>>),
    PubsubSubscribed(Channel<Vec<String>>),
    AddListeningAddress(Multiaddr, Channel<Multiaddr>),
    RemoveListeningAddress(Multiaddr, Channel<()>),
    Bootstrap(Channel<ReceiverChannel<KadResult>>),
    AddPeer(PeerId, Multiaddr, Channel<()>),
    RemovePeer(PeerId, Option<Multiaddr>, Channel<bool>),
    GetClosestPeers(PeerId, Channel<ReceiverChannel<KadResult>>),
    FindPeerIdentity(PeerId, Channel<ReceiverChannel<libp2p::identify::Info>>),
    FindPeer(
        PeerId,
        bool,
        Channel<Either<Vec<Multiaddr>, ReceiverChannel<KadResult>>>,
    ),
    GetProviders(Cid, Channel<Option<BoxStream<'static, PeerId>>>),
    Provide(Cid, Channel<ReceiverChannel<KadResult>>),
    DhtMode(DhtMode, Channel<()>),
    DhtGet(Key, Channel<BoxStream<'static, Record>>),
    DhtPut(Key, Vec<u8>, Quorum, Channel<ReceiverChannel<KadResult>>),
    GetBootstrappers(OneshotSender<Vec<Multiaddr>>),
    AddBootstrapper(Multiaddr, Channel<Multiaddr>),
    RemoveBootstrapper(Multiaddr, Channel<Multiaddr>),
    ClearBootstrappers(Channel<Vec<Multiaddr>>),
    DefaultBootstrap(Channel<Vec<Multiaddr>>),

    AddRelay(PeerId, Multiaddr, Channel<()>),
    RemoveRelay(PeerId, Multiaddr, Channel<()>),
    EnableRelay(Option<PeerId>, Channel<()>),
    DisableRelay(PeerId, Channel<()>),
    ListRelays(Channel<Vec<(PeerId, Vec<Multiaddr>)>>),
    ListActiveRelays(Channel<Vec<(PeerId, Vec<Multiaddr>)>>),
    //event streams
    PubsubEventStream(OneshotSender<UnboundedReceiver<InnerPubsubEvent>>),

    RegisterRendezvousNamespace(Namespace, PeerId, Option<u64>, Channel<()>),
    UnregisterRendezvousNamespace(Namespace, PeerId, Channel<()>),
    RendezvousNamespaceDiscovery(
        Option<Namespace>,
        bool,
        Option<u64>,
        PeerId,
        Channel<HashMap<PeerId, Vec<Multiaddr>>>,
    ),
    #[cfg(feature = "experimental_stream")]
    StreamControlHandle(Channel<libp2p_stream::Control>),
    #[cfg(feature = "experimental_stream")]
    NewStream(StreamProtocol, Channel<libp2p_stream::IncomingStreams>),
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
    keys: Option<Keypair>,
    options: IpfsOptions,
    fdlimit: Option<FDLimit>,
    repo_handle: Option<Repo>,
    local_external_addr: bool,
    swarm_event: Option<TSwarmEventFn<C>>,
    // record_validators: HashMap<String, Arc<dyn Fn(&str, &Record) -> bool + Sync + Send>>,
    record_key_validator: HashMap<String, Arc<dyn Fn(&str) -> anyhow::Result<Key> + Sync + Send>>,
    custom_behaviour: Option<C>,
    custom_transport: Option<TTransportFn>,
    gc_config: Option<GCConfig>,
    gc_repo_duration: Option<Duration>,
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
        UninitializedIpfs {
            keys: None,
            options: Default::default(),
            fdlimit: None,
            repo_handle: None,
            // record_validators: Default::default(),
            record_key_validator: Default::default(),
            local_external_addr: false,
            swarm_event: None,
            custom_behaviour: None,
            custom_transport: None,
            gc_config: None,
            gc_repo_duration: None,
        }
    }

    /// New uninitualized instance without any listener addresses
    #[deprecated(
        note = "UninitializedIpfs::empty will be removed in the future. Use UninitializedIpfs::new()"
    )]
    pub fn empty() -> Self {
        Self::new()
    }

    /// Configures a new UninitializedIpfs with from the given options and optionally a span.
    /// If the span is not given, it is defaulted to `tracing::trace_span!("ipfs")`.
    ///
    /// The span is attached to all operations called on the later created `Ipfs` along with all
    /// operations done in the background task as well as tasks spawned by the underlying
    /// `libp2p::Swarm`.
    #[deprecated(
        note = "UninitializedIpfs::with_opt will be removed in the future. Use UninitializedIpfs::new()"
    )]
    pub fn with_opt(options: IpfsOptions) -> Self {
        let mut opt = Self::new();
        opt.options = options;
        opt
    }

    /// Set default listening unspecified ipv4 and ipv6 addresseses for tcp and udp/quic
    pub fn set_default_listener(self) -> Self {
        self.add_listening_addrs(vec![
            "/ip4/0.0.0.0/tcp/0".parse().unwrap(),
            "/ip4/0.0.0.0/udp/0/quic-v1".parse().unwrap(),
        ])
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
        self.options.listening_addrs.extend(addrs);
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

    #[cfg(feature = "beetle_bitswap")]
    /// Load default behaviour for basic functionality
    pub fn with_default(self) -> Self {
        self.with_identify(Default::default())
            .with_autonat()
            .with_bitswap(Default::default())
            .with_kademlia(Either::Left(Default::default()), Default::default())
            .with_ping(Default::default())
            .with_pubsub(Default::default())
    }

    #[cfg(feature = "libp2p_bitswap")]
    /// Load default behaviour for basic functionality
    pub fn with_default(self) -> Self {
        self.with_identify(Default::default())
            .with_autonat()
            .with_bitswap()
            .with_kademlia(Either::Left(Default::default()), Default::default())
            .with_ping(Default::default())
            .with_pubsub(Default::default())
    }

    #[cfg(not(any(feature = "libp2p_bitswap", feature = "beetle_bitswap")))]
    /// Load default behaviour for basic functionality
    pub fn with_default(self) -> Self {
        self.with_identify(Default::default())
            .with_autonat()
            .with_bitswap()
            .with_kademlia(Either::Left(Default::default()), Default::default())
            .with_ping(Default::default())
            .with_pubsub(Default::default())
    }

    /// Enables kademlia
    pub fn with_kademlia(
        mut self,
        config: impl Into<Either<KadConfig, libp2p::kad::Config>>,
        store: KadStoreConfig,
    ) -> Self {
        let config = config.into();
        self.options.protocols.kad = true;
        self.options.kad_configuration = config;
        self.options.kad_store_config = store;
        self
    }

    #[cfg(feature = "beetle_bitswap")]
    /// Enables bitswap
    pub fn with_bitswap(mut self, config: BitswapConfig) -> Self {
        self.options.protocols.bitswap = true;
        self.options.bitswap_config = config;
        self
    }

    #[cfg(feature = "libp2p_bitswap")]
    /// Enables bitswap
    pub fn with_bitswap(mut self) -> Self {
        self.options.protocols.bitswap = true;
        self
    }

    #[cfg(not(any(feature = "libp2p_bitswap", feature = "beetle_bitswap")))]
    /// Enables bitswap
    pub fn with_bitswap(mut self) -> Self {
        self.options.protocols.bitswap = true;
        self
    }

    /// Enable mdns
    pub fn with_mdns(mut self) -> Self {
        self.options.protocols.mdns = true;
        self
    }

    /// Enable relay client
    pub fn with_relay(mut self, with_dcutr: bool) -> Self {
        self.options.protocols.relay_client = true;
        self.options.protocols.dcutr = with_dcutr;
        self
    }

    /// Enable relay server
    pub fn with_relay_server(mut self, config: RelayConfig) -> Self {
        self.options.protocols.relay_server = true;
        self.options.relay_server_config = config;
        self
    }

    /// Enable port mapping (AKA UPnP)
    pub fn with_upnp(mut self) -> Self {
        self.options.protocols.upnp = true;
        self
    }

    /// Enables rendezvous server
    pub fn with_rendezvous_server(mut self) -> Self {
        self.options.protocols.rendezvous_server = true;
        self
    }

    /// Enables rendezvous client
    pub fn with_rendezvous_client(mut self) -> Self {
        self.options.protocols.rendezvous_client = true;
        self
    }

    /// Enables identify
    pub fn with_identify(mut self, config: crate::p2p::IdentifyConfiguration) -> Self {
        self.options.protocols.identify = true;
        self.options.identify_configuration = config;
        self
    }

    #[cfg(feature = "experimental_stream")]
    pub fn with_streams(mut self) -> Self {
        self.options.protocols.streams = true;
        self
    }

    /// Enables pubsub
    pub fn with_pubsub(mut self, config: PubsubConfig) -> Self {
        self.options.protocols.pubsub = true;
        self.options.pubsub_config = config;
        self
    }

    /// Enables autonat
    pub fn with_autonat(mut self) -> Self {
        self.options.protocols.autonat = true;
        self
    }

    /// Enables ping
    pub fn with_ping(mut self, config: PingConfig) -> Self {
        self.options.protocols.ping = true;
        self.options.ping_configuration = config;
        self
    }

    /// Set a custom behaviour
    pub fn with_custom_behaviour(mut self, behaviour: C) -> Self {
        self.custom_behaviour = Some(behaviour);
        self
    }

    /// Enables automatic garbage collection
    pub fn with_gc(mut self, config: GCConfig) -> Self {
        self.gc_config = Some(config);
        self
    }

    /// Set a duration for which blocks are not removed due to the garbage collector
    /// Defaults: 2 mins
    pub fn set_temp_pin_duration(mut self, duration: Duration) -> Self {
        self.gc_repo_duration = Some(duration);
        self
    }

    /// Sets a path
    pub fn set_path<P: AsRef<Path>>(mut self, path: P) -> Self {
        let path = path.as_ref().to_path_buf();
        self.options.ipfs_path = StoragePath::Disk(path);
        self
    }

    /// Set transport configuration
    pub fn set_transport_configuration(mut self, config: crate::p2p::TransportConfig) -> Self {
        self.options.transport_configuration = config;
        self
    }

    /// Set timeout for idle connections
    pub fn set_idle_connection_timeout(mut self, duration: u64) -> Self {
        self.options.connection_idle = Duration::from_secs(duration);
        self
    }

    /// Set swarm configuration
    pub fn set_swarm_configuration(mut self, config: crate::p2p::SwarmConfig) -> Self {
        self.options.swarm_configuration = config;
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
        self.options.addr_config = config;
        self
    }

    /// Set RepoProvider option to provide blocks automatically
    pub fn set_provider(mut self, opt: RepoProvider) -> Self {
        self.options.provider = opt;
        self
    }

    /// Set keypair
    pub fn set_keypair(mut self, keypair: &Keypair) -> Self {
        self.keys = Some(keypair.clone());
        self
    }

    /// Set block and data repo
    pub fn set_repo(mut self, repo: &Repo) -> Self {
        self.repo_handle = Some(repo.clone());
        self
    }

    /// Set a keystore
    pub fn set_keystore(mut self, keystore: &Keystore) -> Self {
        self.options.keystore = keystore.clone();
        self
    }

    /// Automatically add any listened address as an external address
    pub fn listen_as_external_addr(mut self) -> Self {
        self.local_external_addr = true;
        self
    }

    /// Set a transport
    pub fn with_custom_transport(mut self, transport: TTransportFn) -> Self {
        self.custom_transport = Some(transport);
        self
    }

    /// Set file desc limit
    pub fn fd_limit(mut self, limit: FDLimit) -> Self {
        self.fdlimit = Some(limit);
        self
    }

    /// Set tracing span
    pub fn set_span(mut self, span: Span) -> Self {
        self.options.span = Some(span);
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
            mut options,
            swarm_event,
            custom_behaviour,
            custom_transport,
            record_key_validator,
            local_external_addr,
            repo_handle,
            gc_config,
            gc_repo_duration,
            ..
        } = self;

        let keys = keys.unwrap_or(Keypair::generate_ed25519());

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
                Repo::new(&mut options.ipfs_path, gc_repo_duration)
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

        let token = CancellationToken::new();
        let _guard = Arc::new(token.clone().drop_guard());

        let (to_task, receiver) = channel::<IpfsEvent>(1);
        let id_conf = options.identify_configuration.clone();

        let keystore = options.keystore.clone();

        let ipfs = Ipfs {
            span: facade_span,
            repo,
            identify_conf: id_conf,
            key: keys.clone(),
            keystore,
            to_task,
            record_key_validator,
            _guard,
        };

        //Note: If `All` or `Pinned` are used, we would have to auto adjust the amount of
        //      provider records by adding the amount of blocks to the config.
        //TODO: Add persistent layer for kad store
        let blocks = match options.provider {
            RepoProvider::None => vec![],
            RepoProvider::All => ipfs.repo.list_blocks().await.unwrap_or_default(),
            RepoProvider::Pinned => {
                ipfs.repo
                    .list_pins(None)
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

        let swarm = create_swarm(
            &keys,
            &options,
            &ipfs.repo,
            exec_span,
            (custom_behaviour, custom_transport),
        )
        .instrument(tracing::trace_span!(parent: &init_span, "swarm"))
        .await?;

        let IpfsOptions {
            listening_addrs, ..
        } = options;

        if let Some(config) = gc_config {
            tokio::spawn({
                let repo = ipfs.repo.clone();
                let token = token.clone();
                async move {
                    let GCConfig { duration, trigger } = config;
                    let use_config_timer = duration != Duration::ZERO;
                    if trigger == GCTrigger::None && !use_config_timer {
                        tracing::warn!("GC does not have a set timer or a trigger. Disabling GC");
                        return;
                    }

                    let time = match use_config_timer {
                        true => duration,
                        false => Duration::from_secs(60 * 60),
                    };

                    let mut interval =
                        tokio::time::interval_at(tokio::time::Instant::now() + time, time);

                    loop {
                        tokio::select! {
                            _ = token.cancelled() => {
                                tracing::debug!("gc task cancelled");
                                break
                            },
                            _ = interval.tick() => {
                                let _g = repo.inner.gclock.write().await;
                                tracing::debug!("preparing gc operation");
                                let pinned = repo
                                    .list_pins(None)
                                    .await
                                    .try_filter_map(|(cid, _)| futures::future::ready(Ok(Some(cid))))
                                    .try_collect::<BTreeSet<_>>()
                                    .await
                                    .unwrap_or_default();
                                let pinned = Vec::from_iter(pinned);
                                let total_size = repo.get_total_size().await.unwrap_or_default();
                                let pinned_size = repo
                                    .get_blocks_size(&pinned)
                                    .await
                                    .ok()
                                    .flatten()
                                    .unwrap_or_default();

                                let unpinned_blocks = total_size - pinned_size;

                                tracing::debug!(total_size = %total_size, ?trigger, unpinned_blocks);

                                let cleanup = match trigger {
                                    GCTrigger::At { size } => {
                                        total_size > 0 && unpinned_blocks >= size
                                    }
                                    GCTrigger::AtStorage => {
                                        unpinned_blocks > 0
                                            && unpinned_blocks >= repo.max_storage_size()
                                    }
                                    GCTrigger::None => unpinned_blocks > 0,
                                };

                                tracing::debug!(will_run = %cleanup);

                                if cleanup {
                                    tracing::debug!("running cleanup of unpinned blocks");
                                    let blocks = repo.cleanup().await.unwrap();
                                    tracing::debug!(removed_blocks = blocks.len(), "blocks removed");
                                    tracing::debug!("cleanup finished");
                                }
                            }
                        }
                    }
                }
            });
        }

        let mut fut = task::IpfsTask::new(swarm, repo_events.fuse(), receiver.fuse(), &ipfs.repo);
        fut.swarm_event = swarm_event;
        fut.local_external_addr = local_external_addr;

        for addr in listening_addrs.into_iter() {
            match fut.swarm.listen_on(addr) {
                Ok(id) => {
                    let (tx, _rx) = oneshot_channel();
                    fut.pending_add_listener.insert(id, tx);
                }
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

                let fut = if as_fut {
                    fut.boxed()
                } else {
                    fut.run().boxed()
                };

                tokio::select! {
                    _ = fut => {}
                    _ = token.cancelled() => {},
                };
            }
            .instrument(swarm_span)
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
    pub async fn remove_block(&self, cid: Cid, recursive: bool) -> Result<Vec<Cid>, Error> {
        self.repo
            .remove_block(&cid, recursive)
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
    pub fn insert_pin(&self, cid: &Cid) -> RepoInsertPin {
        self.repo().pin(cid).span(self.span.clone())
    }

    /// Unpins a given Cid recursively or only directly.
    ///
    /// Recursively unpinning a previously only directly pinned Cid will remove the direct pin.
    ///
    /// Unpinning an indirectly pinned Cid is not possible other than through its recursively
    /// pinned tree roots.
    pub fn remove_pin(&self, cid: &Cid) -> RepoRemovePin {
        self.repo().remove_pin(cid).span(self.span.clone())
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
    pub fn put_dag(&self, ipld: Ipld) -> DagPut {
        self.dag().put_dag(ipld).span(self.span.clone())
    }

    /// Gets an ipld node from the ipfs, fetching the block if necessary.
    ///
    /// See [`IpldDag::get`] for more information.
    pub fn get_dag<I: Into<IpfsPath>>(&self, path: I) -> DagGet {
        self.dag().get_dag(path).span(self.span.clone())
    }

    /// Creates a stream which will yield the bytes of an UnixFS file from the root Cid, with the
    /// optional file byte range. If the range is specified and is outside of the file, the stream
    /// will end without producing any bytes.
    pub fn cat_unixfs(&self, starting_point: impl Into<unixfs::StartingPoint>) -> UnixfsCat {
        self.unixfs().cat(starting_point).span(self.span.clone())
    }

    /// Add a file from a path to the blockstore
    #[deprecated(note = "Use `Ipfs::add_unixfs` instead")]
    pub fn add_file_unixfs<P: AsRef<std::path::Path>>(&self, path: P) -> UnixfsAdd {
        let path = path.as_ref().to_path_buf();
        self.add_unixfs(path)
    }

    /// Add a file through a stream of data to the blockstore
    pub fn add_unixfs(&self, opt: impl Into<AddOpt>) -> UnixfsAdd {
        self.unixfs().add(opt).span(self.span.clone())
    }

    /// Retreive a file and saving it to a path.
    pub fn get_unixfs<P: AsRef<Path>>(&self, path: IpfsPath, dest: P) -> UnixfsGet {
        self.unixfs().get(path, dest).span(self.span.clone())
    }

    /// List directory contents
    pub fn ls_unixfs(&self, path: IpfsPath) -> UnixfsLs {
        self.unixfs().ls(path).span(self.span.clone())
    }

    /// Resolves a ipns path to an ipld path; currently only supports dht and dnslink resolution.
    pub async fn resolve_ipns(&self, path: &IpfsPath, recursive: bool) -> Result<IpfsPath, Error> {
        async move {
            let ipns = self.ipns();
            let mut resolved = ipns.resolve(path).await;

            if recursive {
                let mut seen = HashSet::with_capacity(1);
                while let Ok(ref res) = resolved {
                    if !seen.insert(res.clone()) {
                        break;
                    }
                    resolved = ipns.resolve(res).await;
                }
            }
            resolved
        }
        .instrument(self.span.clone())
        .await
    }

    /// Publish ipns record to DHT
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

            rx.await?
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

            rx.await?
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

                    rx.await??.await?.map(PeerInfo::from)
                }
                None => {
                    let mut addresses = HashSet::new();

                    let (local_result, external_result) =
                        futures::join!(self.listening_addresses(), self.external_addresses());

                    let external: HashSet<Multiaddr> =
                        HashSet::from_iter(external_result.unwrap_or_default());
                    let local: HashSet<Multiaddr> =
                        HashSet::from_iter(local_result.unwrap_or_default());

                    addresses.extend(external.iter().cloned());
                    addresses.extend(local.iter().cloned());

                    let mut addresses = Vec::from_iter(addresses);

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
    pub async fn pubsub_subscribe(
        &self,
        topic: impl Into<String>,
    ) -> Result<SubscriptionStream, Error> {
        async move {
            let topic = topic.into();
            let (tx, rx) = oneshot_channel();

            self.to_task
                .clone()
                .send(IpfsEvent::PubsubSubscribe(topic.clone(), tx))
                .await?;

            rx.await??
                .ok_or_else(|| format_err!("already subscribed to {:?}", topic))
        }
        .instrument(self.span.clone())
        .await
    }

    /// Stream that returns [`PubsubEvent`] for a given topic
    pub async fn pubsub_events(
        &self,
        topic: impl Into<String>,
    ) -> Result<BoxStream<'static, PubsubEvent>, Error> {
        async move {
            let topic = topic.into();
            let (tx, rx) = oneshot_channel();

            self.to_task
                .clone()
                .send(IpfsEvent::PubsubEventStream(tx))
                .await?;

            let mut receiver = rx
                .await?;

            let defined_topic = topic.to_string();

            let stream = async_stream::stream! {
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
    pub async fn pubsub_publish(
        &self,
        topic: impl Into<String>,
        data: impl Into<Bytes>,
    ) -> Result<MessageId, Error> {
        async move {
            let topic = topic.into();
            let data = data.into();
            let (tx, rx) = oneshot_channel();

            self.to_task
                .clone()
                .send(IpfsEvent::PubsubPublish(topic, data, tx))
                .await?;
            rx.await??.map_err(anyhow::Error::from)
        }
        .instrument(self.span.clone())
        .await
    }

    /// Forcibly unsubscribes a previously made [`SubscriptionStream`], which could also be
    /// unsubscribed by dropping the stream.
    ///
    /// Returns true if unsubscription was successful
    pub async fn pubsub_unsubscribe(&self, topic: impl Into<String>) -> Result<bool, Error> {
        async move {
            let (tx, rx) = oneshot_channel();

            self.to_task
                .clone()
                .send(IpfsEvent::PubsubUnsubscribe(topic.into(), tx))
                .await?;

            rx.await??
        }
        .instrument(self.span.clone())
        .await
    }

    /// Returns all known pubsub peers with the optional topic filter
    pub async fn pubsub_peers(
        &self,
        topic: impl Into<Option<String>>,
    ) -> Result<Vec<PeerId>, Error> {
        async move {
            let topic = topic.into();
            let (tx, rx) = oneshot_channel();

            self.to_task
                .clone()
                .send(IpfsEvent::PubsubPeers(topic, tx))
                .await?;

            rx.await?
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

            rx.await?
        }
        .instrument(self.span.clone())
        .await
    }

    /// Returns the known wantlist for the local node when the `peer` is `None` or the wantlist of the given `peer`
    pub async fn bitswap_wantlist(
        &self,
        peer: impl Into<Option<PeerId>>,
    ) -> Result<Vec<Cid>, Error> {
        async move {
            let peer = peer.into();
            let (tx, rx) = oneshot_channel();

            self.to_task
                .clone()
                .send(IpfsEvent::WantList(peer, tx))
                .await?;

            Ok(rx.await??.await)
        }
        .instrument(self.span.clone())
        .await
    }

    #[cfg(feature = "experimental_stream")]
    pub async fn stream_control(&self) -> Result<libp2p_stream::Control, Error> {
        async move {
            let (tx, rx) = oneshot_channel();

            self.to_task
                .clone()
                .send(IpfsEvent::StreamControlHandle(tx))
                .await?;

            rx.await?
        }
        .instrument(self.span.clone())
        .await
    }

    #[cfg(feature = "experimental_stream")]
    pub async fn new_stream(
        &self,
        protocol: impl Into<StreamProtocolRef>,
    ) -> Result<libp2p_stream::IncomingStreams, Error> {
        let protocol: StreamProtocol = protocol.into().try_into()?;
        async move {
            let (tx, rx) = oneshot_channel();

            self.to_task
                .clone()
                .send(IpfsEvent::NewStream(protocol, tx))
                .await?;

            rx.await?
        }
        .instrument(self.span.clone())
        .await
    }

    #[cfg(feature = "experimental_stream")]
    pub async fn open_stream(
        &self,
        peer_id: PeerId,
        protocol: impl Into<StreamProtocolRef>,
    ) -> Result<libp2p::Stream, Error> {
        let protocol: StreamProtocol = protocol.into().try_into()?;
        async move {
            let mut control = self.stream_control().await?;
            let stream = control
                .open_stream(peer_id, protocol)
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            Ok(stream)
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
            rx.await?
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

            rx.await?
        }
        .instrument(self.span.clone())
        .await
    }

    /// Add a given multiaddr as a listening address. Will fail if the address is unsupported, or
    /// if it is already being listened on. Currently will invoke `Swarm::listen_on` internally,
    /// returning the first `Multiaddr` that is being listened on.
    pub async fn add_listening_address(&self, addr: Multiaddr) -> Result<Multiaddr, Error> {
        async move {
            let (tx, rx) = oneshot_channel();

            self.to_task
                .clone()
                .send(IpfsEvent::AddListeningAddress(addr, tx))
                .await?;

            rx.await?
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

            rx.await?
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

            match rx.await?? {
                Either::Left(addrs) if !addrs.is_empty() => Ok(addrs),
                Either::Left(_) => unreachable!(),
                Either::Right(future) => {
                    future.await??;

                    let (tx, rx) = oneshot_channel();

                    self.to_task
                        .clone()
                        .send(IpfsEvent::FindPeer(peer_id, true, tx))
                        .await?;

                    match rx.await?? {
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

            rx.await??.ok_or_else(|| anyhow!("Provider already exist"))
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
        if !self.repo.contains(&cid).await? {
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

            Ok(rx.await??).map_err(|e: String| anyhow!(e))
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

            rx.await?
        }
        .instrument(self.span.clone())
        .await
    }

    /// Stores the given key + value record locally and replicates it in the DHT. It doesn't
    /// expire locally and is periodically replicated in the DHT, as per the `KademliaConfig`
    /// setup.
    pub async fn dht_put(
        &self,
        key: impl AsRef<[u8]>,
        value: impl Into<Vec<u8>>,
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
                .send(IpfsEvent::DhtPut(key, value.into(), quorum, tx))
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

    /// Add relay address
    pub async fn add_relay(&self, peer_id: PeerId, addr: Multiaddr) -> Result<(), Error> {
        async move {
            let (tx, rx) = oneshot_channel();

            self.to_task
                .clone()
                .send(IpfsEvent::AddRelay(peer_id, addr, tx))
                .await?;

            rx.await?
        }
        .instrument(self.span.clone())
        .await
    }

    /// Remove relay address
    pub async fn remove_relay(&self, peer_id: PeerId, addr: Multiaddr) -> Result<(), Error> {
        async move {
            let (tx, rx) = oneshot_channel();

            self.to_task
                .clone()
                .send(IpfsEvent::RemoveRelay(peer_id, addr, tx))
                .await?;

            rx.await?
        }
        .instrument(self.span.clone())
        .await
    }

    /// List all relays. if `active` is true, it will list all active relays
    pub async fn list_relays(&self, active: bool) -> Result<Vec<(PeerId, Vec<Multiaddr>)>, Error> {
        async move {
            let (tx, rx) = oneshot_channel();

            match active {
                true => {
                    self.to_task
                        .clone()
                        .send(IpfsEvent::ListActiveRelays(tx))
                        .await?
                }
                false => self.to_task.clone().send(IpfsEvent::ListRelays(tx)).await?,
            };

            rx.await?
        }
        .instrument(self.span.clone())
        .await
    }

    pub async fn enable_autorelay(&self) -> Result<(), Error> {
        Err(anyhow::anyhow!("Unimplemented"))
    }

    pub async fn disable_autorelay(&self) -> Result<(), Error> {
        Err(anyhow::anyhow!("Unimplemented"))
    }

    /// Enable use of a relay. If `peer_id` is `None`, it will select a relay at random to use, if one have been added
    pub async fn enable_relay(&self, peer_id: impl Into<Option<PeerId>>) -> Result<(), Error> {
        async move {
            let peer_id = peer_id.into();
            let (tx, rx) = oneshot_channel();

            self.to_task
                .clone()
                .send(IpfsEvent::EnableRelay(peer_id, tx))
                .await?;

            rx.await?
        }
        .instrument(self.span.clone())
        .await
    }

    /// Disable the use of a selected relay.
    pub async fn disable_relay(&self, peer_id: PeerId) -> Result<(), Error> {
        async move {
            let (tx, rx) = oneshot_channel();

            self.to_task
                .clone()
                .send(IpfsEvent::DisableRelay(peer_id, tx))
                .await?;

            rx.await?
        }
        .instrument(self.span.clone())
        .await
    }

    pub async fn rendezvous_register_namespace(
        &self,
        namespace: impl Into<String>,
        ttl: impl Into<Option<u64>>,
        peer_id: PeerId,
    ) -> Result<(), Error> {
        async move {
            let namespace = Namespace::new(namespace.into())?;
            let ttl = ttl.into();
            let (tx, rx) = oneshot_channel();

            self.to_task
                .clone()
                .send(IpfsEvent::RegisterRendezvousNamespace(
                    namespace, peer_id, ttl, tx,
                ))
                .await?;

            rx.await?
        }
        .instrument(self.span.clone())
        .await
    }

    pub async fn rendezvous_unregister_namespace(
        &self,
        namespace: impl Into<String>,
        peer_id: PeerId,
    ) -> Result<(), Error> {
        async move {
            let namespace = Namespace::new(namespace.into())?;

            let (tx, rx) = oneshot_channel();

            self.to_task
                .clone()
                .send(IpfsEvent::UnregisterRendezvousNamespace(
                    namespace, peer_id, tx,
                ))
                .await?;

            rx.await?
        }
        .instrument(self.span.clone())
        .await
    }

    pub async fn rendezvous_namespace_discovery(
        &self,
        namespace: impl Into<String>,
        ttl: impl Into<Option<u64>>,
        peer_id: PeerId,
    ) -> Result<HashMap<PeerId, Vec<Multiaddr>>, Error> {
        async move {
            let namespace = Namespace::new(namespace.into())?;
            let ttl = ttl.into();

            let (tx, rx) = oneshot_channel();

            self.to_task
                .clone()
                .send(IpfsEvent::RendezvousNamespaceDiscovery(
                    Some(namespace),
                    false,
                    ttl,
                    peer_id,
                    tx,
                ))
                .await?;

            rx.await?
        }
        .instrument(self.span.clone())
        .await
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

            rx.await?
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

        Ok(rx.await??.await)
    }

    /// Returns the keypair to the node
    pub fn keypair(&self) -> &Keypair {
        &self.key
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

pub enum StreamProtocolRef {
    Static(&'static str),
    Owned(String),
    Direct(StreamProtocol),
}

impl From<&'static str> for StreamProtocolRef {
    fn from(protocol: &'static str) -> Self {
        StreamProtocolRef::Static(protocol)
    }
}

impl From<String> for StreamProtocolRef {
    fn from(protocol: String) -> Self {
        StreamProtocolRef::Owned(protocol)
    }
}

impl From<StreamProtocol> for StreamProtocolRef {
    fn from(protocol: StreamProtocol) -> Self {
        StreamProtocolRef::Direct(protocol)
    }
}

impl TryFrom<StreamProtocolRef> for StreamProtocol {
    type Error = std::io::Error;
    fn try_from(protocl_ref: StreamProtocolRef) -> Result<Self, Self::Error> {
        let protocol = match protocl_ref {
            StreamProtocolRef::Direct(protocol) => protocol,
            StreamProtocolRef::Owned(protocol) => StreamProtocol::try_from_owned(protocol)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?,
            StreamProtocolRef::Static(protocol) => StreamProtocol::new(protocol),
        };

        Ok(protocol)
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
            Self::with_options(Some(trace_span!("ipfs", node = name.as_ref())), None).await
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
        pub async fn with_options(span: Option<Span>, addr: Option<Vec<Multiaddr>>) -> Self {
            // for future: assume UninitializedIpfs handles instrumenting any futures with the
            // given span
            let mut uninit = UninitializedIpfsNoop::new().with_default();

            if let Some(span) = span {
                uninit = uninit.set_span(span);
            }

            let list = match addr {
                Some(addr) => addr,
                None => vec!["/ip4/127.0.0.1/tcp/0".parse().unwrap()],
            };

            let ipfs = uninit.start().await.unwrap();

            ipfs.dht_mode(DhtMode::Server).await.unwrap();

            let id = ipfs.keypair().public().to_peer_id();
            for addr in list {
                ipfs.add_listening_address(addr).await.expect("To succeed");
            }

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
            &self.ipfs.repo.inner.subscriptions
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
        let new_data = ipfs.get_dag(cid).await.unwrap();
        assert_eq!(data, new_data);
    }

    #[tokio::test]
    async fn test_pin_and_unpin() {
        let ipfs = Node::new("test_node").await;

        let data = ipld!([-1, -2, -3]);
        let cid = ipfs.put_dag(data.clone()).pin(false).await.unwrap();

        assert!(ipfs.is_pinned(&cid).await.unwrap());
        ipfs.remove_pin(&cid).await.unwrap();
        assert!(!ipfs.is_pinned(&cid).await.unwrap());
    }
}
