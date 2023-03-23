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
pub mod p2p;
pub mod path;
pub mod refs;
pub mod repo;
mod subscription;
mod task;
pub mod unixfs;

#[macro_use]
extern crate tracing;

use anyhow::{anyhow, format_err};
use either::Either;
use futures::{
    channel::{
        mpsc::{channel, Sender},
        oneshot::{self, channel as oneshot_channel, Sender as OneshotSender},
    },
    sink::SinkExt,
    stream::{BoxStream, Stream},
    StreamExt,
};

use p2p::{
    IdentifyConfiguration, KadConfig, KadStoreConfig, PeerInfo, ProviderStream, RecordStream,
    RelayConfig,
};
use repo::{BlockStore, DataStore, Lock};
use tokio::{sync::Notify, task::JoinHandle};
use tracing::Span;
use tracing_futures::Instrument;
use unixfs::UnixfsStatus;

use std::{
    borrow::Borrow,
    collections::{HashMap, HashSet},
    fmt, io,
    ops::{Deref, DerefMut, Range},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use self::{
    dag::IpldDag,
    ipns::Ipns,
    p2p::{create_swarm, SwarmOptions, TSwarm},
    repo::{create_repo, Repo, RepoEvent},
};

pub use self::p2p::gossipsub::SubscriptionStream;

pub use self::{
    error::Error,
    p2p::BehaviourEvent,
    p2p::{Connection, KadResult, MultiaddrWithPeerId, MultiaddrWithoutPeerId},
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

use libp2p::{kad::KademliaConfig, ping::Config as PingConfig, swarm::dial_opts::DialOpts};

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

    /// The keypair used with libp2p, the identity of the node.
    pub keypair: Keypair,

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
    pub kad_store_config: Option<KadStoreConfig>,

    /// Ping Configuration
    pub ping_configuration: Option<PingConfig>,

    /// Enables port mapping (aka UPnP)
    pub port_mapping: bool,

    /// The span for tracing purposes, `None` value is converted to `tracing::trace_span!("ipfs")`.
    ///
    /// All futures returned by `Ipfs`, background task actions and swarm actions are instrumented
    /// with this span or spans referring to this as their parent. Setting this other than `None`
    /// default is useful when running multiple nodes.
    pub span: Option<Span>,
}

impl Default for IpfsOptions {
    fn default() -> Self {
        Self {
            ipfs_path: StoragePath::Memory,
            keypair: Keypair::generate_ed25519(),
            mdns: Default::default(),
            mdns_ipv6: Default::default(),
            dcutr: Default::default(),
            bootstrap: Default::default(),
            relay: Default::default(),
            disable_kad: Default::default(),
            keep_alive: Default::default(),
            relay_server: Default::default(),
            relay_server_config: Default::default(),
            kad_configuration: Default::default(),
            kad_store_config: Default::default(),
            ping_configuration: Default::default(),
            identify_configuration: Default::default(),
            listening_addrs: vec![
                "/ip4/0.0.0.0/tcp/0".parse().unwrap(),
                "/ip4/0.0.0.0/udp/0/quic-v1".parse().unwrap(),
                "/ip6/::/tcp/0".parse().unwrap(),
                "/ip6/::/udp/0/quic-v1".parse().unwrap(),
            ],
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
            .field("keypair", &DebuggableKeypair(&self.keypair))
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

/// Workaround for libp2p::identity::Keypair missing a Debug impl, works with references and owned
/// keypairs.
#[derive(Clone)]
struct DebuggableKeypair<I: Borrow<Keypair>>(I);

//TODO: Maybe remove this in the future?
impl<I: Borrow<Keypair>> fmt::Debug for DebuggableKeypair<I> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self.get_ref().clone().into_ed25519() {
            Some(_) => "Ed25519",
            _ => "Unknown",
        };

        write!(fmt, "Keypair::{kind}")
    }
}

impl<I: Borrow<Keypair>> DebuggableKeypair<I> {
    fn get_ref(&self) -> &Keypair {
        self.0.borrow()
    }
}

/// The facade for the Ipfs node.
///
/// The facade has most of the functionality either directly as a method or the functionality can
/// be implemented using the provided methods. For more information, see examples or the HTTP
/// endpoint implementations in `ipfs-http`.
///
/// The facade is created through [`UninitializedIpfs`] which is configured with [`IpfsOptions`].
#[derive(Debug)]
pub struct Ipfs {
    span: Span,
    repo: Repo,
    keys: DebuggableKeypair<Keypair>,
    identify_conf: IdentifyConfiguration,
    to_task: Sender<IpfsEvent>,
}

impl Clone for Ipfs {
    fn clone(&self) -> Self {
        Ipfs {
            span: self.span.clone(),
            repo: self.repo.clone(),
            identify_conf: self.identify_conf.clone(),
            keys: self.keys.clone(),
            to_task: self.to_task.clone(),
        }
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
    Listeners(Channel<Vec<Multiaddr>>),
    /// Connections
    Connections(Channel<Vec<Connection>>),
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
    /// Request background task to return the listened and external addresses
    GetAddresses(OneshotSender<Vec<Multiaddr>>),
    PubsubSubscribe(String, OneshotSender<Option<SubscriptionStream>>),
    PubsubUnsubscribe(String, OneshotSender<Result<bool, Error>>),
    PubsubPublish(
        String,
        Vec<u8>,
        OneshotSender<Result<MessageId, PublishError>>,
    ),
    PubsubPeers(Option<String>, OneshotSender<Vec<PeerId>>),
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
    AddPeer(PeerId, Option<Multiaddr>),
    GetClosestPeers(PeerId, OneshotSender<ReceiverChannel<KadResult>>),
    FindPeerIdentity(PeerId, OneshotSender<ReceiverChannel<PeerInfo>>),
    FindPeer(
        PeerId,
        bool,
        OneshotSender<Either<Vec<Multiaddr>, ReceiverChannel<KadResult>>>,
    ),
    WhitelistPeer(PeerId, Channel<()>),
    RemoveWhitelistPeer(PeerId, Channel<()>),
    GetProviders(Cid, OneshotSender<Option<ProviderStream>>),
    Provide(Cid, Channel<ReceiverChannel<KadResult>>),
    DhtGet(Key, OneshotSender<RecordStream>),
    DhtPut(Key, Vec<u8>, Quorum, Channel<ReceiverChannel<KadResult>>),
    GetBootstrappers(OneshotSender<Vec<Multiaddr>>),
    AddBootstrapper(MultiaddrWithPeerId, Channel<Multiaddr>),
    RemoveBootstrapper(MultiaddrWithPeerId, Channel<Multiaddr>),
    ClearBootstrappers(OneshotSender<Vec<Multiaddr>>),
    DefaultBootstrap(Channel<Vec<Multiaddr>>),
    Exit,
}

type TSwarmEvent = <TSwarm as Stream>::Item;
type TSwarmEventFn = Arc<dyn Fn(&mut TSwarm, &TSwarmEvent) + Sync + Send>;

#[derive(Debug, Copy, Clone)]
pub enum FDLimit {
    Max,
    Custom(u64),
}

/// Configured Ipfs which can only be started.

pub struct UninitializedIpfs {
    keys: Keypair,
    options: IpfsOptions,
    fdlimit: Option<FDLimit>,
    delay: bool,
    swarm_event: Option<TSwarmEventFn>,
}

impl Default for UninitializedIpfs {
    fn default() -> Self {
        Self::with_opt(Default::default())
    }
}

impl UninitializedIpfs {
    pub fn new() -> Self {
        Self::default()
    }

    /// Configures a new UninitializedIpfs with from the given options and optionally a span.
    /// If the span is not given, it is defaulted to `tracing::trace_span!("ipfs")`.
    ///
    /// The span is attached to all operations called on the later created `Ipfs` along with all
    /// operations done in the background task as well as tasks spawned by the underlying
    /// `libp2p::Swarm`.
    pub fn with_opt(options: IpfsOptions) -> Self {
        let keys = options.keypair.clone();
        let fdlimit = None;
        let delay = true;
        UninitializedIpfs {
            keys,
            options,
            fdlimit,
            delay,
            swarm_event: None,
        }
    }

    /// Adds a listening address
    pub fn add_listening_addr(mut self, addr: Multiaddr) -> Self {
        if !self.options.listening_addrs.contains(&addr) {
            self.options.listening_addrs.push(addr)
        }
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
    pub fn set_kad_configuration(
        mut self,
        config: KadConfig,
        store: Option<KadStoreConfig>,
    ) -> Self {
        self.options.kad_configuration = Some(Either::Left(config));
        self.options.kad_store_config = store;
        self
    }

    /// Set ping configuration
    pub fn set_ping_configuration(mut self, config: PingConfig) -> Self {
        self.options.ping_configuration = Some(config);
        self
    }

    /// Set keypair
    pub fn set_keypair(mut self, keypair: Keypair) -> Self {
        self.keys = keypair;
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
        F: Fn(&mut TSwarm, &TSwarmEvent) + Sync + Send + 'static,
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
            ..
        } = self;

        if let StoragePath::Disk(path) = &options.ipfs_path {
            if !path.is_dir() {
                tokio::fs::create_dir_all(path).await?;
            }
        }

        let (repo, repo_events) = create_repo(options.ipfs_path.clone());

        let root_span = options
            .span
            .take()
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

        repo.init().instrument(init_span.clone()).await?;

        let (to_task, receiver) = channel::<IpfsEvent>(1);
        let id_conf = options.identify_configuration.clone().unwrap_or_default();
        let ipfs = Ipfs {
            span: facade_span,
            repo: repo.clone(),
            identify_conf: id_conf,
            keys: DebuggableKeypair(keys),
            to_task,
        };

        // FIXME: mutating options above is an unfortunate side-effect of this call, which could be
        // reordered for less error prone code.
        let swarm_options = SwarmOptions::from(&options);

        let swarm_config = options.swarm_configuration.unwrap_or_default();
        let transport_config = options.transport_configuration.unwrap_or_default();
        let swarm = create_swarm(
            swarm_options,
            swarm_config,
            transport_config,
            repo.clone(),
            exec_span,
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
            kad_subscriptions,
            listener_subscriptions,
            repo,
            bootstraps,
            swarm_event,
        };

        for addr in listening_addrs.into_iter() {
            match fut.swarm.listen_on(addr) {
                Ok(id) => fut.listeners.insert(id),
                _ => continue,
            };
        }

        let notify = Arc::new(Notify::new());
        tokio::spawn({
            let notify = notify.clone();
            async move {
                fut.run(delay, notify).instrument(swarm_span).await;
            }
        });
        notify.notified().await;
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

    fn ipns(&self) -> Ipns {
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
        self.repo.get_block(cid).instrument(self.span.clone()).await
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
        use futures::stream::TryStreamExt;
        let span = debug_span!(parent: &self.span, "insert_pin", cid = %cid, recursive);
        let refs_span = debug_span!(parent: &span, "insert_pin refs");

        async move {
            // this needs to download everything but /pin/ls does not
            let block = self.repo.get_block(cid).await?;

            if !recursive {
                self.repo.insert_direct_pin(cid).await
            } else {
                let ipld = block.decode::<IpldCodec, Ipld>()?;

                let st = crate::refs::IpldRefs::default()
                    .with_only_unique()
                    .refs_of_resolved(self, vec![(*cid, ipld.clone())].into_iter())
                    .map_ok(|crate::refs::Edge { destination, .. }| destination)
                    .into_stream()
                    .instrument(refs_span)
                    .boxed();

                self.repo.insert_recursive_pin(cid, st).await
            }
        }
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
                    .refs_of_resolved(self.to_owned(), vec![(*cid, ipld.clone())].into_iter())
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
            .get(path)
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
        // convert early not to worry about the lifetime of parameter
        let starting_point = starting_point.into();
        unixfs::cat(self, starting_point, range)
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
        unixfs::add_file(self, path, None)
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
        unixfs::add(self, None, stream, None)
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
        unixfs::get(self, path, dest)
            .instrument(self.span.clone())
            .await
    }

    /// Resolves a ipns path to an ipld path; currently only supports dnslink resolution.
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

    /// Returns local listening addresses
    pub async fn addrs_local(&self) -> Result<Vec<Multiaddr>, Error> {
        async move {
            let (tx, rx) = oneshot_channel();
            self.to_task.clone().send(IpfsEvent::Listeners(tx)).await?;
            rx.await?
        }
        .instrument(self.span.clone())
        .await
    }

    /// Returns the connected peers
    pub async fn peers(&self) -> Result<Vec<Connection>, Error> {
        async move {
            let (tx, rx) = oneshot_channel();
            self.to_task
                .clone()
                .send(IpfsEvent::Connections(tx))
                .await?;
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

    pub async fn protocols(&self) -> Result<Vec<String>, Error> {
        async move {
            let (tx, rx) = oneshot_channel();
            self.to_task.clone().send(IpfsEvent::Protocol(tx)).await?;
            Ok(rx.await?)
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

                    rx.await?.await?
                }
                None => {
                    let (tx, rx) = oneshot_channel();
                    self.to_task
                        .clone()
                        .send(IpfsEvent::GetAddresses(tx))
                        .await?;

                    let mut addresses = rx.await?;
                    let protocols = self.protocols().await?;

                    let public_key = self.keys.get_ref().public();
                    let peer_id = public_key.to_peer_id();

                    for addr in &mut addresses {
                        addr.push(Protocol::P2p(peer_id.into()))
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
    // pub async fn bitswap_wantlist(
    //     &self,
    //     peer: Option<PeerId>,
    // ) -> Result<Vec<(Cid, ipfs_bitswap::Priority)>, Error> {
    //     async move {
    //         let (tx, rx) = oneshot_channel();

    //         self.to_task
    //             .clone()
    //             .send(IpfsEvent::WantList(peer, tx))
    //             .await?;

    //         Ok(rx.await?)
    //     }
    //     .instrument(self.span.clone())
    //     .await
    // }

    /// Returns a list of local blocks
    ///
    /// This implementation is subject to change into a stream, which might only include the pinned
    /// blocks.
    pub async fn refs_local(&self) -> Result<Vec<Cid>, Error> {
        self.repo.list_blocks().instrument(self.span.clone()).await
    }

    /// Returns the accumulated bitswap stats
    // pub async fn bitswap_stats(&self) -> Result<BitswapStats, Error> {
    //     async move {
    //         let (tx, rx) = oneshot_channel();

    //         self.to_task
    //             .clone()
    //             .send(IpfsEvent::BitswapStats(tx))
    //             .await?;

    //         Ok(rx.await?)
    //     }
    //     .instrument(self.span.clone())
    //     .await
    // }

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

            rx.await?
                .ok_or_else(|| anyhow!("Provider already exist"))
                .map(|s| s.0)
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

    /// Attempts to look a key up in the DHT and returns the values found in the records
    /// containing that key.
    pub async fn dht_get<T: Into<Key>>(&self, key: T) -> Result<RecordStream, Error> {
        async move {
            let (tx, rx) = oneshot_channel();

            self.to_task
                .clone()
                .send(IpfsEvent::DhtGet(key.into(), tx))
                .await?;

            Ok(rx.await?).map_err(|e: String| anyhow!(e))
        }
        .instrument(self.span.clone())
        .await
    }

    /// Stores the given key + value record locally and replicates it in the DHT. It doesn't
    /// expire locally and is periodically replicated in the DHT, as per the `KademliaConfig`
    /// setup.
    pub async fn dht_put<T: Into<Key>>(
        &self,
        key: T,
        value: Vec<u8>,
        quorum: Quorum,
    ) -> Result<(), Error> {
        let kad_result = async move {
            let (tx, rx) = oneshot_channel();

            self.to_task
                .clone()
                .send(IpfsEvent::DhtPut(key.into(), value, quorum, tx))
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
        refs::iplds_refs(self, iplds, max_depth, unique)
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
    pub async fn add_bootstrap(&self, addr: MultiaddrWithPeerId) -> Result<Multiaddr, Error> {
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
    pub async fn remove_bootstrap(&self, addr: MultiaddrWithPeerId) -> Result<Multiaddr, Error> {
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

    /// Add a known listen address of a peer participating in the DHT to the routing table.
    /// This is mandatory in order for the peer to be discoverable by other members of the
    /// DHT.
    pub async fn add_peer(
        &self,
        peer_id: PeerId,
        mut addr: Option<Multiaddr>,
    ) -> Result<(), Error> {
        // Kademlia::add_address requires the address to not contain the PeerId
        if let Some(addr) = addr.as_mut() {
            if matches!(addr.iter().last(), Some(Protocol::P2p(_))) {
                addr.pop();
            }
        }

        self.to_task
            .clone()
            .send(IpfsEvent::AddPeer(peer_id, addr))
            .await?;

        Ok(())
    }

    /// Returns the Bitswap peers for the a `Node`.
    // pub async fn get_bitswap_peers(&self) -> Result<Vec<PeerId>, Error> {
    //     let (tx, rx) = oneshot_channel();

    //     self.to_task
    //         .clone()
    //         .send(IpfsEvent::GetBitswapPeers(tx))
    //         .await?;

    //     rx.await.map_err(|e| anyhow!(e))
    // }

    /// Exit daemon.
    pub async fn exit_daemon(mut self) {
        // FIXME: this is a stopgap measure needed while repo is part of the struct Ipfs instead of
        // the background task or stream. After that this could be handled by dropping.
        self.repo.shutdown();

        // ignoring the error because it'd mean that the background task had already been dropped
        let _ = self.to_task.try_send(IpfsEvent::Exit);
    }
}

// <<<<<<< HEAD
// /// Background task of `Ipfs` created when calling `UninitializedIpfs::start`.
// // The receivers are Fuse'd so that we don't have to manage state on them being exhausted.
// #[allow(clippy::type_complexity)]
// struct IpfsFuture<Types: IpfsTypes> {
//     swarm: TSwarm<Types>,
//     repo_events: Fuse<Receiver<RepoEvent>>,
//     from_facade: Fuse<Receiver<IpfsEvent>>,
//     listening_addresses: HashMap<Multiaddr, ListenerId>,
//     listeners: HashSet<ListenerId>,
//     provider_stream: HashMap<QueryId, UnboundedSender<PeerId>>,
//     bitswap_provider_stream:
//         HashMap<QueryId, tokio::sync::mpsc::Sender<Result<HashSet<PeerId>, String>>>,
//     record_stream: HashMap<QueryId, UnboundedSender<Record>>,
//     repo: Repo<Types>,
//     kad_subscriptions: SubscriptionRegistry<KadResult, String>,
//     listener_subscriptions: SubscriptionRegistry<Option<Option<Multiaddr>>, String>,
//     autonat_limit: Arc<AtomicU64>,
//     autonat_counter: Arc<AtomicU64>,
//     bootstraps: HashSet<MultiaddrWithPeerId>,
//     swarm_event: Option<TSwarmEventFn<Types>>,
// }

// impl<TRepoTypes: RepoTypes> Future for IpfsFuture<TRepoTypes> {
//     type Output = ();

//     fn poll(mut self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Self::Output> {
//         use libp2p::swarm::SwarmEvent;

//         // begin by polling the swarm so that initially it'll first have chance to bind listeners
//         // and such.

//         let mut done = false;

//         loop {
//             loop {
//                 let inner = {
//                     let next = self.swarm.select_next_some();
//                     futures::pin_mut!(next);
//                     match next.poll(ctx) {
//                         Poll::Ready(inner) => inner,
//                         Poll::Pending if done => return Poll::Pending,
//                         Poll::Pending => break,
//                     }
//                 };
//                 // as a swarm event was returned, we need to do at least one more round to fully
//                 // exhaust the swarm before possibly causing the swarm to do more work by popping
//                 // off the events from Ipfs and ... this looping goes on for a while.
//                 done = false;
//                 if let Some(handler) = self.swarm_event.clone() {
//                     handler(&mut self.swarm, &inner)
//                 }
//                 match inner {
//                     SwarmEvent::NewListenAddr {
//                         listener_id,
//                         address,
//                     } => {
//                         self.listening_addresses
//                             .insert(address.clone(), listener_id);

//                         self.listener_subscriptions
//                             .finish_subscription(listener_id.into(), Ok(Some(Some(address))));
//                     }
//                     SwarmEvent::ExpiredListenAddr {
//                         listener_id,
//                         address,
//                     } => {
//                         self.listeners.remove(&listener_id);
//                         self.listening_addresses.remove(&address);
//                         self.listener_subscriptions
//                             .finish_subscription(listener_id.into(), Ok(Some(None)));
//                     }
//                     SwarmEvent::ListenerClosed {
//                         listener_id,
//                         reason,
//                         addresses,
//                     } => {
//                         self.listeners.remove(&listener_id);
//                         for address in addresses {
//                             self.listening_addresses.remove(&address);
//                         }
//                         let reason = reason.map(|_| Some(None)).map_err(|e| e.to_string());
//                         self.listener_subscriptions
//                             .finish_subscription(listener_id.into(), reason);
//                     }
//                     SwarmEvent::ListenerError { listener_id, error } => {
//                         self.listeners.remove(&listener_id);
//                         self.listener_subscriptions
//                             .finish_subscription(listener_id.into(), Err(error.to_string()));
//                     }
//                     SwarmEvent::Behaviour(BehaviourEvent::Mdns(event)) => match event {
//                         MdnsEvent::Discovered(list) => {
//                             for (peer, addr) in list {
//                                 trace!("mdns: Discovered peer {}", peer.to_base58());
//                                 self.swarm.behaviour_mut().add_peer(peer, Some(addr));
//                             }
//                         }
//                         MdnsEvent::Expired(list) => {
//                             for (peer, _) in list {
//                                 if let Some(mdns) = self.swarm.behaviour().mdns.as_ref() {
//                                     if !mdns.has_node(&peer) {
//                                         trace!("mdns: Expired peer {}", peer.to_base58());
//                                         self.swarm.behaviour_mut().remove_peer(&peer);
//                                     }
//                                 }
//                             }
//                         }
//                     },
//                     SwarmEvent::Behaviour(BehaviourEvent::Kad(event)) => {
//                         match event {
//                             InboundRequest { request } => {
//                                 trace!("kad: inbound {:?} request handled", request);
//                             }
//                             OutboundQueryProgressed {
//                                 result, id, step, ..
//                             } => {
//                                 // make sure the query is exhausted
//                                 if self.swarm.behaviour().kademlia.query(&id).is_none() {
//                                     match result {
//                                         // these subscriptions return actual values
//                                         GetClosestPeers(_) | GetProviders(_) | GetRecord(_) => {}
//                                         // we want to return specific errors for the following
//                                         Bootstrap(Err(_))
//                                         | StartProviding(Err(_))
//                                         | PutRecord(Err(_)) => {}
//                                         // and the rest can just return a general KadResult::Complete
//                                         _ => {
//                                             self.kad_subscriptions.finish_subscription(
//                                                 id.into(),
//                                                 Ok(KadResult::Complete),
//                                             );
//                                         }
//                                     }
//                                 }

//                                 match result {
//                                     Bootstrap(Ok(BootstrapOk {
//                                         peer,
//                                         num_remaining,
//                                     })) => {
//                                         debug!(
//                                             "kad: bootstrapped with {}, {} peers remain",
//                                             peer, num_remaining
//                                         );
//                                     }
//                                     Bootstrap(Err(BootstrapError::Timeout { .. })) => {
//                                         warn!("kad: timed out while trying to bootstrap");

//                                         if self.swarm.behaviour().kademlia.query(&id).is_none() {
//                                             self.kad_subscriptions.finish_subscription(
//                                                 id.into(),
//                                                 Err("kad: timed out while trying to bootstrap"
//                                                     .into()),
//                                             );
//                                         }
//                                     }
//                                     GetClosestPeers(Ok(GetClosestPeersOk { key: _, peers })) => {
//                                         if self.swarm.behaviour().kademlia.query(&id).is_none() {
//                                             self.kad_subscriptions.finish_subscription(
//                                                 id.into(),
//                                                 Ok(KadResult::Peers(peers)),
//                                             );
//                                         }
//                                     }
//                                     GetClosestPeers(Err(GetClosestPeersError::Timeout {
//                                         key: _,
//                                         peers: _,
//                                     })) => {
//                                         // don't mention the key here, as this is just the id of our node
//                                         warn!(
//                                             "kad: timed out while trying to find all closest peers"
//                                         );

//                                         if self.swarm.behaviour().kademlia.query(&id).is_none() {
//                                             self.kad_subscriptions.finish_subscription(
//                                 id.into(),
//                                 Err("timed out while trying to get providers for the given key"
//                                     .into()),
//                             );
//                                         }
//                                     }
//                                     GetProviders(Ok(GetProvidersOk::FoundProviders {
//                                         key: _,
//                                         providers,
//                                     })) => {
//                                         if let Entry::Occupied(entry) =
//                                             self.provider_stream.entry(id)
//                                         {
//                                             if !providers.is_empty() {
//                                                 tokio::spawn({
//                                                     let mut tx = entry.get().clone();
//                                                     let providers = providers.clone();
//                                                     async move {
//                                                         for provider in providers {
//                                                             let _ = tx.send(provider).await;
//                                                         }
//                                                     }
//                                                 });
//                                             }
//                                         }
//                                         if let Entry::Occupied(entry) =
//                                             self.bitswap_provider_stream.entry(id)
//                                         {
//                                             if !providers.is_empty() {
//                                                 tokio::spawn({
//                                                     let tx = entry.get().clone();
//                                                     async move {
//                                                         let _ = tx.send(Ok(providers)).await;
//                                                     }
//                                                 });
//                                             }
//                                         }
//                                     }
//                                     GetProviders(Ok(
//                                         GetProvidersOk::FinishedWithNoAdditionalRecord { .. },
//                                     )) => {
//                                         if step.last {
//                                             if let Some(tx) = self.provider_stream.remove(&id) {
//                                                 tx.close_channel();
//                                             }
//                                             if let Some(tx) =
//                                                 self.bitswap_provider_stream.remove(&id)
//                                             {
//                                                 drop(tx)
//                                             }
//                                         }
//                                     }
//                                     GetProviders(Err(GetProvidersError::Timeout {
//                                         key, ..
//                                     })) => {
//                                         let key = multibase::encode(Base::Base32Lower, key);
//                                         warn!(
//                                             "kad: timed out while trying to get providers for {}",
//                                             key
//                                         );

//                                         if self.swarm.behaviour().kademlia.query(&id).is_none() {
//                                             self.kad_subscriptions.finish_subscription(
//                                 id.into(),
//                                 Err("timed out while trying to get providers for the given key"
//                                     .into()),
//                             );
//                                         }
//                                     }
//                                     StartProviding(Ok(AddProviderOk { key })) => {
//                                         let key = multibase::encode(Base::Base32Lower, key);
//                                         debug!("kad: providing {}", key);
//                                     }
//                                     StartProviding(Err(AddProviderError::Timeout { key })) => {
//                                         let key = multibase::encode(Base::Base32Lower, key);
//                                         warn!("kad: timed out while trying to provide {}", key);

//                                         if self.swarm.behaviour().kademlia.query(&id).is_none() {
//                                             self.kad_subscriptions.finish_subscription(
//                                 id.into(),
//                                 Err("kad: timed out while trying to provide the record".into()),
//                             );
//                                         }
//                                     }
//                                     RepublishProvider(Ok(AddProviderOk { key })) => {
//                                         let key = multibase::encode(Base::Base32Lower, key);
//                                         debug!("kad: republished provider {}", key);
//                                     }
//                                     RepublishProvider(Err(AddProviderError::Timeout { key })) => {
//                                         let key = multibase::encode(Base::Base32Lower, key);
//                                         warn!(
//                                             "kad: timed out while trying to republish provider {}",
//                                             key
//                                         );
//                                     }
//                                     GetRecord(Ok(GetRecordOk::FoundRecord(record))) => {
//                                         if let Entry::Occupied(entry) = self.record_stream.entry(id)
//                                         {
//                                             tokio::spawn({
//                                                 let mut tx = entry.get().clone();
//                                                 async move {
//                                                     let _ = tx.send(record.record).await;
//                                                 }
//                                             });
//                                         }
//                                     }
//                                     GetRecord(Ok(
//                                         GetRecordOk::FinishedWithNoAdditionalRecord { .. },
//                                     )) => {
//                                         if step.last {
//                                             if let Some(tx) = self.record_stream.remove(&id) {
//                                                 tx.close_channel();
//                                             }
//                                         }
//                                     }
//                                     GetRecord(Err(GetRecordError::NotFound {
//                                         key,
//                                         closest_peers: _,
//                                     })) => {
//                                         let key = multibase::encode(Base::Base32Lower, key);
//                                         warn!("kad: couldn't find record {}", key);

//                                         if self.swarm.behaviour().kademlia.query(&id).is_none() {
//                                             if let Some(tx) = self.record_stream.remove(&id) {
//                                                 tx.close_channel();
//                                             }
//                                         }
//                                     }
//                                     GetRecord(Err(GetRecordError::QuorumFailed {
//                                         key,
//                                         records: _,
//                                         quorum,
//                                     })) => {
//                                         let key = multibase::encode(Base::Base32Lower, key);
//                                         warn!(
//                                             "kad: quorum failed {} when trying to get key {}",
//                                             quorum, key
//                                         );

//                                         if self.swarm.behaviour().kademlia.query(&id).is_none() {
//                                             if let Some(tx) = self.record_stream.remove(&id) {
//                                                 tx.close_channel();
//                                             }
//                                         }
//                                     }
//                                     GetRecord(Err(GetRecordError::Timeout { key })) => {
//                                         let key = multibase::encode(Base::Base32Lower, key);
//                                         warn!("kad: timed out while trying to get key {}", key);

//                                         if self.swarm.behaviour().kademlia.query(&id).is_none() {
//                                             if let Some(tx) = self.record_stream.remove(&id) {
//                                                 tx.close_channel();
//                                             }
//                                         }
//                                     }
//                                     PutRecord(Ok(PutRecordOk { key }))
//                                     | RepublishRecord(Ok(PutRecordOk { key })) => {
//                                         let key = multibase::encode(Base::Base32Lower, key);
//                                         debug!("kad: successfully put record {}", key);
//                                     }
//                                     PutRecord(Err(PutRecordError::QuorumFailed {
//                                         key,
//                                         success: _,
//                                         quorum,
//                                     }))
//                                     | RepublishRecord(Err(PutRecordError::QuorumFailed {
//                                         key,
//                                         success: _,
//                                         quorum,
//                                     })) => {
//                                         let key = multibase::encode(Base::Base32Lower, key);
//                                         warn!(
//                                             "kad: quorum failed ({}) when trying to put record {}",
//                                             quorum, key
//                                         );

//                                         if self.swarm.behaviour().kademlia.query(&id).is_none() {
//                                             self.kad_subscriptions.finish_subscription(
//                                 id.into(),
//                                 Err("kad: quorum failed when trying to put the record".into()),
//                             );
//                                         }
//                                     }
//                                     PutRecord(Err(PutRecordError::Timeout {
//                                         key,
//                                         success: _,
//                                         quorum: _,
//                                     })) => {
//                                         let key = multibase::encode(Base::Base32Lower, key);
//                                         warn!("kad: timed out while trying to put record {}", key);

//                                         if self.swarm.behaviour().kademlia.query(&id).is_none() {
//                                             self.kad_subscriptions.finish_subscription(
//                                                 id.into(),
//                                                 Err(
//                                                     "kad: timed out while trying to put the record"
//                                                         .into(),
//                                                 ),
//                                             );
//                                         }
//                                     }
//                                     RepublishRecord(Err(PutRecordError::Timeout {
//                                         key,
//                                         success: _,
//                                         quorum: _,
//                                     })) => {
//                                         let key = multibase::encode(Base::Base32Lower, key);
//                                         warn!(
//                                             "kad: timed out while trying to republish record {}",
//                                             key
//                                         );
//                                     }
//                                 }
//                             }
//                             RoutingUpdated {
//                                 peer,
//                                 is_new_peer: _,
//                                 addresses,
//                                 bucket_range: _,
//                                 old_peer: _,
//                             } => {
//                                 trace!("kad: routing updated; {}: {:?}", peer, addresses);
//                             }
//                             UnroutablePeer { peer } => {
//                                 trace!("kad: peer {} is unroutable", peer);
//                             }
//                             RoutablePeer { peer, address } => {
//                                 trace!("kad: peer {} ({}) is routable", peer, address);
//                             }
//                             PendingRoutablePeer { peer, address } => {
//                                 trace!("kad: pending routable peer {} ({})", peer, address);
//                             }
//                         }
//                     }
//                     SwarmEvent::Behaviour(BehaviourEvent::Bitswap(event)) => match event {
//                         BitswapEvent::Provide { key } => {
//                             let key = key.hash().to_bytes();
//                             let _id = self
//                                 .swarm
//                                 .behaviour_mut()
//                                 .kademlia
//                                 .start_providing(key.into())
//                                 .ok();
//                         }
//                         BitswapEvent::FindProviders { key, response, .. } => {
//                             let key = key.hash().to_bytes();
//                             let id = self
//                                 .swarm
//                                 .behaviour_mut()
//                                 .kademlia
//                                 .get_providers(key.into());
//                             self.bitswap_provider_stream.insert(id, response);
//                         }
//                         BitswapEvent::Ping { peer, response } => {
//                             let duration = self.swarm.behaviour().swarm.peer_rtt(&peer);
//                             let _ = response.send(duration).ok();
//                         }
//                     },
//                     SwarmEvent::Behaviour(BehaviourEvent::Ping(event)) => match event {
//                         libp2p::ping::Event {
//                             peer,
//                             result: Result::Ok(PingSuccess::Ping { rtt }),
//                         } => {
//                             trace!(
//                                 "ping: rtt to {} is {} ms",
//                                 peer.to_base58(),
//                                 rtt.as_millis()
//                             );
//                             self.swarm.behaviour_mut().swarm.set_rtt(&peer, rtt);
//                         }
//                         libp2p::ping::Event {
//                             peer,
//                             result: Result::Ok(PingSuccess::Pong),
//                         } => {
//                             trace!("ping: pong from {}", peer);
//                         }
//                         libp2p::ping::Event {
//                             peer,
//                             result: Result::Err(libp2p::ping::Failure::Timeout),
//                         } => {
//                             trace!("ping: timeout to {}", peer);
//                             self.swarm.behaviour_mut().remove_peer(&peer);
//                         }
//                         libp2p::ping::Event {
//                             peer,
//                             result: Result::Err(libp2p::ping::Failure::Other { error }),
//                         } => {
//                             error!("ping: failure with {}: {}", peer.to_base58(), error);
//                         }
//                         libp2p::ping::Event {
//                             peer,
//                             result: Result::Err(libp2p::ping::Failure::Unsupported),
//                         } => {
//                             error!("ping: failure with {}: unsupported", peer.to_base58());
//                         }
//                     },
//                     SwarmEvent::Behaviour(BehaviourEvent::Identify(event)) => match event {
//                         IdentifyEvent::Received { peer_id, info } => {
//                             self.swarm
//                                 .behaviour_mut()
//                                 .swarm
//                                 .inject_identify_info(peer_id, info.clone());

//                             let IdentifyInfo {
//                                 listen_addrs,
//                                 protocols,
//                                 ..
//                             } = info.clone();

//                             self.swarm
//                                 .behaviour_mut()
//                                 .bitswap
//                                 .on_identify(&peer_id, &protocols);
//                             if protocols
//                                 .iter()
//                                 .any(|p| p.as_bytes() == libp2p::kad::protocol::DEFAULT_PROTO_NAME)
//                             {
//                                 for addr in &listen_addrs {
//                                     self.swarm
//                                         .behaviour_mut()
//                                         .kademlia()
//                                         .add_address(&peer_id, addr.clone());
//                                 }
//                             }

//                             #[allow(clippy::collapsible_if)]
//                             if protocols
//                                 .iter()
//                                 .any(|p| p.as_bytes() == libp2p::autonat::DEFAULT_PROTOCOL_NAME)
//                             {
//                                 if self.autonat_counter.load(Ordering::Relaxed)
//                                     <= self.autonat_limit.load(Ordering::Relaxed)
//                                 {
//                                     for addr in listen_addrs {
//                                         self.swarm
//                                             .behaviour_mut()
//                                             .autonat
//                                             .add_server(peer_id, Some(addr));
//                                     }

//                                     let mut counter = self.autonat_counter.load(Ordering::Relaxed);
//                                     counter += 1;
//                                     self.autonat_counter.store(counter, Ordering::Relaxed);
//                                 }
//                             }
//                         }
//                         event => trace!("identify: {:?}", event),
//                     },
//                     SwarmEvent::Behaviour(BehaviourEvent::Autonat(
//                         autonat::Event::StatusChanged { old, new },
//                     )) => {
//                         //TODO: Use status to indicate if we should port forward or not
//                         debug!("Old Nat Status: {:?}", old);
//                         debug!("New Nat Status: {:?}", new);
//                     }
//                     _ => trace!("Swarm event: {:?}", inner),
//                 }
//             }

//             // temporary pinning of the receivers should be safe as we are pinning through the
//             // already pinned self. with the receivers we can also safely ignore exhaustion
//             // as those are fused.
//             loop {
//                 let inner = match Pin::new(&mut self.from_facade).poll_next(ctx) {
//                     Poll::Ready(Some(evt)) => evt,
//                     // doing teardown also after the `Ipfs` has been dropped
//                     Poll::Ready(None) => IpfsEvent::Exit,
//                     Poll::Pending => break,
//                 };

//                 match inner {
//                     IpfsEvent::Connect(target, ret) => {
//                         ret.send(self.swarm.behaviour_mut().connect(target)).ok();
//                     }
//                     IpfsEvent::Protocol(ret) => {
//                         let info = self.swarm.behaviour().supported_protocols();

//                         let _ = ret.send(info);
//                     }
//                     IpfsEvent::SwarmDial(opt, ret) => {
//                         let result = self.swarm.dial(opt);
//                         let _ = ret.send(result);
//                     }
//                     IpfsEvent::Addresses(ret) => {
//                         let addrs = self.swarm.behaviour_mut().addrs();
//                         ret.send(Ok(addrs)).ok();
//                     }
//                     IpfsEvent::Listeners(ret) => {
//                         let listeners = self.swarm.listeners().cloned().collect::<Vec<Multiaddr>>();
//                         ret.send(Ok(listeners)).ok();
//                     }
//                     IpfsEvent::Connections(ret) => {
//                         let connections = self.swarm.behaviour_mut().connections();
//                         ret.send(Ok(connections.collect())).ok();
//                     }
//                     IpfsEvent::Connected(ret) => {
//                         let connections = self.swarm.connected_peers().cloned();
//                         ret.send(Ok(connections.collect())).ok();
//                     }
//                     IpfsEvent::Disconnect(peer, ret) => {
//                         let _ = ret.send(
//                             self.swarm
//                                 .disconnect_peer_id(peer)
//                                 .map_err(|_| anyhow::anyhow!("Peer was not connected")),
//                         );
//                     }
//                     IpfsEvent::Ban(peer, ret) => {
//                         self.swarm.ban_peer_id(peer);
//                         let _ = ret.send(Ok(()));
//                     }
//                     IpfsEvent::Unban(peer, ret) => {
//                         self.swarm.unban_peer_id(peer);
//                         let _ = ret.send(Ok(()));
//                     }
//                     IpfsEvent::GetAddresses(ret) => {
//                         // perhaps this could be moved under `IpfsEvent` or free functions?
//                         let mut addresses = Vec::new();
//                         addresses.extend(self.swarm.listeners().map(|a| a.to_owned()));
//                         addresses
//                             .extend(self.swarm.external_addresses().map(|ar| ar.addr.to_owned()));
//                         // ignore error, perhaps caller went away already
//                         let _ = ret.send(addresses);
//                     }
//                     IpfsEvent::PubsubSubscribe(topic, ret) => {
//                         let pubsub = self.swarm.behaviour_mut().pubsub().subscribe(topic).ok();
//                         let _ = ret.send(pubsub);
//                     }
//                     IpfsEvent::PubsubUnsubscribe(topic, ret) => {
//                         let _ = ret.send(self.swarm.behaviour_mut().pubsub().unsubscribe(topic));
//                     }
//                     IpfsEvent::PubsubPublish(topic, data, ret) => {
//                         let _ = ret.send(self.swarm.behaviour_mut().pubsub().publish(topic, data));
//                     }
//                     IpfsEvent::PubsubPeers(Some(topic), ret) => {
//                         let _ =
//                             ret.send(self.swarm.behaviour_mut().pubsub().subscribed_peers(&topic));
//                     }
//                     IpfsEvent::PubsubPeers(None, ret) => {
//                         let _ = ret.send(self.swarm.behaviour_mut().pubsub().known_peers());
//                     }
//                     IpfsEvent::PubsubSubscribed(ret) => {
//                         let _ = ret.send(self.swarm.behaviour_mut().pubsub().subscribed_topics());
//                     }
//                     // IpfsEvent::WantList(peer, ret) => {
//                     //     let list = if let Some(peer) = peer {
//                     //         self.swarm
//                     //             .behaviour_mut()
//                     //             .bitswap()
//                     //             .peer_wantlist(&peer)
//                     //             .unwrap_or_default()
//                     //     } else {
//                     //         self.swarm.behaviour_mut().bitswap().local_wantlist()
//                     //     };
//                     //     let _ = ret.send(list);
//                     // }
//                     // IpfsEvent::BitswapStats(ret) => {
//                     //     let stats = self.swarm.behaviour_mut().bitswap().stats();
//                     //     let peers = self.swarm.behaviour_mut().bitswap().peers();
//                     //     let wantlist = self.swarm.behaviour_mut().bitswap().local_wantlist();
//                     //     let _ = ret.send((stats, peers, wantlist).into());
//                     // }
//                     IpfsEvent::AddListeningAddress(addr, ret) => match self.swarm.listen_on(addr) {
//                         Ok(id) => {
//                             self.listeners.insert(id);
//                             let fut = self
//                                 .listener_subscriptions
//                                 .create_subscription(id.into(), None);
//                             let _ = ret.send(Ok(fut));
//                         }
//                         Err(e) => {
//                             let _ = ret.send(Err(anyhow::anyhow!(e)));
//                         }
//                     },
//                     IpfsEvent::RemoveListeningAddress(addr, ret) => {
//                         let removed = if let Some(id) = self.listening_addresses.remove(&addr) {
//                             if !self.swarm.remove_listener(id) {
//                                 Err(format_err!(
//                                     "Failed to remove previously added listening address: {}",
//                                     addr
//                                 ))
//                             } else {
//                                 self.listeners.remove(&id);
//                                 let fut = self
//                                     .listener_subscriptions
//                                     .create_subscription(id.into(), None);
//                                 Ok(fut)
//                             }
//                         } else {
//                             Err(format_err!("Address was not listened to before: {}", addr))
//                         };

//                         let _ = ret.send(removed);
//                     }
//                     IpfsEvent::Bootstrap(ret) => {
//                         let future = match self.swarm.behaviour_mut().kademlia.bootstrap() {
//                             Ok(id) => {
//                                 Ok(self.kad_subscriptions.create_subscription(id.into(), None))
//                             }
//                             Err(e) => {
//                                 error!("kad: can't bootstrap the node: {:?}", e);
//                                 Err(anyhow!("kad: can't bootstrap the node: {:?}", e))
//                             }
//                         };
//                         let _ = ret.send(future);
//                     }
//                     IpfsEvent::AddPeer(peer_id, addr) => {
//                         self.swarm.behaviour_mut().add_peer(peer_id, addr);
//                     }
//                     IpfsEvent::GetClosestPeers(peer_id, ret) => {
//                         let id = self
//                             .swarm
//                             .behaviour_mut()
//                             .kademlia
//                             .get_closest_peers(peer_id);

//                         let future = self.kad_subscriptions.create_subscription(id.into(), None);

//                         let _ = ret.send(future);
//                     }
//                     IpfsEvent::FindPeerIdentity(peer_id, local_only, ret) => {
//                         let locally_known = self
//                             .swarm
//                             .behaviour()
//                             .swarm
//                             .peers()
//                             .find(|(k, _)| peer_id.eq(k))
//                             .and_then(|(_, v)| v.clone())
//                             .map(|v| v.into());

//                         let addrs = if locally_known.is_some() || local_only {
//                             Either::Left(locally_known)
//                         } else {
//                             Either::Right({
//                                 let id = self
//                                     .swarm
//                                     .behaviour_mut()
//                                     .kademlia
//                                     .get_closest_peers(peer_id);

//                                 self.kad_subscriptions.create_subscription(id.into(), None)
//                             })
//                         };
//                         let _ = ret.send(addrs);
//                     }
//                     IpfsEvent::FindPeer(peer_id, local_only, ret) => {
//                         let swarm_addrs = self.swarm.behaviour_mut().swarm.connections_to(&peer_id);
//                         let locally_known_addrs = if !swarm_addrs.is_empty() {
//                             swarm_addrs
//                         } else {
//                             self.swarm
//                                 .behaviour_mut()
//                                 .kademlia()
//                                 .addresses_of_peer(&peer_id)
//                         };
//                         let addrs = if !locally_known_addrs.is_empty() || local_only {
//                             Either::Left(locally_known_addrs)
//                         } else {
//                             Either::Right({
//                                 let id = self
//                                     .swarm
//                                     .behaviour_mut()
//                                     .kademlia
//                                     .get_closest_peers(peer_id);

//                                 self.kad_subscriptions.create_subscription(id.into(), None)
//                             })
//                         };
//                         let _ = ret.send(addrs);
//                     }
//                     IpfsEvent::GetProviders(cid, ret) => {
//                         let key = Key::from(cid.hash().to_bytes());
//                         let id = self.swarm.behaviour_mut().kademlia.get_providers(key);
//                         let (tx, mut rx) = futures::channel::mpsc::unbounded();
//                         let stream = async_stream::stream! {
//                             let mut current_providers: HashSet<PeerId> = Default::default();
//                             while let Some(provider) = rx.next().await {
//                                 if current_providers.insert(provider) {
//                                     yield provider;
//                                 }
//                             }
//                         };
//                         self.provider_stream.insert(id, tx);

//                         let _ = ret.send(Some(ProviderStream(stream.boxed())));
//                     }
//                     IpfsEvent::Provide(cid, ret) => {
//                         let key = Key::from(cid.hash().to_bytes());
//                         let future = match self.swarm.behaviour_mut().kademlia.start_providing(key)
//                         {
//                             Ok(id) => {
//                                 Ok(self.kad_subscriptions.create_subscription(id.into(), None))
//                             }
//                             Err(e) => {
//                                 error!("kad: can't provide a key: {:?}", e);
//                                 Err(anyhow!("kad: can't provide the key: {:?}", e))
//                             }
//                         };
//                         let _ = ret.send(future);
//                     }
//                     IpfsEvent::DhtGet(key, ret) => {
//                         let id = self.swarm.behaviour_mut().kademlia.get_record(key);
//                         let (tx, mut rx) = futures::channel::mpsc::unbounded();
//                         let stream = async_stream::stream! {
//                             while let Some(record) = rx.next().await {
//                                     yield record;
//                             }
//                         };
//                         self.record_stream.insert(id, tx);

//                         let _ = ret.send(RecordStream(stream.boxed()));
//                     }
//                     IpfsEvent::DhtPut(key, value, quorum, ret) => {
//                         let record = Record {
//                             key,
//                             value,
//                             publisher: None,
//                             expires: None,
//                         };
//                         let future = match self
//                             .swarm
//                             .behaviour_mut()
//                             .kademlia
//                             .put_record(record, quorum)
//                         {
//                             Ok(id) => {
//                                 Ok(self.kad_subscriptions.create_subscription(id.into(), None))
//                             }
//                             Err(e) => {
//                                 error!("kad: can't put a record: {:?}", e);
//                                 Err(anyhow!("kad: can't provide the record: {:?}", e))
//                             }
//                         };
//                         let _ = ret.send(future);
//                     }
//                     IpfsEvent::GetBootstrappers(ret) => {
//                         let list = self
//                             .bootstraps
//                             .iter()
//                             .map(MultiaddrWithPeerId::clone)
//                             .map(Multiaddr::from)
//                             .collect::<Vec<_>>();
//                         let _ = ret.send(list);
//                     }
//                     IpfsEvent::AddBootstrapper(addr, ret) => {
//                         let ret_addr = addr.clone().into();
//                         if self.bootstraps.insert(addr.clone()) {
//                             let MultiaddrWithPeerId {
//                                 multiaddr: ma,
//                                 peer_id,
//                             } = addr;

//                             self.swarm
//                                 .behaviour_mut()
//                                 .kademlia
//                                 .add_address(&peer_id, ma.into());
//                             // the return value of add_address doesn't implement Debug
//                             trace!(peer_id=%peer_id, "tried to add a bootstrapper");
//                         }
//                         let _ = ret.send(Ok(ret_addr));
//                     }
//                     IpfsEvent::RemoveBootstrapper(addr, ret) => {
//                         let result = addr.clone().into();
//                         if self.bootstraps.remove(&addr) {
//                             let peer_id = addr.peer_id;
//                             let prefix: Multiaddr = addr.multiaddr.into();

//                             if let Some(e) = self
//                                 .swarm
//                                 .behaviour_mut()
//                                 .kademlia
//                                 .remove_address(&peer_id, &prefix)
//                             {
//                                 info!(peer_id=%peer_id, status=?e.status, "removed bootstrapper");
//                             } else {
//                                 warn!(peer_id=%peer_id, "attempted to remove an unknown bootstrapper");
//                             }
//                         }
//                         let _ = ret.send(Ok(result));
//                     }
//                     IpfsEvent::ClearBootstrappers(ret) => {
//                         let removed = self.bootstraps.drain().collect::<Vec<_>>();
//                         let mut list = Vec::with_capacity(removed.len());
//                         for addr_with_peer_id in removed {
//                             let peer_id = &addr_with_peer_id.peer_id;
//                             let prefix: Multiaddr = addr_with_peer_id.multiaddr.clone().into();

//                             if let Some(e) = self
//                                 .swarm
//                                 .behaviour_mut()
//                                 .kademlia
//                                 .remove_address(peer_id, &prefix)
//                             {
//                                 info!(peer_id=%peer_id, status=?e.status, "cleared bootstrapper");
//                                 list.push(addr_with_peer_id.into());
//                             } else {
//                                 error!(peer_id=%peer_id, "attempted to clear an unknown bootstrapper");
//                             }
//                         }
//                         let _ = ret.send(list);
//                     }
//                     IpfsEvent::DefaultBootstrap(ret) => {
//                         let mut rets = Vec::new();

//                         for addr in BOOTSTRAP_NODES {
//                             let addr = addr
//                                 .parse::<MultiaddrWithPeerId>()
//                                 .expect("see test bootstrap_nodes_are_multiaddr_with_peerid");
//                             if self.bootstraps.insert(addr.clone()) {
//                                 let MultiaddrWithPeerId {
//                                     multiaddr: ma,
//                                     peer_id,
//                                 } = addr.clone();

//                                 // this is intentionally the multiaddr without peerid turned into plain multiaddr:
//                                 // libp2p cannot dial addresses which include peerids.
//                                 let ma: Multiaddr = ma.into();

//                                 // same as with add_bootstrapper: the return value from kademlia.add_address
//                                 // doesn't implement Debug
//                                 self.swarm
//                                     .behaviour_mut()
//                                     .kademlia
//                                     .add_address(&peer_id, ma.clone());
//                                 trace!(peer_id=%peer_id, "tried to restore a bootstrapper");

//                                 // report with the peerid
//                                 let reported: Multiaddr = addr.into();
//                                 rets.push(reported);
//                             }
//                         }

//                         let _ = ret.send(Ok(rets));
//                     }
//                     IpfsEvent::Exit => {
//                         // FIXME: we could do a proper teardown
//                         return Poll::Ready(());
//                     }
//                 }
//             }

//             // Poll::Ready(None) and Poll::Pending can be used to break out of the loop, clippy
//             // wants this to be written with a `while let`.
//             while let Poll::Ready(Some(evt)) = Pin::new(&mut self.repo_events).poll_next(ctx) {
//                 match evt {
//                     RepoEvent::WantBlock(cid) => {
//                         let client = self.swarm.behaviour_mut().bitswap().client().clone();
//                         let repo = self.repo.clone();
//                         tokio::spawn(async move {
//                             let session = client.new_session().await;
//                             if let Some(block) =
//                                 session.get_block(&cid).await.ok().and_then(|block| {
//                                     Block::new(block.cid, block.data.to_vec()).ok()
//                                 })
//                             {
//                                 let res = repo.put_block(block).await;
//                                 if let Err(e) = res {
//                                     debug!("Got block {} but failed to store it: {}", cid, e);
//                                 }
//                             }
//                             let _ = session.stop().await.ok();
//                         });
//                     }
//                     RepoEvent::UnwantBlock(_cid) => {}
//                     RepoEvent::NewBlock(block, ret) => {
//                         let client = self.swarm.behaviour_mut().bitswap().client().clone();
//                         let server = self.swarm.behaviour_mut().bitswap().server().cloned();
//                         tokio::task::spawn(async move {
//                             let block = iroh_bitswap::Block::new(
//                                 bytes::Bytes::copy_from_slice(block.data()),
//                                 *block.cid(),
//                             );
//                             if let Err(err) = client.notify_new_blocks(&[block.clone()]).await {
//                                 warn!("failed to notify bitswap about blocks: {:?}", err);
//                             }
//                             if let Some(server) = server {
//                                 if let Err(err) = server.notify_new_blocks(&[block]).await {
//                                     warn!("failed to notify bitswap about blocks: {:?}", err);
//                                 }
//                             }
//                         });
//                         let _ = ret.send(Err(anyhow!("not actively providing blocks yet")));
//                     }
//                     RepoEvent::RemovedBlock(cid) => {
//                         self.swarm.behaviour_mut().stop_providing_block(&cid)
//                     }
//                 }
//             }

//             done = true;
//         }
//     }
// }

// pub fn peerid_from_multiaddr(addr: &Multiaddr) -> anyhow::Result<PeerId> {
// =======
#[allow(dead_code)]
pub(crate) fn peerid_from_multiaddr(addr: &Multiaddr) -> anyhow::Result<PeerId> {
    let mut addr = addr.clone();
    let peer_id = match addr.pop() {
        Some(Protocol::P2p(hash)) => {
            PeerId::from_multihash(hash).map_err(|_| anyhow::anyhow!("Multihash is not valid"))?
        }
        _ => anyhow::bail!("Invalid PeerId"),
    };
    Ok(peer_id)
}

#[doc(hidden)]
pub use node::Node;

/// Node module provides an easy to use interface used in `tests/`.
mod node {
    use futures::TryFutureExt;

    use super::*;
    use std::convert::TryFrom;

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
        pub async fn connect(&self, addr: Multiaddr) -> Result<(), Error> {
            let addr = MultiaddrWithPeerId::try_from(addr).unwrap();
            if self.ipfs.is_connected(addr.peer_id).await? {
                return Ok(());
            }
            self.ipfs.connect(addr).await
        }

        /// Returns a new `Node` based on `IpfsOptions`.
        pub async fn with_options(opts: IpfsOptions) -> Self {
            let id = opts.keypair.public().to_peer_id();

            // for future: assume UninitializedIpfs handles instrumenting any futures with the
            // given span
            let ipfs: Ipfs = UninitializedIpfs::with_opt(opts).start().await.unwrap();

            let addrs = ipfs.identity(None).await.unwrap().listen_addrs;

            Node { ipfs, id, addrs }
        }

        /// Returns the subscriptions for a `Node`.
        pub fn get_subscriptions(
            &self,
        ) -> &parking_lot::RwLock<subscription::Subscriptions<Block, String>> {
            &self.ipfs.repo.subscriptions.subscriptions
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
