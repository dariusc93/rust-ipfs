use either::Either;
use futures::future::Either as FutureEither;
use libp2p::core::muxing::StreamMuxerBox;
use libp2p::core::transport::timeout::TransportTimeout;
use libp2p::core::transport::upgrade::Version;
use libp2p::core::transport::{Boxed, MemoryTransport, OrTransport};
use libp2p::core::upgrade::SelectUpgrade;
use libp2p::dns::{ResolverConfig, ResolverOpts, TokioDnsConfig};
use libp2p::quic::tokio::Transport as TokioQuicTransport;
use libp2p::quic::Config as QuicConfig;
use libp2p::relay::client::Transport as ClientTransport;
use libp2p::tcp::{tokio::Transport as TokioTcpTransport, Config as GenTcpConfig};
use libp2p::yamux::{Config as YamuxConfig, WindowUpdateMode};
use libp2p::{identity, noise};
use libp2p::{PeerId, Transport};
use libp2p_mplex::MplexConfig;
use std::io::{self, ErrorKind};
use std::time::Duration;
use trust_dns_resolver::system_conf;

/// Transport type.
pub(crate) type TTransport = Boxed<(PeerId, StreamMuxerBox)>;

#[derive(Debug, Clone, Copy)]
pub struct TransportConfig {
    pub yamux_max_buffer_size: usize,
    pub yamux_receive_window_size: u32,
    pub yamux_update_mode: UpdateMode,
    pub multiplex_option: MultiPlexOption,
    pub mplex_max_buffer_size: usize,
    pub no_delay: bool,
    pub port_reuse: bool,
    pub timeout: Duration,
    pub dns_resolver: Option<DnsResolver>,
    pub version: Option<UpgradeVersion>,
    pub enable_quic: bool,
    pub enable_websocket: bool,
    pub enable_secure_websocket: bool,
    pub support_quic_draft_29: bool,
    pub enable_webrtc: bool,
}

#[derive(Debug, Clone, Copy)]
pub enum MultiPlexOption {
    Yamux,
    YmuxAndMplex,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            yamux_max_buffer_size: 16 * 1024 * 1024,
            yamux_receive_window_size: 16 * 1024 * 1024,
            yamux_update_mode: UpdateMode::default(),
            multiplex_option: MultiPlexOption::Yamux,
            mplex_max_buffer_size: 1024,
            no_delay: true,
            port_reuse: true,
            enable_quic: false,
            enable_websocket: false,
            enable_secure_websocket: false,
            support_quic_draft_29: false,
            enable_webrtc: false,
            timeout: Duration::from_secs(30),
            dns_resolver: None,
            version: None,
        }
    }
}

#[derive(Default, Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DnsResolver {
    /// Google DNS Resolver
    Google,
    /// Cloudflare DNS Resolver
    #[default]
    Cloudflare,
    /// Local DNS Resolver
    Local,
    /// No DNS Resolver
    None,
}

impl From<DnsResolver> for (ResolverConfig, ResolverOpts) {
    fn from(value: DnsResolver) -> Self {
        match value {
            DnsResolver::Google => (ResolverConfig::google(), Default::default()),
            DnsResolver::Cloudflare => (ResolverConfig::cloudflare(), Default::default()),
            DnsResolver::Local => system_conf::read_system_conf().unwrap_or_default(),
            DnsResolver::None => (ResolverConfig::new(), Default::default()),
        }
    }
}

#[derive(Default, Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum UpdateMode {
    /// See [`WindowUpdateMode::on_receive`]
    #[default]
    Receive,

    /// See [`WindowUpdateMode::on_read`]
    Read,
}

impl From<UpdateMode> for WindowUpdateMode {
    fn from(mode: UpdateMode) -> Self {
        match mode {
            UpdateMode::Read => WindowUpdateMode::on_read(),
            UpdateMode::Receive => WindowUpdateMode::on_receive(),
        }
    }
}

#[derive(Default, Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum UpgradeVersion {
    /// See [`Version::V1`]
    #[default]
    Standard,

    /// See [`Version::V1Lazy`]
    Lazy,
}

impl From<UpgradeVersion> for Version {
    fn from(value: UpgradeVersion) -> Self {
        match value {
            UpgradeVersion::Standard => Version::V1,
            UpgradeVersion::Lazy => Version::V1Lazy,
        }
    }
}

/// Builds the transport that serves as a common ground for all connections.
///
/// Set up an encrypted TCP transport over the Yamux and Mplex protocol.
pub(crate) fn build_transport(
    keypair: identity::Keypair,
    relay: Option<ClientTransport>,
    TransportConfig {
        no_delay,
        port_reuse,
        timeout,
        multiplex_option,
        yamux_max_buffer_size,
        yamux_receive_window_size,
        yamux_update_mode,
        mplex_max_buffer_size,
        dns_resolver,
        version,
        enable_quic,
        support_quic_draft_29,
        ..
    }: TransportConfig,
) -> io::Result<TTransport> {
    let noise_config =
        noise::Config::new(&keypair).map_err(|e| io::Error::new(ErrorKind::Other, e))?;

    let yamux_config = {
        let mut config = YamuxConfig::default();
        config.set_max_buffer_size(yamux_max_buffer_size);
        config.set_receive_window_size(yamux_receive_window_size);
        config.set_window_update_mode(yamux_update_mode.into());
        config
    };

    let mplex_config = {
        let mut config = MplexConfig::default();
        config.set_max_buffer_size(mplex_max_buffer_size);
        config
    };

    let multiplex_upgrade = match multiplex_option {
        MultiPlexOption::Yamux => Either::Left(yamux_config),
        MultiPlexOption::YmuxAndMplex => {
            Either::Right(SelectUpgrade::new(yamux_config, mplex_config))
        }
    };

    //TODO: Cleanup
    let tcp_config = GenTcpConfig::default()
        .nodelay(no_delay)
        .port_reuse(port_reuse);

    let tcp_transport = TokioTcpTransport::new(tcp_config.clone());

    //TODO: Make togglable by flag in config
    let ws_transport = libp2p::websocket::WsConfig::new(TokioTcpTransport::new(tcp_config));

    let transport = tcp_transport.or_transport(ws_transport);

    let transport_timeout = TransportTimeout::new(transport, timeout);

    let (cfg, opts) = dns_resolver.unwrap_or_default().into();

    let transport = TokioDnsConfig::custom(transport_timeout, cfg, opts)?;

    let version = version.unwrap_or_default();

    let transport = match relay {
        Some(relay) => {
            let transport = OrTransport::new(relay, transport);

            transport
                .upgrade(version.into())
                .authenticate(noise_config)
                .multiplex(multiplex_upgrade)
                .timeout(timeout)
                .map(|(peer_id, muxer), _| (peer_id, StreamMuxerBox::new(muxer)))
                .boxed()
        }
        None => transport
            .upgrade(version.into())
            .authenticate(noise_config)
            .multiplex(multiplex_upgrade)
            .timeout(timeout)
            .map(|(peer_id, muxer), _| (peer_id, StreamMuxerBox::new(muxer)))
            .boxed(),
    };

    let transport = match enable_quic {
        true => {
            let mut quic_config = QuicConfig::new(&keypair);
            quic_config.support_draft_29 = support_quic_draft_29;

            let quic_transport = TokioQuicTransport::new(quic_config);

            OrTransport::new(quic_transport, transport)
                .map(|either_output, _| match either_output {
                    FutureEither::Left((peer_id, muxer)) => (peer_id, StreamMuxerBox::new(muxer)),
                    FutureEither::Right((peer_id, muxer)) => (peer_id, StreamMuxerBox::new(muxer)),
                })
                .boxed()
        }
        false => transport,
    };

    Ok(transport)
}

#[allow(dead_code)]
pub(crate) fn memory_transport(
    keypair: &identity::Keypair,
    relay: Option<ClientTransport>,
) -> io::Result<TTransport> {
    let noise_config =
        noise::Config::new(keypair).map_err(|e| io::Error::new(ErrorKind::Other, e))?;

    let transport = match relay {
        Some(relay) => OrTransport::new(relay, MemoryTransport::default())
            .upgrade(Version::V1)
            .authenticate(noise_config)
            .multiplex(YamuxConfig::default())
            .timeout(Duration::from_secs(20))
            .boxed(),
        None => MemoryTransport::default()
            .upgrade(Version::V1)
            .authenticate(noise_config)
            .multiplex(YamuxConfig::default())
            .timeout(Duration::from_secs(20))
            .boxed(),
    };

    Ok(transport)
}
