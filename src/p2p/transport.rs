use futures::future::Either;
use libp2p::core::muxing::StreamMuxerBox;
use libp2p::core::transport::timeout::TransportTimeout;
use libp2p::core::transport::upgrade::Version;
use libp2p::core::transport::{Boxed, OrTransport};
use libp2p::core::upgrade::SelectUpgrade;
use libp2p::dns::{ResolverConfig, ResolverOpts, TokioDnsConfig};
use libp2p::identity;
use libp2p::mplex::MplexConfig;
use libp2p::noise::{self, NoiseConfig};
use libp2p::quic::tokio::Transport as TokioQuicTransport;
use libp2p::quic::Config as QuicConfig;
use libp2p::relay::client::Transport as ClientTransport;
use libp2p::tcp::{tokio::Transport as TokioTcpTransport, Config as GenTcpConfig};
use libp2p::yamux::{WindowUpdateMode, YamuxConfig};
use libp2p::{PeerId, Transport};
use std::io;
use std::time::Duration;
use trust_dns_resolver::system_conf;

/// Transport type.
pub(crate) type TTransport = Boxed<(PeerId, StreamMuxerBox)>;

#[derive(Debug, Clone, Copy)]
pub struct TransportConfig {
    pub yamux_max_buffer_size: usize,
    pub yamux_receive_window_size: u32,
    pub yamux_update_mode: u8,
    pub mplex_max_buffer_size: usize,
    pub no_delay: bool,
    pub port_reuse: bool,
    pub timeout: Duration,
    pub dns_resolver: Option<DnsResolver>,
    pub version: Option<UpgradeVersion>,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            yamux_max_buffer_size: 8 * 1024 * 1024,
            yamux_receive_window_size: 8 * 1024 * 1024,
            yamux_update_mode: 0,
            mplex_max_buffer_size: 1024,
            no_delay: true,
            port_reuse: true,
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
pub fn build_transport(
    keypair: identity::Keypair,
    relay: Option<ClientTransport>,
    TransportConfig {
        no_delay,
        port_reuse,
        timeout,
        yamux_max_buffer_size,
        yamux_receive_window_size,
        yamux_update_mode,
        mplex_max_buffer_size,
        dns_resolver,
        version,
    }: TransportConfig,
) -> io::Result<TTransport> {
    let xx_keypair = noise::Keypair::<noise::X25519Spec>::new()
        .into_authentic(&keypair)
        .unwrap();
    let noise_config = NoiseConfig::xx(xx_keypair).into_authenticated();

    let multiplex_upgrade = SelectUpgrade::new(
        {
            let mut config = YamuxConfig::default();
            config.set_max_buffer_size(yamux_max_buffer_size);
            config.set_receive_window_size(yamux_receive_window_size);
            config.set_window_update_mode(if yamux_update_mode == 0 {
                WindowUpdateMode::on_receive()
            } else {
                WindowUpdateMode::on_read()
            });
            config
        },
        {
            let mut config = MplexConfig::default();
            config.set_max_buffer_size(mplex_max_buffer_size);
            config
        },
    );

    //TODO: Cleanup
    let tcp_config = GenTcpConfig::default()
        .nodelay(no_delay)
        .port_reuse(port_reuse);

    let quic_config = QuicConfig::new(&keypair);
    let quic_transport = TokioQuicTransport::new(quic_config);

    let tcp_transport = TokioTcpTransport::new(tcp_config.clone());
    let ws_transport = libp2p::websocket::WsConfig::new(TokioTcpTransport::new(tcp_config));

    let transport = tcp_transport.or_transport(ws_transport);

    let transport_timeout = TransportTimeout::new(transport, Duration::from_secs(30));

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

    let transport = OrTransport::new(quic_transport, transport)
        .map(|either_output, _| match either_output {
            Either::Left((peer_id, muxer)) => (peer_id, StreamMuxerBox::new(muxer)),
            Either::Right((peer_id, muxer)) => (peer_id, StreamMuxerBox::new(muxer)),
        })
        .boxed();

    Ok(transport)
}
