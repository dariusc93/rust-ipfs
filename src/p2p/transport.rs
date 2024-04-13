use either::Either;
use futures::future::Either as FutureEither;
use hickory_resolver::system_conf;
use libp2p::core::muxing::StreamMuxerBox;
use libp2p::core::transport::timeout::TransportTimeout;
use libp2p::core::transport::upgrade::Version;
use libp2p::core::transport::{Boxed, MemoryTransport, OrTransport};
#[cfg(not(target_arch = "wasm32"))]
use libp2p::dns::{ResolverConfig, ResolverOpts};
use libp2p::relay::client::Transport as ClientTransport;
use libp2p::yamux::Config as YamuxConfig;
use libp2p::{identity, noise};
use libp2p::{PeerId, Transport};
use std::io::{self, ErrorKind};
use std::time::Duration;

/// Transport type.
pub(crate) type TTransport = Boxed<(PeerId, StreamMuxerBox)>;

#[derive(Debug, Clone, Copy)]
pub struct TransportConfig {
    pub timeout: Duration,
    pub dns_resolver: Option<DnsResolver>,
    pub version: UpgradeVersion,
    pub enable_quic: bool,
    pub quic_max_idle_timeout: Duration,
    pub enable_websocket: bool,
    pub enable_secure_websocket: bool,
    pub support_quic_draft_29: bool,
    pub enable_webrtc: bool,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            enable_quic: true,
            enable_websocket: false,
            enable_secure_websocket: true,
            support_quic_draft_29: false,
            enable_webrtc: false,
            timeout: Duration::from_secs(30),
            quic_max_idle_timeout: Duration::from_secs(10),
            dns_resolver: None,
            version: UpgradeVersion::default(),
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

#[cfg(not(target_arch = "wasm32"))]
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
    Standard,

    /// See [`Version::V1Lazy`]
    #[default]
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
#[cfg(not(target_arch = "wasm32"))]
#[allow(unused_variables)]
pub(crate) fn build_transport(
    keypair: identity::Keypair,
    relay: Option<ClientTransport>,
    TransportConfig {
        timeout,
        dns_resolver,
        version,
        enable_quic,
        support_quic_draft_29,
        quic_max_idle_timeout,
        enable_websocket,
        enable_secure_websocket,
        enable_webrtc,
    }: TransportConfig,
) -> io::Result<TTransport> {
    use libp2p::dns::tokio::Transport as TokioDnsConfig;
    use libp2p::quic::tokio::Transport as TokioQuicTransport;
    use libp2p::quic::Config as QuicConfig;
    use libp2p::tcp::{tokio::Transport as TokioTcpTransport, Config as GenTcpConfig};

    let noise_config =
        noise::Config::new(&keypair).map_err(|e| io::Error::new(ErrorKind::Other, e))?;

    let yamux_config = YamuxConfig::default();

    let tcp_config = GenTcpConfig::default().nodelay(true).port_reuse(true);

    let transport = TokioTcpTransport::new(tcp_config.clone());

    let transport = match enable_websocket {
        true => {
            let mut ws_transport =
                libp2p::websocket::WsConfig::new(TokioTcpTransport::new(tcp_config));
            if enable_secure_websocket {
                let rcgen::CertifiedKey {
                    cert: self_cert,
                    key_pair,
                } = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
                    .map_err(io::Error::other)?;

                let priv_key = libp2p::websocket::tls::PrivateKey::new(key_pair.serialize_der());
                let self_cert = libp2p::websocket::tls::Certificate::new(self_cert.der().to_vec());
                let tls_config = libp2p::websocket::tls::Config::new(priv_key, [self_cert])
                    .map_err(io::Error::other)?;
                ws_transport.set_tls_config(tls_config);
            }
            let transport = ws_transport.or_transport(transport);
            Either::Left(transport)
        }
        false => Either::Right(transport),
    };

    let transport_timeout = TransportTimeout::new(transport, timeout);

    let (cfg, opts) = dns_resolver.unwrap_or_default().into();

    let transport = TokioDnsConfig::custom(transport_timeout, cfg, opts);

    let transport = match relay {
        Some(relay) => {
            let transport = OrTransport::new(relay, transport);
            transport
                .upgrade(version.into())
                .authenticate(noise_config)
                .multiplex(yamux_config)
                .timeout(timeout)
                .boxed()
        }
        None => transport
            .upgrade(version.into())
            .authenticate(noise_config)
            .multiplex(yamux_config)
            .timeout(timeout)
            .boxed(),
    };

    #[cfg(feature = "webrtc_transport")]
    let transport = match enable_webrtc {
        true => {
            let cert = libp2p_webrtc::tokio::Certificate::generate(&mut rand::thread_rng())
                .map_err(std::io::Error::other)?;
            let kp = keypair.clone();
            let wrtc_tp = libp2p_webrtc::tokio::Transport::new(kp, cert);
            wrtc_tp
                .or_transport(transport)
                .map(|either_output, _| match either_output {
                    FutureEither::Left((peer_id, muxer)) => (peer_id, StreamMuxerBox::new(muxer)),
                    FutureEither::Right((peer_id, muxer)) => (peer_id, StreamMuxerBox::new(muxer)),
                })
                .boxed()
        }
        false => transport.boxed(),
    };

    let transport = match enable_quic {
        true => {
            let mut quic_config = QuicConfig::new(&keypair);
            quic_config.support_draft_29 = support_quic_draft_29;
            quic_config.max_idle_timeout = quic_max_idle_timeout.as_millis() as _;
            quic_config.keep_alive_interval = quic_max_idle_timeout / 2;
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

#[cfg(target_arch = "wasm32")]
pub(crate) fn build_transport(
    keypair: identity::Keypair,
    relay: Option<ClientTransport>,
    TransportConfig { .. }: TransportConfig,
) -> io::Result<TTransport> {
    memory_transport(&keypair, relay)
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
