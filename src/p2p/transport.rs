#[cfg(not(target_arch = "wasm32"))]
mod misc;

#[allow(unused_imports)]
use either::Either;
#[allow(unused_imports)]
use futures::future::Either as FutureEither;
use libp2p::core::muxing::StreamMuxerBox;
#[allow(unused_imports)]
use libp2p::core::transport::timeout::TransportTimeout;
use libp2p::core::transport::upgrade::Version;
use libp2p::core::transport::{Boxed, MemoryTransport, OrTransport};
#[cfg(not(target_arch = "wasm32"))]
use libp2p::dns::{ResolverConfig, ResolverOpts};
use libp2p::relay::client::Transport as ClientTransport;
use libp2p::yamux::Config as YamuxConfig;
use libp2p::{identity, noise};
use libp2p::{PeerId, Transport};
use std::io;
use std::time::Duration;

/// Transport type.
pub(crate) type TTransport = Boxed<(PeerId, StreamMuxerBox)>;

#[derive(Debug, Clone)]
pub struct TransportConfig {
    pub timeout: Duration,
    pub dns_resolver: Option<DnsResolver>,
    pub version: UpgradeVersion,
    pub enable_quic: bool,
    pub quic_max_idle_timeout: Duration,
    pub quic_keep_alive: Option<Duration>,
    pub enable_websocket: bool,
    pub enable_dns: bool,
    pub enable_memory_transport: bool,
    pub websocket_pem: Option<(String, String)>,
    pub enable_secure_websocket: bool,
    pub support_quic_draft_29: bool,
    pub enable_webrtc: bool,
    pub webrtc_pem: Option<String>,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            enable_quic: true,
            enable_websocket: false,
            websocket_pem: None,
            enable_secure_websocket: true,
            enable_memory_transport: false,
            support_quic_draft_29: false,
            enable_dns: true,
            enable_webrtc: false,
            webrtc_pem: None,
            timeout: Duration::from_secs(10),
            //Note: This is set low due to quic transport not properly resetting connection state when reconnecting before connection timeout
            //      While in smaller settings this would be alright, we should be cautious of this setting for nodes with larger connections
            //      since this may increase cpu and network usage.
            //      see https://github.com/libp2p/rust-libp2p/issues/5097
            quic_max_idle_timeout: Duration::from_millis(300),
            quic_keep_alive: Some(Duration::from_millis(100)),
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
            DnsResolver::Local => {
                hickory_resolver::system_conf::read_system_conf().unwrap_or_default()
            }
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
        enable_memory_transport,
        support_quic_draft_29,
        quic_max_idle_timeout,
        quic_keep_alive,
        enable_dns,
        enable_websocket,
        enable_secure_websocket,
        enable_webrtc,
        webrtc_pem,
        websocket_pem,
    }: TransportConfig,
) -> io::Result<TTransport> {
    use libp2p::dns::tokio::Transport as TokioDnsConfig;
    use libp2p::quic::tokio::Transport as TokioQuicTransport;
    use libp2p::quic::Config as QuicConfig;
    use libp2p::tcp::{tokio::Transport as TokioTcpTransport, Config as GenTcpConfig};
    use rcgen::KeyPair;

    let noise_config = noise::Config::new(&keypair).map_err(io::Error::other)?;

    let yamux_config = YamuxConfig::default();

    let tcp_config = GenTcpConfig::default().nodelay(true).port_reuse(true);

    let transport = TokioTcpTransport::new(tcp_config.clone());

    let transport = match enable_memory_transport {
        true => {
            let mem_ts = MemoryTransport::new();
            Either::Left(mem_ts.or_transport(transport))
        }
        false => Either::Right(transport),
    };

    let transport = match enable_websocket {
        true => {
            let mut ws_transport =
                libp2p::websocket::WsConfig::new(TokioTcpTransport::new(tcp_config));
            if enable_secure_websocket {
                let (cert, priv_key) = match websocket_pem {
                    Some((cert, kp)) => {
                        let kp = KeyPair::from_pem(&kp).map_err(io::Error::other)?;
                        let priv_key = libp2p::websocket::tls::PrivateKey::new(kp.serialize_der());
                        let cert = libp2p::websocket::tls::Certificate::new(cert.into_bytes());
                        (cert, priv_key)
                    }
                    None => {
                        let (cert, prv, _) = misc::generate_deterministic_cert(
                            &keypair,
                            b"libp2p-websocket",
                            false,
                        )?;

                        let priv_key = libp2p::websocket::tls::PrivateKey::new(prv.serialize_der());
                        let self_cert =
                            libp2p::websocket::tls::Certificate::new(cert.der().to_vec());

                        (self_cert, priv_key)
                    }
                };

                let tls_config = libp2p::websocket::tls::Config::new(priv_key, [cert])
                    .map_err(io::Error::other)?;
                ws_transport.set_tls_config(tls_config);
            }
            let transport = ws_transport.or_transport(transport);
            Either::Left(transport)
        }
        false => Either::Right(transport),
    };

    let transport_timeout = TransportTimeout::new(transport, timeout);

    let transport = match enable_dns {
        true => {
            let (cfg, opts) = dns_resolver.unwrap_or_default().into();
            let dns_transport = TokioDnsConfig::custom(transport_timeout, cfg, opts);
            Either::Left(dns_transport)
        }
        false => Either::Right(transport_timeout),
    };

    let transport = match relay {
        Some(relay) => Either::Left(OrTransport::new(relay, transport)),
        None => Either::Right(transport),
    };

    let transport = transport
        .upgrade(version.into())
        .authenticate(noise_config)
        .multiplex(yamux_config)
        .timeout(timeout)
        .boxed();

    #[cfg(feature = "webrtc_transport")]
    let transport = match enable_webrtc {
        true => {
            let cert = match webrtc_pem {
                Some(pem) => libp2p_webrtc::tokio::Certificate::from_pem(&pem)
                    .map_err(std::io::Error::other)?,
                None => {
                    // This flag is internal, but is meant to allow generating an expired pem to satify webrtc
                    let expired = true;
                    let (cert, prv, expired_pem) =
                        misc::generate_deterministic_cert(&keypair, b"libp2p-webrtc", expired)?;
                    // dtls requires pem with a dash in the label?
                    let priv_key = prv.serialize_pem().replace("PRIVATE KEY", "PRIVATE_KEY");
                    let cert = cert.pem();

                    let pem = priv_key + "\n\n" + &cert;

                    let pem = match expired_pem {
                        Some(epem) => epem + "\n\n" + &pem,
                        None => pem,
                    };

                    libp2p_webrtc::tokio::Certificate::from_pem(&pem)
                        .map_err(std::io::Error::other)?
                }
            };

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
            quic_config.keep_alive_interval = quic_keep_alive.unwrap_or(quic_max_idle_timeout / 2);
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
    TransportConfig {
        timeout,
        version,
        enable_websocket,
        enable_secure_websocket,
        enable_webrtc,
        ..
    }: TransportConfig,
) -> io::Result<TTransport> {
    use libp2p::websocket_websys;
    use libp2p_webrtc_websys as webrtc_websys;

    let noise_config = noise::Config::new(&keypair).map_err(io::Error::other)?;
    let yamux_config = YamuxConfig::default();

    let transport = MemoryTransport::default();

    let transport = match enable_websocket | enable_secure_websocket {
        true => {
            let ws_transport = websocket_websys::Transport::default();
            let transport = ws_transport.or_transport(transport);
            Either::Left(transport)
        }
        false => Either::Right(transport),
    };

    let transport = TransportTimeout::new(transport, timeout);

    let transport = match relay {
        Some(relay) => Either::Left(OrTransport::new(relay, transport)),
        None => Either::Right(transport),
    };

    let transport = transport
        .upgrade(version.into())
        .authenticate(noise_config)
        .multiplex(yamux_config)
        .timeout(timeout)
        .boxed();

    let transport = match enable_webrtc {
        true => {
            let wrtc_transport =
                webrtc_websys::Transport::new(webrtc_websys::Config::new(&keypair));
            wrtc_transport
                .or_transport(transport)
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
