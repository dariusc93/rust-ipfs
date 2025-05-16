#[cfg(not(target_arch = "wasm32"))]
#[cfg(any(feature = "webrtc", feature = "websocket"))]
mod misc;

#[cfg(not(target_arch = "wasm32"))]
#[cfg(any(feature = "webrtc", feature = "websocket"))]
pub use misc::generate_cert;

#[allow(unused_imports)]
use either::Either;
#[allow(unused_imports)]
use futures::future::Either as FutureEither;
use libp2p::core::muxing::StreamMuxerBox;
use libp2p::core::transport::dummy::{DummyStream, DummyTransport};
#[allow(unused_imports)]
use libp2p::core::transport::timeout::TransportTimeout;
use libp2p::core::transport::upgrade::Version;
use libp2p::core::transport::Boxed;
#[cfg(not(target_arch = "wasm32"))]
#[cfg(feature = "dns")]
use libp2p::dns::{ResolverConfig, ResolverOpts};
use libp2p::identity;

#[cfg(not(target_arch = "wasm32"))]
#[cfg(feature = "pnet")]
use libp2p::pnet::{PnetConfig, PreSharedKey};
use libp2p::relay::client::Transport as ClientTransport;
use libp2p::PeerId;
use std::io;
use std::time::Duration;

use {
    libp2p::core::transport::{MemoryTransport, OrTransport},
    libp2p::Transport,
};

/// Transport type.
pub(crate) type TTransport = Boxed<(PeerId, StreamMuxerBox)>;

#[derive(Debug, Clone)]
pub struct TransportConfig {
    pub timeout: Duration,
    #[cfg(feature = "dns")]
    pub dns_resolver: Option<DnsResolver>,
    pub version: UpgradeVersion,
    #[cfg(feature = "quic")]
    pub enable_quic: bool,
    #[cfg(feature = "quic")]
    pub quic_max_idle_timeout: Duration,
    #[cfg(feature = "quic")]
    pub quic_keep_alive: Option<Duration>,
    #[cfg(feature = "websocket")]
    pub enable_websocket: bool,
    #[cfg(feature = "dns")]
    pub enable_dns: bool,
    pub enable_memory_transport: bool,
    #[cfg(feature = "webtransport")]
    pub enable_webtransport: bool,
    #[cfg(feature = "websocket")]
    pub websocket_pem: Option<(Vec<String>, String)>,
    #[cfg(feature = "websocket")]
    pub enable_secure_websocket: bool,
    #[cfg(feature = "quic")]
    pub support_quic_draft_29: bool,
    #[cfg(feature = "webrtc")]
    pub enable_webrtc: bool,
    #[cfg(feature = "webrtc")]
    pub webrtc_pem: Option<String>,
    #[cfg(not(target_arch = "wasm32"))]
    #[cfg(feature = "pnet")]
    pub enable_pnet: bool,
    #[cfg(not(target_arch = "wasm32"))]
    #[cfg(feature = "pnet")]
    pub pnet_psk: Option<PreSharedKey>,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            #[cfg(feature = "quic")]
            enable_quic: true,
            #[cfg(feature = "websocket")]
            enable_websocket: false,
            #[cfg(feature = "websocket")]
            websocket_pem: None,
            #[cfg(feature = "websocket")]
            enable_secure_websocket: true,
            enable_memory_transport: false,
            #[cfg(feature = "quic")]
            support_quic_draft_29: false,
            #[cfg(feature = "dns")]
            enable_dns: true,
            #[cfg(feature = "webtransport")]
            enable_webtransport: false,
            #[cfg(feature = "webrtc")]
            enable_webrtc: false,
            #[cfg(feature = "webrtc")]
            webrtc_pem: None,
            timeout: Duration::from_secs(10),
            //Note: This is set low due to quic transport not properly resetting connection state when reconnecting before connection timeout
            //      While in smaller settings this would be alright, we should be cautious of this setting for nodes with larger connections
            //      since this may increase cpu and network usage.
            //      see https://github.com/libp2p/rust-libp2p/issues/5097
            #[cfg(feature = "quic")]
            quic_max_idle_timeout: Duration::from_millis(300),
            #[cfg(feature = "quic")]
            quic_keep_alive: Some(Duration::from_millis(100)),
            #[cfg(feature = "dns")]
            dns_resolver: None,
            version: UpgradeVersion::default(),
            #[cfg(not(target_arch = "wasm32"))]
            #[cfg(feature = "pnet")]
            enable_pnet: false,
            #[cfg(not(target_arch = "wasm32"))]
            #[cfg(feature = "pnet")]
            pnet_psk: None,
        }
    }
}

#[cfg(feature = "dns")]
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

#[cfg(feature = "dns")]
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
        #[cfg(feature = "dns")]
        dns_resolver,
        version,
        #[cfg(feature = "quic")]
        enable_quic,
        enable_memory_transport,
        #[cfg(feature = "quic")]
        support_quic_draft_29,
        #[cfg(feature = "quic")]
        quic_max_idle_timeout,
        #[cfg(feature = "quic")]
        quic_keep_alive,
        #[cfg(feature = "dns")]
        enable_dns,
        #[cfg(feature = "websocket")]
        enable_websocket,
        #[cfg(feature = "websocket")]
        enable_secure_websocket,
        #[cfg(feature = "webrtc")]
        enable_webrtc,
        #[cfg(feature = "webrtc")]
        webrtc_pem,
        #[cfg(feature = "websocket")]
        websocket_pem,
        #[cfg(feature = "webtransport")]
            enable_webtransport: _,
        #[cfg(feature = "pnet")]
        enable_pnet,
        #[cfg(feature = "pnet")]
        pnet_psk,
    }: TransportConfig,
) -> io::Result<TTransport> {
    #[cfg(all(feature = "noise", feature = "tls"))]
    use crate::p2p::transport::dual_transport::SelectSecurityUpgrade;
    #[cfg(feature = "dns")]
    use libp2p::dns::tokio::Transport as TokioDnsConfig;
    #[cfg(feature = "noise")]
    use libp2p::noise;
    #[cfg(feature = "quic")]
    use libp2p::quic::{tokio::Transport as TokioQuicTransport, Config as QuicConfig};
    #[cfg(feature = "tcp")]
    use libp2p::tcp::{tokio::Transport as TokioTcpTransport, Config as GenTcpConfig};
    #[cfg(feature = "tls")]
    use libp2p::tls;

    let transport = match enable_memory_transport {
        true => Either::Left(MemoryTransport::new()),
        false => Either::Right(DummyTransport::<DummyStream>::new()),
    };

    #[cfg(feature = "dns")]
    let transport = match enable_dns {
        true => {
            let (cfg, opts) = dns_resolver.unwrap_or_default().into();
            let dns_transport = TokioDnsConfig::custom(transport, cfg, opts);
            Either::Left(dns_transport)
        }
        false => Either::Right(transport),
    };

    let transport = match relay {
        Some(relay) => Either::Left(OrTransport::new(relay, transport)),
        None => Either::Right(transport),
    };

    #[cfg(any(feature = "noise", feature = "tls"))]
    let transport = {
        let config = {
            #[cfg(all(feature = "noise", feature = "tls"))]
            {
                let noise_config = noise::Config::new(&keypair).map_err(io::Error::other)?;
                let tls_config = tls::Config::new(&keypair).map_err(io::Error::other)?;

                //TODO: Make configurable
                let config: SelectSecurityUpgrade<noise::Config, tls::Config> =
                    SelectSecurityUpgrade::new(noise_config, tls_config);
                config
            }
            #[cfg(all(feature = "noise", not(feature = "tls")))]
            {
                noise::Config::new(&keypair).map_err(io::Error::other)?
            }
            #[cfg(all(not(feature = "noise"), feature = "tls"))]
            {
                tls::Config::new(&keypair).map_err(io::Error::other)?
            }
        };

        let yamux_config = libp2p::yamux::Config::default();

        #[cfg(feature = "tcp")]
        let (tcp_config, transport) = {
            let tcp_config = GenTcpConfig::default().nodelay(true);
            let config = tcp_config.clone();
            let tcp_transport = TokioTcpTransport::new(tcp_config);
            (config, tcp_transport.or_transport(transport))
        };

        #[cfg(all(feature = "websocket", feature = "tcp"))]
        let transport = match enable_websocket {
            true => {
                let tcp_config = GenTcpConfig::default().nodelay(true);

                let mut ws_transport =
                    libp2p::websocket::WsConfig::new(TokioTcpTransport::new(tcp_config));
                if enable_secure_websocket {
                    let (certs, priv_key) = match websocket_pem {
                        Some((cert, kp)) => {
                            let mut certs = Vec::with_capacity(cert.len());
                            let kp = rcgen::KeyPair::from_pem(&kp).map_err(io::Error::other)?;
                            let priv_key =
                                libp2p::websocket::tls::PrivateKey::new(kp.serialize_der());
                            for cert in cert.iter().map(|c| c.as_bytes()) {
                                let pem = pem::parse(cert)
                                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                                let cert =
                                    libp2p::websocket::tls::Certificate::new(pem.into_contents());
                                certs.push(cert);
                            }

                            (certs, priv_key)
                        }
                        None => {
                            let (cert, prv, _) =
                                misc::generate_cert(&keypair, b"libp2p-websocket", false)?;

                            let priv_key =
                                libp2p::websocket::tls::PrivateKey::new(prv.serialize_der());
                            let self_cert =
                                libp2p::websocket::tls::Certificate::new(cert.der().to_vec());

                            (vec![self_cert], priv_key)
                        }
                    };

                    let tls_config = libp2p::websocket::tls::Config::new(priv_key, certs)
                        .map_err(io::Error::other)?;
                    ws_transport.set_tls_config(tls_config);
                }
                let transport = ws_transport.or_transport(transport);
                Either::Left(transport)
            }
            false => Either::Right(transport),
        };

        let transport = TransportTimeout::new(transport, timeout);

        #[cfg(feature = "pnet")]
        let transport = match (enable_pnet, pnet_psk) {
            (true, Some(psk)) => Either::Left(
                transport.and_then(move |socket, _| PnetConfig::new(psk).handshake(socket)),
            ),
            _ => Either::Right(transport),
        };

        transport
            .upgrade(version.into())
            .authenticate(config)
            .multiplex(yamux_config)
            .timeout(timeout)
            .boxed()
    };

    #[cfg(not(all(feature = "noise", feature = "tls")))]
    let transport = DummyTransport::<(PeerId, StreamMuxerBox)>::new().boxed();

    #[cfg(feature = "webrtc")]
    fn generate_webrtc_transport(
        keypair: &identity::Keypair,
        pem: &Option<String>,
    ) -> io::Result<libp2p_webrtc::tokio::Transport> {
        let cert = match pem {
            Some(pem) => {
                libp2p_webrtc::tokio::Certificate::from_pem(&pem).map_err(io::Error::other)?
            }
            None => {
                // This flag is internal, but is meant to allow generating an expired pem to satify webrtc
                let expired = true;
                let pem = misc::generate_wrtc_cert(&keypair)?;

                libp2p_webrtc::tokio::Certificate::from_pem(&pem).map_err(io::Error::other)?
            }
        };

        let kp = keypair.clone();
        let wrtc_tp = libp2p_webrtc::tokio::Transport::new(kp, cert);
        Ok(wrtc_tp)
    }

    #[cfg(feature = "webrtc")]
    let transport = match enable_webrtc {
        true => {
            let wrtc_tp = generate_webrtc_transport(&keypair, &webrtc_pem)?;

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

    #[cfg(feature = "quic")]
    fn build_quic_transport(
        keypair: &identity::Keypair,
        draft_29: bool,
        idle_timeout: Duration,
        keep_alive: Option<Duration>,
    ) -> TokioQuicTransport {
        let mut quic_config = QuicConfig::new(keypair);
        quic_config.support_draft_29 = draft_29;
        quic_config.max_idle_timeout = idle_timeout.as_millis() as _;
        quic_config.keep_alive_interval = keep_alive.unwrap_or(idle_timeout / 2);
        TokioQuicTransport::new(quic_config)
    }

    #[cfg(feature = "quic")]
    let transport = match enable_quic {
        true => {
            let quic_transport = build_quic_transport(
                &keypair,
                support_quic_draft_29,
                quic_max_idle_timeout,
                quic_keep_alive,
            );
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
        #[cfg(feature = "websocket")]
        enable_websocket,
        #[cfg(feature = "websocket")]
        enable_secure_websocket,
        #[cfg(feature = "webrtc")]
        enable_webrtc,
        #[cfg(feature = "webtransport")]
        enable_webtransport,
        enable_memory_transport,
        ..
    }: TransportConfig,
) -> io::Result<TTransport> {
    #[cfg(feature = "websocket")]
    use libp2p::websocket_websys;
    #[cfg(feature = "webtransport")]
    use libp2p::webtransport_websys;

    #[cfg(feature = "webrtc")]
    use libp2p_webrtc_websys as webrtc_websys;

    let transport = match enable_memory_transport {
        true => Either::Left(MemoryTransport::new()),
        false => Either::Right(DummyTransport::<DummyStream>::new()),
    };

    let transport = match relay {
        Some(relay) => Either::Left(OrTransport::new(relay, transport)),
        None => Either::Right(transport),
    };

    let noise_config = libp2p::noise::Config::new(&keypair).map_err(io::Error::other)?;
    let yamux_config = libp2p::yamux::Config::default();

    #[cfg(feature = "websocket")]
    let transport = match enable_websocket | enable_secure_websocket {
        true => {
            let ws_transport = websocket_websys::Transport::default();
            let transport = ws_transport.or_transport(transport);
            Either::Left(transport)
        }
        false => Either::Right(transport),
    };

    let transport = TransportTimeout::new(transport, timeout);

    let transport = transport
        .upgrade(version.into())
        .authenticate(noise_config)
        .multiplex(yamux_config)
        .timeout(timeout)
        .boxed();

    #[cfg(feature = "webtransport")]
    let transport = match enable_webtransport {
        true => {
            let config = webtransport_websys::Config::new(&keypair);
            let wtransport = webtransport_websys::Transport::new(config);
            wtransport
                .or_transport(transport)
                .map(|either_output, _| match either_output {
                    FutureEither::Left((peer_id, muxer)) => (peer_id, StreamMuxerBox::new(muxer)),
                    FutureEither::Right((peer_id, muxer)) => (peer_id, StreamMuxerBox::new(muxer)),
                })
                .boxed()
        }
        false => transport.boxed(),
    };

    #[cfg(feature = "webrtc")]
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

    Ok(transport.boxed())
}

// borrow from libp2p SwarmBuilder
#[cfg(not(target_arch = "wasm32"))]
#[cfg(all(feature = "noise", feature = "tls"))]
mod dual_transport {
    use either::Either;
    use futures::{
        future::{self, MapOk},
        TryFutureExt,
    };
    use libp2p::{
        core::{
            either::EitherFuture,
            upgrade::{InboundConnectionUpgrade, OutboundConnectionUpgrade},
            UpgradeInfo,
        },
        PeerId,
    };
    use std::iter::{Chain, Map};

    #[derive(Debug, Clone)]
    pub struct SelectSecurityUpgrade<A, B>(A, B);

    impl<A, B> SelectSecurityUpgrade<A, B> {
        /// Combines two upgrades into an `SelectUpgrade`.
        ///
        /// The protocols supported by the first element have a higher priority.
        pub fn new(a: A, b: B) -> Self {
            SelectSecurityUpgrade(a, b)
        }
    }

    impl<A, B> UpgradeInfo for SelectSecurityUpgrade<A, B>
    where
        A: UpgradeInfo,
        B: UpgradeInfo,
    {
        type Info = Either<A::Info, B::Info>;
        type InfoIter = Chain<
            Map<<A::InfoIter as IntoIterator>::IntoIter, fn(A::Info) -> Self::Info>,
            Map<<B::InfoIter as IntoIterator>::IntoIter, fn(B::Info) -> Self::Info>,
        >;

        fn protocol_info(&self) -> Self::InfoIter {
            let a = self
                .0
                .protocol_info()
                .into_iter()
                .map(Either::Left as fn(A::Info) -> _);
            let b = self
                .1
                .protocol_info()
                .into_iter()
                .map(Either::Right as fn(B::Info) -> _);

            a.chain(b)
        }
    }

    impl<C, A, B, TA, TB, EA, EB> InboundConnectionUpgrade<C> for SelectSecurityUpgrade<A, B>
    where
        A: InboundConnectionUpgrade<C, Output = (PeerId, TA), Error = EA>,
        B: InboundConnectionUpgrade<C, Output = (PeerId, TB), Error = EB>,
    {
        type Output = (PeerId, future::Either<TA, TB>);
        type Error = Either<EA, EB>;
        type Future = MapOk<
            EitherFuture<A::Future, B::Future>,
            fn(future::Either<(PeerId, TA), (PeerId, TB)>) -> (PeerId, future::Either<TA, TB>),
        >;

        fn upgrade_inbound(self, sock: C, info: Self::Info) -> Self::Future {
            match info {
                Either::Left(info) => EitherFuture::First(self.0.upgrade_inbound(sock, info)),
                Either::Right(info) => EitherFuture::Second(self.1.upgrade_inbound(sock, info)),
            }
            .map_ok(future::Either::factor_first)
        }
    }

    impl<C, A, B, TA, TB, EA, EB> OutboundConnectionUpgrade<C> for SelectSecurityUpgrade<A, B>
    where
        A: OutboundConnectionUpgrade<C, Output = (PeerId, TA), Error = EA>,
        B: OutboundConnectionUpgrade<C, Output = (PeerId, TB), Error = EB>,
    {
        type Output = (PeerId, future::Either<TA, TB>);
        type Error = Either<EA, EB>;
        type Future = MapOk<
            EitherFuture<A::Future, B::Future>,
            fn(future::Either<(PeerId, TA), (PeerId, TB)>) -> (PeerId, future::Either<TA, TB>),
        >;

        fn upgrade_outbound(self, sock: C, info: Self::Info) -> Self::Future {
            match info {
                Either::Left(info) => EitherFuture::First(self.0.upgrade_outbound(sock, info)),
                Either::Right(info) => EitherFuture::Second(self.1.upgrade_outbound(sock, info)),
            }
            .map_ok(future::Either::factor_first)
        }
    }
}
