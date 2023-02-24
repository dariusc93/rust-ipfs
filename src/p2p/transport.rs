use libp2p::core::muxing::StreamMuxerBox;
use libp2p::core::transport::timeout::TransportTimeout;
use libp2p::core::transport::upgrade::Version;
use libp2p::core::transport::{Boxed, OrTransport};
use libp2p::core::upgrade::SelectUpgrade;
use libp2p::dns::{ResolverConfig, TokioDnsConfig};
use libp2p::identity;
use libp2p::mplex::MplexConfig;
use libp2p::noise::{self, NoiseConfig};
use libp2p::quic::tokio::Transport as TokioQuicTransport;
use libp2p::quic::Config as QuicConfig;
use libp2p::relay::v2::client::transport::ClientTransport;
use libp2p::swarm::derive_prelude::EitherOutput;
use libp2p::tcp::{tokio::Transport as TokioTcpTransport, Config as GenTcpConfig};
use libp2p::yamux::{YamuxConfig, WindowUpdateMode};
use libp2p::{PeerId, Transport};
use std::io::{self, Error, ErrorKind};
use std::time::Duration;

/// Transport type.
pub(crate) type TTransport = Boxed<(PeerId, StreamMuxerBox)>;

#[derive(Debug, Clone)]
pub struct TransportConfig {
    pub yamux_config: YamuxConfig,
    pub mplex_config: MplexConfig,
    pub no_delay: bool,
    pub port_reuse: bool,
    pub timeout: Duration,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            yamux_config: {
                let mut config = YamuxConfig::default();
                config.set_max_buffer_size(16 * 1024 * 1024);
                config.set_receive_window_size(16 * 1024 * 1024);
                config.set_window_update_mode(WindowUpdateMode::on_receive());
                config
            },
            mplex_config: {
                let mut config = MplexConfig::default();
                config.set_max_buffer_size(usize::MAX);
                config
            },
            no_delay: true,
            port_reuse: true,
            timeout: Duration::from_secs(30),
        }
    }
}

/// Builds the transport that serves as a common ground for all connections.
///
/// Set up an encrypted TCP transport over the Yamux and Mplex protocol.
pub fn build_transport(
    keypair: identity::Keypair,
    relay: Option<ClientTransport>,
    config: TransportConfig,
) -> io::Result<TTransport> {
    let TransportConfig {
        yamux_config,
        mplex_config,
        no_delay,
        port_reuse,
        timeout,
    } = config;
    let xx_keypair = noise::Keypair::<noise::X25519Spec>::new()
        .into_authentic(&keypair)
        .unwrap();
    let noise_config = NoiseConfig::xx(xx_keypair).into_authenticated();

    let multiplex_upgrade = SelectUpgrade::new(yamux_config, mplex_config);

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

    //TODO: Make this configurable to use, google, cloudflare or a custom resolver
    let transport = TokioDnsConfig::custom(
        transport_timeout,
        ResolverConfig::cloudflare(),
        Default::default(),
    )?;

    let transport = match relay {
        Some(relay) => {
            let transport = OrTransport::new(relay, transport);

            transport
                .upgrade(Version::V1)
                .authenticate(noise_config)
                .multiplex(multiplex_upgrade)
                .timeout(timeout)
                .map(|(peer_id, muxer), _| (peer_id, StreamMuxerBox::new(muxer)))
                .map_err(|err| Error::new(ErrorKind::Other, err))
                .boxed()
        }
        None => transport
            .upgrade(Version::V1)
            .authenticate(noise_config)
            .multiplex(multiplex_upgrade)
            .timeout(timeout)
            .map(|(peer_id, muxer), _| (peer_id, StreamMuxerBox::new(muxer)))
            .map_err(|err| Error::new(ErrorKind::Other, err))
            .boxed(),
    };

    let transport = OrTransport::new(quic_transport, transport)
        .map(|either_output, _| match either_output {
            EitherOutput::First((peer_id, muxer)) => (peer_id, StreamMuxerBox::new(muxer)),
            EitherOutput::Second((peer_id, muxer)) => (peer_id, StreamMuxerBox::new(muxer)),
        })
        .boxed();

    Ok(transport)
}
