use libp2p::core::muxing::StreamMuxerBox;
use libp2p::core::transport::timeout::TransportTimeout;
use libp2p::core::transport::upgrade::Version;
use libp2p::core::transport::{Boxed, OrTransport};
use libp2p::core::upgrade::SelectUpgrade;
use libp2p::dns::TokioDnsConfig;
use libp2p::identity;
use libp2p::mplex::MplexConfig;
use libp2p::noise::{self, NoiseConfig};
use libp2p::relay::v2::client::transport::ClientTransport;
use libp2p::tcp::{GenTcpConfig, TokioTcpTransport};
use libp2p::yamux::{YamuxConfig, WindowUpdateMode};
use libp2p::{PeerId, Transport};
use std::io::{self, Error, ErrorKind};
use std::time::Duration;

/// Transport type.
pub(crate) type TTransport = Boxed<(PeerId, StreamMuxerBox)>;

/// Builds the transport that serves as a common ground for all connections.
///
/// Set up an encrypted TCP transport over the Yamux and Mplex protocol.
pub fn build_transport(keypair: identity::Keypair, relay: Option<ClientTransport>) -> io::Result<TTransport> {
    let xx_keypair = noise::Keypair::<noise::X25519Spec>::new()
        .into_authentic(&keypair)
        .unwrap();
    let noise_config = NoiseConfig::xx(xx_keypair).into_authenticated();

    let yamux_config = {
        let mut config = YamuxConfig::default();
        config.set_max_buffer_size(16 * 1024 * 1024);
        config.set_receive_window_size(16 * 1024 * 1024); 
        config.set_window_update_mode(WindowUpdateMode::on_receive());
        config
    };

    let multiplex_upgrade = SelectUpgrade::new(yamux_config, MplexConfig::new());

    //TODO: Cleanup 
    let tcp_config = GenTcpConfig::default().nodelay(true).port_reuse(true);

    let tcp_transport = TokioTcpTransport::new(tcp_config.clone());
    let ws_transport = libp2p::websocket::WsConfig::new(TokioTcpTransport::new(
        tcp_config
    ));

    let transport = tcp_transport.or_transport(ws_transport);
    
    let transport_timeout = TransportTimeout::new(transport, Duration::from_secs(30));
    #[cfg(not(target_os="android"))]
    let transport = TokioDnsConfig::system(transport_timeout)?;
    #[cfg(target_os="android")]
    let transport = TokioDnsConfig::custom(transport_timeout, Default::default(), Default::default())?;

    let transport = match relay {
        Some(relay) => {
            let transport = OrTransport::new(relay, transport);

            transport
            .upgrade(Version::V1)
            .authenticate(noise_config)
            .multiplex(multiplex_upgrade)
            .timeout(Duration::from_secs(20))
            .map(|(peer_id, muxer), _| (peer_id, StreamMuxerBox::new(muxer)))
            .map_err(|err| Error::new(ErrorKind::Other, err))
            .boxed()
        }
        None => {
            transport.upgrade(Version::V1)
            .authenticate(noise_config)
            .multiplex(multiplex_upgrade)
            .timeout(Duration::from_secs(20))
            .map(|(peer_id, muxer), _| (peer_id, StreamMuxerBox::new(muxer)))
            .map_err(|err| Error::new(ErrorKind::Other, err))
            .boxed()
        }
    };

    Ok(transport)
}