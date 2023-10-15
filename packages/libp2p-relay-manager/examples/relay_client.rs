use std::{io, time::Duration};

use clap::Parser;
use futures::{future::Either, FutureExt, StreamExt};
use libp2p::{
    core::{
        muxing::StreamMuxerBox,
        transport::{timeout::TransportTimeout, Boxed, OrTransport},
        upgrade::Version,
    },
    dns::{DnsConfig, ResolverConfig},
    identify::{self, Behaviour as Identify},
    identity::{self, Keypair},
    multiaddr::Protocol,
    noise,
    ping::{self, Behaviour as Ping},
    relay::client::{Behaviour as RelayClient, Transport as ClientTransport},
    swarm::{NetworkBehaviour, SwarmBuilder, SwarmEvent},
    tcp::{async_io::Transport as AsyncTcpTransport, Config as GenTcpConfig},
    Multiaddr, PeerId, Transport,
};

use libp2p::quic::{async_std::Transport as AsyncQuicTransport, Config as QuicConfig};

#[derive(NetworkBehaviour)]
pub struct Behaviour {
    relay_client: RelayClient,
    relay_manager: libp2p_relay_manager::Behaviour,
    identify: Identify,
    ping: Ping,
}

#[derive(Debug, Parser)]
#[clap(name = "relay client")]
struct Opts {
    /// Fixed value to generate deterministic peer id.
    #[clap(long)]
    secret_key_seed: Option<u8>,

    /// List of relay addresses
    #[clap(long)]
    relay_addrs: Vec<Multiaddr>,

    /// Peer id of a specific relay. If none is provided, it will select at random
    #[clap(long)]
    select_relay: Vec<PeerId>,

    /// Listen on local addresses
    #[clap(long)]
    listener: bool,

    /// Attempts to establish connection to a specific address after 10 seconds
    #[clap(long)]
    connect: Option<Multiaddr>,
}

#[async_std::main]
#[allow(deprecated)]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let opts = Opts::parse();

    let local_keypair = match opts.secret_key_seed {
        Some(seed) => generate_ed25519(seed),
        None => Keypair::generate_ed25519(),
    };

    let local_peer_id = PeerId::from(local_keypair.public());

    println!("Local Node: {local_peer_id}");

    let (relay_transport, relay_client) = libp2p::relay::client::new(local_peer_id);

    let transport = build_transport(local_keypair.clone(), relay_transport).await?;

    let behaviour = Behaviour {
        ping: Ping::new(Default::default()),
        identify: Identify::new({
            let mut config =
                identify::Config::new("/test/0.1.0".to_string(), local_keypair.public());
            config.push_listen_addr_updates = true;
            config
        }),
        relay_client,
        relay_manager: libp2p_relay_manager::Behaviour::default(),
    };

    let mut swarm =
        SwarmBuilder::with_async_std_executor(transport, behaviour, local_peer_id).build();

    if opts.listener {
        let addr = "/ip4/0.0.0.0/tcp/0".parse().unwrap();
        swarm.listen_on(addr)?;
    }

    for mut node in opts.relay_addrs {
        if !node.iter().any(|proto| matches!(proto, Protocol::P2p(_))) {
            println!("{node} requires a peer id");
            continue;
        }

        if node
            .iter()
            .any(|proto| matches!(proto, Protocol::P2pCircuit))
        {
            println!("{node} should not contain a circuit");
            continue;
        }

        let peer_id = match node.pop() {
            Some(Protocol::P2p(peer_id)) => peer_id,
            _ => {
                continue;
            }
        };

        swarm
            .behaviour_mut()
            .relay_manager
            .add_address(peer_id, node);
    }

    if !opts.select_relay.is_empty() {
        for relay_peer_id in opts.select_relay {
            swarm.behaviour_mut().relay_manager.select(relay_peer_id);
        }
    } else {
        swarm.behaviour_mut().relay_manager.random_select();
    }

    let mut timer = futures_timer::Delay::new(Duration::from_secs(10));
    let mut connect_addr = opts.connect.clone();

    futures::future::poll_fn(move |cx| {
        if timer.poll_unpin(cx).is_ready() {
            if let Some(addr) = connect_addr.take() {
                if let Err(e) = swarm.dial(addr.clone()) {
                    println!("Error dialing {addr}: {e}");
                }
            }
        }

        loop {
            match swarm.poll_next_unpin(cx) {
                std::task::Poll::Ready(Some(event)) => match event {
                    SwarmEvent::NewListenAddr { address, .. } => {
                        println!("Listening on {address}");
                    }
                    SwarmEvent::Behaviour(event) => {
                        println!("{event:?}");
                        match event {
                            BehaviourEvent::RelayClient(event) => {
                                swarm
                                    .behaviour_mut()
                                    .relay_manager
                                    .process_relay_event(event);
                            }
                            BehaviourEvent::Ping(ping::Event {
                                peer,
                                connection,
                                result: Result::Ok(rtt),
                            }) => {
                                swarm
                                    .behaviour_mut()
                                    .relay_manager
                                    .set_peer_rtt(peer, connection, rtt);
                            }
                            _ => {}
                        }
                    }
                    _e => println!("{_e:?}"),
                },
                std::task::Poll::Ready(None) => return std::task::Poll::Ready(Option::<()>::None),
                std::task::Poll::Pending => return std::task::Poll::Pending,
            }
        }
    })
    .await;

    Ok(())
}

pub async fn build_transport(
    keypair: Keypair,
    relay: ClientTransport,
) -> io::Result<Boxed<(PeerId, StreamMuxerBox)>> {
    let noise_config = noise::Config::new(&keypair).unwrap();

    let multiplex_upgrade = libp2p::yamux::Config::default();

    let quic_transport = AsyncQuicTransport::new(QuicConfig::new(&keypair));

    let transport = AsyncTcpTransport::new(GenTcpConfig::default().nodelay(true).port_reuse(true));

    let transport_timeout = TransportTimeout::new(transport, Duration::from_secs(30));

    let transport = DnsConfig::custom(
        transport_timeout,
        ResolverConfig::cloudflare(),
        Default::default(),
    )
    .await?;

    let transport = OrTransport::new(relay, transport)
        .upgrade(Version::V1)
        .authenticate(noise_config)
        .multiplex(multiplex_upgrade)
        .timeout(Duration::from_secs(30))
        .map(|(peer_id, muxer), _| (peer_id, StreamMuxerBox::new(muxer)))
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err))
        .boxed();

    let transport = OrTransport::new(quic_transport, transport)
        .map(|either_output, _| match either_output {
            Either::Left((peer_id, muxer)) => (peer_id, StreamMuxerBox::new(muxer)),
            Either::Right((peer_id, muxer)) => (peer_id, StreamMuxerBox::new(muxer)),
        })
        .boxed();

    Ok(transport)
}
fn generate_ed25519(secret_key_seed: u8) -> identity::Keypair {
    let mut bytes = [0u8; 32];
    bytes[0] = secret_key_seed;
    identity::Keypair::ed25519_from_bytes(bytes).expect("Keypair is valid")
}

#[allow(dead_code)]
fn peer_id_from_multiaddr(addr: Multiaddr) -> Option<PeerId> {
    let (peer, _) = extract_peer_id_from_multiaddr(addr);
    peer
}

#[allow(dead_code)]
fn extract_peer_id_from_multiaddr(mut addr: Multiaddr) -> (Option<PeerId>, Multiaddr) {
    match addr.pop() {
        Some(Protocol::P2p(id)) => (Some(id), addr),
        _ => (None, addr),
    }
}
