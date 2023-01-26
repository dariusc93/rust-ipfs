use anyhow::Error;
use futures::channel::oneshot;
use igd_next::{aio, PortMappingProtocol, SearchOptions};
use libp2p::multiaddr::Protocol;
use libp2p::Multiaddr;
use std::net::{SocketAddr, SocketAddrV4, SocketAddrV6};
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::sync::broadcast::Receiver;
use tokio::time::{self, Instant};
use tracing::debug;

fn multiaddr_to_socket_port(
    addr: Multiaddr,
) -> Result<(SocketAddr, u16, PortMappingProtocol), Error> {
    let mut iter = addr.iter();
    let mut addr = iter
        .next()
        .and_then(|proto| match proto {
            Protocol::Ip4(addr) if addr.is_private() => {
                Some(SocketAddr::V4(SocketAddrV4::new(addr, 0)))
            }
            Protocol::Ip6(addr)
                if !addr.is_loopback()
                    && (addr.segments()[0] & 0xffc0) != 0xfe80
                    && (addr.segments()[0] & 0xfe00) != 0xfc00 =>
            {
                Some(SocketAddr::V6(SocketAddrV6::new(addr, 0, 0, 0)))
            }
            _ => None,
        })
        .ok_or_else(|| anyhow::anyhow!("Invalid address type"))?;

    let (protocol, port) = iter
        .next()
        .and_then(|proto| match proto {
            Protocol::Tcp(port) => Some((PortMappingProtocol::TCP, port)),
            Protocol::Udp(port) => Some((PortMappingProtocol::UDP, port)),
            _ => None,
        })
        .ok_or_else(|| anyhow::anyhow!("Invalid protocol type"))?;

    addr.set_port(port);

    Ok((addr, port, protocol))
}

pub(crate) fn forward_port(
    handle: Handle,
    addr: Multiaddr,
    lease_interval: Duration,
    mut termination_rx: Receiver<()>,
) -> Result<(), Error> {
    let lease_interval = lease_interval.min(Duration::from_secs(u32::MAX.into()));
    let lease_interval_u32 = lease_interval.as_secs() as u32;
    let (tx, rx) = oneshot::channel();

    // Start a tokio task to renew the lease periodically.
    tokio::spawn(async move {
        match add_or_renewal_port(addr.clone(), lease_interval_u32).await {
            Ok(_) => {
                let _ = tx.send(Ok(()));
            }
            Err(e) => {
                let _ = tx.send(Err(e));
                return;
            }
        };
        let mut timer = time::interval_at(Instant::now() + lease_interval, lease_interval);

        loop {
            tokio::select! {
                _e = termination_rx.recv() => {
                    debug!("Terminate renewal task");
                    break;
                },
                _ = timer.tick() => {
                    debug!("Renewing lease for {}", addr);

                    if let Err(error) = add_or_renewal_port(addr.clone(), lease_interval_u32).await {
                        //TODO: Do we want to break loop to end the task or should we continue to retry on an interval?
                        error!("Failed to renew lease: {}", error);
                    }
                }
            };
        }
    });
    tokio::task::block_in_place(|| handle.block_on(rx))?
}

pub(crate) async fn add_or_renewal_port(addr: Multiaddr, lease_duration: u32) -> Result<(), Error> {
    let (local_addr, ext_port, protocol) = multiaddr_to_socket_port(addr)?;
    let gateway = aio::search_gateway(SearchOptions::default()).await?;

    gateway
        .add_port(protocol, ext_port, local_addr, lease_duration, "rust-ipfs")
        .await?;

    Ok(())
}
