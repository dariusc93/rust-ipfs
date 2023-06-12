use libp2p::{multiaddr::Protocol, Multiaddr, PeerId};

pub trait MultiaddrExt {
    /// Peer id
    fn peer_id(&self) -> Option<PeerId>;

    fn extract_peer_id(&mut self) -> Option<PeerId>;

    /// Relay peer id
    fn relay_peer_id(&self) -> Option<PeerId>;

    /// Address that only doesnt include peer protocols
    fn address(&self) -> Option<Multiaddr>;

    /// Determine if the address is a relay circuit
    fn is_relay(&self) -> bool;

    /// Determine if the address is being relayed to a peer
    fn is_relayed(&self) -> bool;
}

impl MultiaddrExt for Multiaddr {
    fn peer_id(&self) -> Option<PeerId> {
        if let Some(Protocol::P2p(peer)) = self.iter().last() {
            return Some(peer);
        }

        None
    }

    fn extract_peer_id(&mut self) -> Option<PeerId> {
        match self.pop() {
            Some(Protocol::P2p(peer)) => Some(peer),
            _ => None,
        }
    }

    fn relay_peer_id(&self) -> Option<PeerId> {
        if !self.is_relay() {
            return None;
        }

        while let Some(protocol) = self.iter().next() {
            //Find the first peer id and return it
            if let Protocol::P2p(peer) = protocol {
                return Some(peer);
            }
        }

        None
    }

    fn address(&self) -> Option<Multiaddr> {
        let mut addr = self.clone();
        while let Some(proto) = addr.pop() {
            if matches!(
                proto,
                Protocol::Ip4(_)
                    | Protocol::Ip6(_)
                    | Protocol::Tcp(_)
                    | Protocol::Udp(_)
                    | Protocol::Quic
                    | Protocol::QuicV1
                    | Protocol::Dnsaddr(_)
            ) {
                return Some(addr);
            }
        }

        None
    }

    fn is_relay(&self) -> bool {
        self.iter()
            .any(|proto| matches!(proto, Protocol::P2pCircuit))
    }

    fn is_relayed(&self) -> bool {
        if !self.is_relay() {
            return false;
        }

        if self.peer_id().is_none() {
            return false;
        }

        true
    }
}

#[allow(dead_code)]
/// Returns the last peer id in a Multiaddr
pub(crate) fn peer_id_from_multiaddr(addr: Multiaddr) -> Option<PeerId> {
    let (peer, _) = extract_peer_id_from_multiaddr(addr);
    peer
}

#[allow(dead_code)]
pub(crate) fn extract_peer_id_from_multiaddr(mut addr: Multiaddr) -> (Option<PeerId>, Multiaddr) {
    let old_addr = addr.clone();
    match addr.pop() {
        Some(Protocol::P2p(peer)) => (Some(peer), addr),
        _ => (None, old_addr),
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn connection_targets() {
        let peer_id = PeerId::from_str("QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ")
            .expect("Valid peer id");
        let multiaddr_wo_peer =
            Multiaddr::from_str("/ip4/104.131.131.82/tcp/4001").expect("Valid multiaddr");
        let multiaddr_with_peer =
            Multiaddr::from_str(&format!("{multiaddr_wo_peer}/p2p/{peer_id}"))
                .expect("valid multiaddr");
        let p2p_peer = Multiaddr::from_str(&format!("/p2p/{peer_id}")).expect("Valid multiaddr");

        assert!(multiaddr_wo_peer.peer_id().is_none());
        assert!(multiaddr_with_peer.peer_id().is_some());
        assert!(p2p_peer.peer_id().is_some());

        let peer_id_target = multiaddr_with_peer.peer_id().expect("Valid peer id");

        assert_eq!(peer_id_target, peer_id);
    }
}
