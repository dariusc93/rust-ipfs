use libp2p::{
    multiaddr::{self, Protocol},
    swarm::dial_opts::DialOpts,
    Multiaddr, PeerId,
};
use std::{
    convert::{TryFrom, TryInto},
    fmt,
    str::FromStr,
};

pub trait MultiaddrExt {
    /// Peer id
    fn peer_id(&self) -> Option<PeerId>;
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
        if let Some(Protocol::P2p(hash)) = self.iter().last() {
            return PeerId::from_multihash(hash).ok();
        }

        None
    }

    fn relay_peer_id(&self) -> Option<PeerId> {
        if !self.is_relay() {
            return None;
        }

        while let Some(protocol) = self.iter().next() {
            //Find the first peer id and return it
            if let Protocol::P2p(hash) = protocol {
                return PeerId::from_multihash(hash).ok();
            }
        }

        None
    }

    fn address(&self) -> Option<Multiaddr> {
        let mut addr = self.clone();
        while let Some(proto) = addr.pop() {
            if matches!(
                proto,
                Protocol::Ip4(_) | Protocol::Ip6(_) | Protocol::Dnsaddr(_)
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

/// An error that can be thrown when converting to `MultiaddrWithPeerId` and
/// `MultiaddrWithoutPeerId`.
#[derive(Debug)]
pub enum MultiaddrWrapperError {
    /// The source `Multiaddr` unexpectedly contains `Protocol::P2p`.
    ContainsProtocolP2p,
    /// The provided `Multiaddr` is invalid.
    InvalidMultiaddr(multiaddr::Error),
    /// The `PeerId` created based on the `Protocol::P2p` is invalid.
    InvalidPeerId,
    /// The `Protocol::P2p` is unexpectedly missing from the source `Multiaddr`.
    MissingProtocolP2p,
}

impl fmt::Display for MultiaddrWrapperError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for MultiaddrWrapperError {}

/// A wrapper for `Multiaddr` that does **not** contain `Protocol::P2p`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MultiaddrWithoutPeerId(Multiaddr);

impl fmt::Display for MultiaddrWithoutPeerId {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, fmt)
    }
}

impl TryFrom<Multiaddr> for MultiaddrWithoutPeerId {
    type Error = MultiaddrWrapperError;

    fn try_from(addr: Multiaddr) -> Result<Self, Self::Error> {
        if addr.iter().any(|p| matches!(p, Protocol::P2p(_))) {
            Err(MultiaddrWrapperError::ContainsProtocolP2p)
        } else {
            Ok(Self(addr))
        }
    }
}

impl From<MultiaddrWithPeerId> for MultiaddrWithoutPeerId {
    fn from(addr: MultiaddrWithPeerId) -> Self {
        let MultiaddrWithPeerId { multiaddr, .. } = addr;
        MultiaddrWithoutPeerId(multiaddr.into())
    }
}

impl From<MultiaddrWithoutPeerId> for Multiaddr {
    fn from(addr: MultiaddrWithoutPeerId) -> Self {
        let MultiaddrWithoutPeerId(multiaddr) = addr;
        multiaddr
    }
}

impl AsRef<Multiaddr> for MultiaddrWithoutPeerId {
    fn as_ref(&self) -> &Multiaddr {
        &self.0
    }
}

impl FromStr for MultiaddrWithoutPeerId {
    type Err = MultiaddrWrapperError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let multiaddr = s
            .parse::<Multiaddr>()
            .map_err(MultiaddrWrapperError::InvalidMultiaddr)?;
        multiaddr.try_into()
    }
}

impl PartialEq<Multiaddr> for MultiaddrWithoutPeerId {
    fn eq(&self, other: &Multiaddr) -> bool {
        &self.0 == other
    }
}

impl MultiaddrWithoutPeerId {
    /// Adds the peer_id information to this address without peer_id, turning it into
    /// [`MultiaddrWithPeerId`].
    pub fn with(self, peer_id: PeerId) -> MultiaddrWithPeerId {
        (self, peer_id).into()
    }
}

/// A `Multiaddr` paired with a discrete `PeerId`. The `Multiaddr` can contain a
/// `Protocol::P2p`, but it's not as easy to work with, and some functionalities
/// don't support it being contained within the `Multiaddr`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MultiaddrWithPeerId {
    /// The [`Multiaddr`] without the [`Protocol::P2p`] suffix.
    pub multiaddr: MultiaddrWithoutPeerId,
    /// The peer id from the [`Protocol::P2p`] suffix.
    pub peer_id: PeerId,
}

impl From<MultiaddrWithPeerId> for DialOpts {
    fn from(value: MultiaddrWithPeerId) -> Self {
        DialOpts::peer_id(value.peer_id)
            .addresses(vec![value.multiaddr.0])
            .build()
    }
}

impl From<(MultiaddrWithoutPeerId, PeerId)> for MultiaddrWithPeerId {
    fn from((multiaddr, peer_id): (MultiaddrWithoutPeerId, PeerId)) -> Self {
        Self { multiaddr, peer_id }
    }
}

impl From<MultiaddrWithPeerId> for Multiaddr {
    fn from(addr: MultiaddrWithPeerId) -> Self {
        let MultiaddrWithPeerId { multiaddr, peer_id } = addr;
        let mut multiaddr: Multiaddr = multiaddr.into();
        multiaddr.push(Protocol::P2p(peer_id.into()));

        multiaddr
    }
}

impl TryFrom<Multiaddr> for MultiaddrWithPeerId {
    type Error = MultiaddrWrapperError;

    fn try_from(multiaddr: Multiaddr) -> Result<Self, Self::Error> {
        if let Some(Protocol::P2p(hash)) = multiaddr.iter().find(|p| matches!(p, Protocol::P2p(_)))
        {
            // FIXME: we've had a case where the PeerId was not the last part of the Multiaddr, which
            // is unexpected; it is hard to trigger, hence this debug-only assertion so we might be
            // able to catch it sometime during tests
            debug_assert!(
                matches!(
                    multiaddr.iter().last(),
                    Some(Protocol::P2p(_)) | Some(Protocol::P2pCircuit)
                ),
                "unexpected Multiaddr format: {multiaddr}"
            );

            let multiaddr = MultiaddrWithoutPeerId(
                multiaddr
                    .into_iter()
                    .filter(|p| !matches!(p, Protocol::P2p(_) | Protocol::P2pCircuit))
                    .collect(),
            );
            let peer_id =
                PeerId::from_multihash(hash).map_err(|_| MultiaddrWrapperError::InvalidPeerId)?;
            Ok(Self { multiaddr, peer_id })
        } else {
            Err(MultiaddrWrapperError::MissingProtocolP2p)
        }
    }
}

impl FromStr for MultiaddrWithPeerId {
    type Err = MultiaddrWrapperError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let multiaddr = s
            .parse::<Multiaddr>()
            .map_err(MultiaddrWrapperError::InvalidMultiaddr)?;
        Self::try_from(multiaddr)
    }
}

impl fmt::Display for MultiaddrWithPeerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/p2p/{}", self.multiaddr, self.peer_id)
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
        Some(Protocol::P2p(hash)) => match PeerId::from_multihash(hash) {
            Ok(id) => (Some(id), addr),
            _ => (None, addr),
        },
        _ => (None, old_addr),
    }
}

#[cfg(test)]
mod tests {
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
