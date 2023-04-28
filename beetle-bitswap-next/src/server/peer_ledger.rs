use ahash::{AHashMap, AHashSet};
use cid::Cid;
use libp2p::PeerId;

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PeerLedger {
    cids: AHashMap<Cid, AHashSet<PeerId>>,
}

impl PeerLedger {
    pub fn wants(&mut self, peer: PeerId, cid: Cid) {
        self.cids.entry(cid).or_default().insert(peer);
    }

    pub fn cancel_want(&mut self, peer: &PeerId, cid: &Cid) {
        if let std::collections::hash_map::Entry::Occupied(mut entry) = self.cids.entry(*cid) {
            let peers = entry.get_mut();
            peers.remove(peer);
            if peers.is_empty() {
                entry.remove();
            }
        }
        self.cids.shrink_to_fit();
    }

    pub fn peers(&self, cid: &Cid) -> Option<&AHashSet<PeerId>> {
        self.cids.get(cid)
    }
}
