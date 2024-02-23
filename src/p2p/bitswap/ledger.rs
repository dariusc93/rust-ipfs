use std::{
    collections::{hash_map::Entry, HashMap},
    sync::Arc,
};

use libipld::Cid;
use libp2p::PeerId;
use parking_lot::Mutex;

#[derive(Clone, Default)]
pub struct Ledger {
    inner: Arc<Mutex<LedgerInner>>,
}

#[derive(Default)]
struct LedgerInner {
    wants: HashMap<Cid, i32>,
    haves: HashMap<PeerId, HashMap<Cid, i32>>,
}

impl Ledger {
    pub fn add_want(&self, cid: Cid, priority: i32) {
        let mut inner = self.inner.lock();
        inner.wants.insert(cid, priority);
    }

    pub fn contains_wants(&self, cid: Cid) -> bool {
        let inner = self.inner.lock();
        inner.wants.contains_key(&cid)
    }

    pub fn remove_wants(&self, cid: Cid) {
        let mut inner = self.inner.lock();
        inner.wants.remove(&cid);
    }

    pub fn add_have(&self, peer_id: PeerId, cid: Cid, priority: i32) {
        let mut inner = self.inner.lock();
        inner
            .haves
            .entry(peer_id)
            .or_default()
            .insert(cid, priority);
    }

    pub fn contains_have(&self, peer_id: PeerId, cid: Cid) -> bool {
        let inner = self.inner.lock();
        inner
            .haves
            .get(&peer_id)
            .map(|map| map.contains_key(&cid))
            .unwrap_or_default()
    }

    pub fn remove_have(&self, peer_id: PeerId, cid: Cid) {
        let mut inner = self.inner.lock();
        if let Entry::Occupied(mut entry) = inner.haves.entry(peer_id) {
            let map = entry.get_mut();
            map.remove(&cid);
            if map.is_empty() {
                entry.remove();
            }
        }
    }

    pub fn remove_haves(&self, cid: Cid) {
        let mut inner = self.inner.lock();
        let list = inner.haves.drain();
    }

    pub fn remove_peer_haves(&self, peer_id: PeerId) {
        let mut inner = self.inner.lock();
        inner.haves.remove(&peer_id);
    }
}
