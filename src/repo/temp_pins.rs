use ipld_core::cid::Cid;
use std::collections::{BTreeSet, HashMap};
use std::time::Duration;
use web_time::Instant;

#[derive(Debug)]
pub(super) struct TemporaryPins {
    duration: Duration,
    stored_at: HashMap<Cid, Instant>,
    by_time: BTreeSet<(Instant, Cid)>,
}

impl Default for TemporaryPins {
    fn default() -> Self {
        Self {
            duration: Duration::from_secs(2 * 60),
            stored_at: HashMap::new(),
            by_time: BTreeSet::new(),
        }
    }
}

impl TemporaryPins {
    pub(super) fn set_duration(&mut self, duration: Duration) {
        self.duration = duration;
        self.prune();
    }

    pub(super) fn record(&mut self, cids: impl IntoIterator<Item = Cid>) {
        self.prune();
        if self.duration.is_zero() {
            return;
        }

        let now = Instant::now();
        for cid in cids {
            if let Some(previous) = self.stored_at.insert(cid, now) {
                self.by_time.remove(&(previous, cid));
            }
            self.by_time.insert((now, cid));
        }
    }

    pub(super) fn prune(&mut self) {
        let now = Instant::now();
        while let Some(&(stored_at, cid)) = self.by_time.first() {
            if now.saturating_duration_since(stored_at) < self.duration {
                break;
            }
            self.by_time.pop_first();
            self.stored_at.remove(&cid);
        }
    }

    pub(super) fn contains(&self, cid: &Cid) -> bool {
        self.stored_at.contains_key(cid)
    }

    pub(super) fn remove(&mut self, cid: &Cid) {
        if let Some(stored_at) = self.stored_at.remove(cid) {
            self.by_time.remove(&(stored_at, *cid));
        }
    }

    pub(super) fn remove_before(&mut self, cid: &Cid, before: Instant) {
        if self.stored_at.get(cid).is_some_and(|at| *at < before) {
            self.remove(cid);
        }
    }

    pub(super) fn next_expiry(&self) -> Option<Duration> {
        self.by_time
            .first()
            .map(|(stored_at, _)| self.duration.saturating_sub(stored_at.elapsed()))
    }
}
