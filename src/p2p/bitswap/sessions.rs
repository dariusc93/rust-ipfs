use std::{
    collections::{HashMap, HashSet, VecDeque},
    future::IntoFuture,
    pin::Pin,
    task::{Context, Poll, Waker},
    time::Duration,
};

use bytes::Bytes;
use futures::{future::BoxFuture, ready, stream::FusedStream, FutureExt, Stream};
use futures_timer::Delay;
use indexmap::IndexMap;
use ipld_core::cid::Cid;
use libp2p::PeerId;
use std::fmt::Debug;

use crate::{repo::Repo, Block};

const CAP_THRESHOLD: usize = 100;

#[derive(Debug)]
pub enum WantSessionEvent {
    Dial { peer_id: PeerId },
    SendWant { peer_id: PeerId },
    SendCancel { peer_id: PeerId },
    SendBlock { peer_id: PeerId },
    BlockStored,
    NeedBlock,
}

pub enum WantSessionState {
    Idle,
    NextBlock {
        previous_peer_id: Option<PeerId>,
    },
    NextBlockPending {
        peer_id: PeerId,
        timer: Delay,
    },
    PutBlock {
        from_peer_id: PeerId,
        fut: BoxFuture<'static, Result<Cid, anyhow::Error>>,
    },
    Complete,
}

impl Debug for WantSessionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WantSessionState")
    }
}

#[derive(Debug)]
enum WantDiscovery {
    Disable,
    Start,
    Running { timer: Delay },
}

#[derive(Debug, PartialEq, Eq)]
enum PeerWantState {
    Pending,
    Sent,
    Have,
    Failed,
    Waiting,
    Disconnect { backoff: bool },
}

#[derive(Debug)]
pub struct WantSession {
    cid: Cid,
    wants: IndexMap<PeerId, PeerWantState>,
    discovery: WantDiscovery,
    received: bool,
    waker: Option<Waker>,
    repo: Repo,
    state: WantSessionState,
    timeout: Option<Duration>,
    cancel: VecDeque<PeerId>,
}

impl WantSession {
    pub fn new(repo: &Repo, cid: Cid) -> Self {
        Self {
            cid,
            wants: Default::default(),
            discovery: WantDiscovery::Disable,
            received: false,
            repo: repo.clone(),
            waker: None,
            state: WantSessionState::Idle,
            timeout: None,
            cancel: Default::default(),
        }
    }

    pub fn send_have_block(&mut self, peer_id: PeerId) {
        match self.wants.entry(peer_id) {
            indexmap::map::Entry::Occupied(mut entry) => {
                let state = entry.get_mut();
                if !matches!(state, PeerWantState::Pending | PeerWantState::Sent) {
                    *state = PeerWantState::Pending;
                }
            }
            indexmap::map::Entry::Vacant(entry) => {
                entry.insert(PeerWantState::Pending);
            }
        };

        tracing::trace!(session = %self.cid, %peer_id, name = "want_session", "send have block");
        self.discovery = WantDiscovery::Disable;
        if let Some(w) = self.waker.take() {
            w.wake();
        }
    }

    pub fn has_block(&mut self, peer_id: PeerId) {
        tracing::debug!(session = %self.cid, %peer_id, name = "want_session", "have block");

        self.wants
            .entry(peer_id)
            .and_modify(|state| *state = PeerWantState::Have)
            .or_insert(PeerWantState::Have);

        if !matches!(self.state, WantSessionState::NextBlock { .. }) {
            tracing::debug!(session = %self.cid, %peer_id, name = "want_session", "change state to next_block");
            self.state = WantSessionState::NextBlock {
                previous_peer_id: None,
            };
        }

        self.discovery = WantDiscovery::Disable;

        if let Some(w) = self.waker.take() {
            w.wake();
        }
    }

    pub fn dont_have_block(&mut self, peer_id: PeerId) {
        tracing::trace!(session = %self.cid, %peer_id, name = "want_session", "dont have block");
        self.wants.shift_remove(&peer_id);

        if self.is_empty() {
            self.state = WantSessionState::Idle;
            if !matches!(
                self.discovery,
                WantDiscovery::Running { .. } | WantDiscovery::Start
            ) {
                self.discovery = WantDiscovery::Start;
            }
            tracing::warn!(session = %self.cid, %peer_id, name = "want_session", "session is empty. setting state to idle.");
        } else {
            // change state to next block so it will perform another request if possible,
            // otherwise notify swarm if no request been sent
            tracing::debug!(session = %self.cid, name = "want_session", "checking next peer for block");
            self.state = WantSessionState::NextBlock {
                previous_peer_id: None,
            };
        }
        if let Some(w) = self.waker.take() {
            w.wake();
        }
    }

    pub fn peer_disconnected(&mut self, peer_id: PeerId) -> bool {
        if !self.contains(peer_id) {
            return false;
        }
        if let indexmap::map::Entry::Occupied(mut entry) = self.wants.entry(peer_id) {
            let state = entry.get_mut();
            if let PeerWantState::Disconnect { backoff } = state {
                if *backoff {
                    entry.shift_remove();
                }
                return false;
            } else {
                *state = PeerWantState::Disconnect { backoff: false };
            }
        }

        true
    }

    pub fn put_block(&mut self, peer_id: PeerId, block: Block) {
        if matches!(self.state, WantSessionState::PutBlock { .. }) {
            tracing::warn!(session = %self.cid, %peer_id, cid = %block.cid(), name = "want_session", "state already putting block into store");
        } else {
            tracing::info!(%peer_id, cid = %block.cid(), name = "want_session", "storing block");
            let fut = self.repo.put_block(block).into_future();
            self.state = WantSessionState::PutBlock {
                from_peer_id: peer_id,
                fut,
            };
            self.discovery = WantDiscovery::Disable;
        }

        if let Some(w) = self.waker.take() {
            w.wake();
        }
    }

    pub fn remove_peer(&mut self, peer_id: PeerId) {
        if !self.is_empty() {
            // tracing::debug!(session = %self.cid, %peer_id, name = "want_session", "removing peer from want_session");
            self.wants.shift_remove(&peer_id);

            if matches!(self.state, WantSessionState::NextBlockPending { peer_id: p, .. } if p == peer_id)
            {
                self.state = WantSessionState::NextBlock {
                    previous_peer_id: Some(peer_id),
                };
            }
        } else if !matches!(
            self.discovery,
            WantDiscovery::Running { .. } | WantDiscovery::Start
        ) {
            self.discovery = WantDiscovery::Start;
        }

        if let Some(w) = self.waker.take() {
            w.wake();
        }
    }

    pub fn contains(&self, peer_id: PeerId) -> bool {
        self.wants.contains_key(&peer_id)
    }

    pub fn is_empty(&self) -> bool {
        self.wants
            .iter()
            .all(|(_, state)| matches!(state, PeerWantState::Failed))
    }
}

impl Unpin for WantSession {}

impl Stream for WantSession {
    type Item = WantSessionEvent;

    #[tracing::instrument(level = "trace", name = "WantSession::poll_next", skip(self, cx))]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let cid = self.cid;
        // We send cancels as we conclude the session.
        if let Some(peer_id) = self.cancel.pop_back() {
            return Poll::Ready(Some(WantSessionEvent::SendCancel { peer_id }));
        }

        if self.received {
            // We received the block by this point so there is nothing more to do for the session
            return Poll::Ready(None);
        }

        if let Some((peer_id, state)) = self
            .wants
            .iter_mut()
            .find(|(_, state)| matches!(state, PeerWantState::Disconnect { backoff } if !backoff))
        {
            // We will attempt to dial the peer if they were reportedly disconnected, making sure to backoff
            // so we dont dial again if the connection fails
            let PeerWantState::Disconnect { backoff } = state else {
                unreachable!("peer state is set to disconnect");
            };
            *backoff = true;
            tracing::info!(session = %cid, %peer_id, name = "want_session", "peer is disconnected. Attempting to dial peer");
            return Poll::Ready(Some(WantSessionEvent::Dial { peer_id: *peer_id }));
        }

        if !matches!(self.state, WantSessionState::Complete) {
            if let Some((peer_id, state)) = self
                .wants
                .iter_mut()
                .find(|(_, state)| matches!(state, PeerWantState::Pending))
            {
                *state = PeerWantState::Sent;
                tracing::debug!(session = %cid, %peer_id, name = "want_session", "sent want block");
                return Poll::Ready(Some(WantSessionEvent::SendWant { peer_id: *peer_id }));
            } else if self.wants.capacity() > CAP_THRESHOLD {
                self.wants.shrink_to_fit()
            }
        }

        let this = &mut *self;

        loop {
            match &mut this.state {
                WantSessionState::Idle => {
                    if let Some(peer_id) = this.cancel.pop_back() {
                        // Kick start the cancel requests.
                        return Poll::Ready(Some(WantSessionEvent::SendCancel { peer_id }));
                    }

                    match &mut this.discovery {
                        WantDiscovery::Disable => {}
                        WantDiscovery::Start => {
                            this.discovery = WantDiscovery::Running {
                                timer: Delay::new(Duration::from_secs(60)),
                            };

                            return Poll::Ready(Some(WantSessionEvent::NeedBlock));
                        }
                        WantDiscovery::Running { timer } => {
                            if timer.poll_unpin(cx).is_ready() {
                                timer.reset(Duration::from_secs(60));
                                return Poll::Ready(Some(WantSessionEvent::NeedBlock));
                            }
                        }
                    }

                    this.waker = Some(cx.waker().clone());

                    return Poll::Pending;
                }
                WantSessionState::NextBlock { previous_peer_id } => {
                    if let Some(peer_id) = previous_peer_id.take() {
                        tracing::debug!(session = %cid, %peer_id, name = "want_session", "failed block");
                        // If we hit this state after sending a have_block request to said peer, this means
                        //      1) Peer had block but was unable to or refuse to send block; or
                        //      2) Peer sent block but it was corrupted; or
                        //      3) Peer took to long to send block and the request timeout
                        // Either way, we will move the peer to a failed status and in the future,
                        // pass this to the behaviour or to another session to keep score of successful or failed exchanges
                        // so we can prioritize those who will likely exchange blocks more successfully.
                        let state = this.wants.get_mut(&peer_id).expect("peer exist in state");
                        *state = PeerWantState::Failed;
                    }

                    if let Some((next_peer_id, state)) = this
                        .wants
                        .iter_mut()
                        .find(|(_, state)| matches!(state, PeerWantState::Have))
                    {
                        tracing::info!(session = %cid, %next_peer_id, name = "want_session", "sending block request to next peer");
                        this.discovery = WantDiscovery::Disable;
                        let timeout = match this.timeout {
                            Some(timeout) if !timeout.is_zero() => timeout,
                            //Note: This duration is fixed since we should assume a single block should not take more than 15 seconds
                            //      to be received, however we probably should take into consideration of the ping of the peer and
                            //      in the future the size of the pending block to determine if it should be higher or lower
                            //      so the session wont be sleeping to long or to short.
                            _ => Duration::from_secs(15),
                        };
                        let timer = Delay::new(timeout);
                        this.state = WantSessionState::NextBlockPending {
                            peer_id: *next_peer_id,
                            timer,
                        };
                        *state = PeerWantState::Waiting;
                        return Poll::Ready(Some(WantSessionEvent::SendBlock {
                            peer_id: *next_peer_id,
                        }));
                    }

                    tracing::debug!(session = %cid, name = "want_session", "session is idle");
                    this.state = WantSessionState::Idle;

                    if this
                        .wants
                        .values()
                        .all(|state| matches!(state, PeerWantState::Failed))
                        && matches!(this.discovery, WantDiscovery::Disable)
                    {
                        this.discovery = WantDiscovery::Start;
                    }
                }
                WantSessionState::NextBlockPending {
                    peer_id,
                    ref mut timer,
                } => {
                    // We will wait until the peer respond and if it does not respond in time to timeout the request and proceed to the next block request
                    ready!(timer.poll_unpin(cx));
                    tracing::warn!(session = %cid, name = "want_session", %peer_id, "request timeout attempting to get next block");
                    this.state = WantSessionState::NextBlock {
                        previous_peer_id: Some(*peer_id),
                    };
                }
                WantSessionState::PutBlock { from_peer_id, fut } => {
                    let peer_id = *from_peer_id;
                    match ready!(fut.poll_unpin(cx)) {
                        Ok(cid) => {
                            tracing::info!(session = %self.cid, %peer_id, block = %cid, name = "want_session", "block stored in block store");
                            self.state = WantSessionState::Complete;
                            return Poll::Ready(Some(WantSessionEvent::BlockStored));
                        }
                        Err(e) => {
                            tracing::error!(session = %cid, %peer_id, error = %e, name = "want_session", "error storing block in store");
                            this.state = WantSessionState::NextBlock {
                                previous_peer_id: Some(peer_id),
                            };
                        }
                    }
                }
                WantSessionState::Complete => {
                    this.received = true;
                    let mut peers: HashSet<PeerId> = HashSet::new();
                    // although this may be empty by the time we reach here, its best to clear it anyway since nothing was ever sent
                    peers.extend(this.wants.keys());

                    tracing::info!(session = %cid, pending_cancellation = peers.len());

                    this.cancel.extend(peers);
                    // Wake up the task so the stream would poll any cancel requests
                    this.state = WantSessionState::Idle;
                }
            }
        }
    }
}

impl FusedStream for WantSession {
    fn is_terminated(&self) -> bool {
        self.received
    }
}

#[derive(Debug)]
pub enum HaveSessionEvent {
    Have { peer_id: PeerId },
    DontHave { peer_id: PeerId },
    Block { peer_id: PeerId, bytes: Bytes },
    Cancelled,
}

enum HaveSessionState {
    Idle,
    ContainBlock {
        fut: BoxFuture<'static, Result<bool, anyhow::Error>>,
    },
    GetBlock {
        fut: BoxFuture<'static, Result<Option<Block>, anyhow::Error>>,
    },
    Block {
        bytes: Bytes,
    },
    Complete,
}

#[derive(Debug)]
enum HaveWantState {
    #[allow(dead_code)]
    Pending {
        send_dont_have: bool,
    },
    Sent,
    Block,
    BlockSent,
}

pub struct HaveSession {
    cid: Cid,
    want: HashMap<PeerId, HaveWantState>,
    send_dont_have: HashSet<PeerId>,
    have: Option<bool>,
    repo: Repo,
    waker: Option<Waker>,
    state: HaveSessionState,
}

impl HaveSession {
    pub fn new(repo: &Repo, cid: Cid) -> Self {
        let mut session = Self {
            cid,
            want: HashMap::new(),
            have: None,
            repo: repo.clone(),
            waker: None,
            send_dont_have: Default::default(),
            state: HaveSessionState::Idle,
        };
        let repo = session.repo.clone();
        // We perform a precheck against the block to determine if we have it so when a peer send a request
        // we can respond accordingly
        let fut = async move { repo.contains(&cid).await }.boxed();

        session.state = HaveSessionState::ContainBlock { fut };

        session
    }

    pub fn has_peer(&self, peer_id: PeerId) -> bool {
        self.want.contains_key(&peer_id)
    }

    pub fn want_block(&mut self, peer_id: PeerId, send_dont_have: bool) {
        if self.want.contains_key(&peer_id) {
            tracing::warn!(session = %self.cid, %peer_id, "peer already requested block. Ignoring additional request");
            return;
        }

        tracing::info!(session = %self.cid, %peer_id, name = "have_session", "peer want block");

        self.want
            .insert(peer_id, HaveWantState::Pending { send_dont_have });

        if let Some(w) = self.waker.take() {
            w.wake();
        }
    }

    pub fn need_block(&mut self, peer_id: PeerId) {
        if self
            .want
            .get(&peer_id)
            .map(|state| matches!(state, HaveWantState::Block | HaveWantState::BlockSent))
            .unwrap_or_default()
        {
            tracing::warn!(session = %self.cid, %peer_id, name = "have_session", "already sending block to peer");
            return;
        }

        tracing::info!(session = %self.cid, %peer_id, name = "have_session", "peer requested block");

        self.want
            .entry(peer_id)
            .and_modify(|state| *state = HaveWantState::Block)
            .or_insert(HaveWantState::Block);

        if !matches!(
            self.state,
            HaveSessionState::GetBlock { .. } | HaveSessionState::Block { .. }
        ) {
            let repo = self.repo.clone();
            let cid = self.cid;
            let fut = async move { repo.get_block_now(&cid).await }.boxed();

            tracing::info!(session = %self.cid, %peer_id, name = "have_session", "change state to get_block");
            self.state = HaveSessionState::GetBlock { fut };

            if let Some(w) = self.waker.take() {
                w.wake();
            }
        }
    }

    pub fn remove_peer(&mut self, peer_id: PeerId) {
        tracing::info!(session = %self.cid, %peer_id, name = "have_session", "removing peer from have_session");
        self.want.remove(&peer_id);
        self.send_dont_have.remove(&peer_id);
        if let Some(w) = self.waker.take() {
            w.wake();
        }
    }

    pub fn reset(&mut self) {
        // Only reset if we have not resolve block
        if self.have.is_none() || self.have.unwrap_or_default() {
            return;
        }

        tracing::info!(session = %self.cid, name = "have_session", "resetting session");

        for (peer_id, state) in self.want.iter_mut() {
            *state = HaveWantState::Pending {
                send_dont_have: self.send_dont_have.contains(peer_id),
            };
            tracing::debug!(session = %self.cid, name = "have_session", %peer_id, "resetting peer state");
        }
        let repo = self.repo.clone();
        let cid = self.cid;
        let fut = async move { repo.contains(&cid).await }.boxed();

        self.state = HaveSessionState::ContainBlock { fut };
        if let Some(w) = self.waker.take() {
            w.wake();
        }
    }

    pub fn cancel(&mut self, peer_id: PeerId) {
        self.want.remove(&peer_id);
        self.send_dont_have.remove(&peer_id);
        tracing::info!(session = %self.cid, %peer_id, name = "have_session", "cancelling request");
    }
}

impl Unpin for HaveSession {}

impl Stream for HaveSession {
    type Item = HaveSessionEvent;

    #[tracing::instrument(level = "trace", name = "HaveSession::poll_next", skip(self, cx))]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if matches!(self.state, HaveSessionState::Complete) {
            return Poll::Ready(None);
        }

        let this = &mut *self;

        // Since our state contains a block, we can attempt to provide it to peers who requests it then we will
        // reset the state back to idle until the wants are empty
        if let HaveSessionState::Block { bytes } = &this.state {
            if let Some((next_peer_id, state)) = this
                .want
                .iter_mut()
                .find(|(_, state)| matches!(state, HaveWantState::Block))
            {
                *state = HaveWantState::BlockSent;
                return Poll::Ready(Some(HaveSessionEvent::Block {
                    peer_id: *next_peer_id,
                    bytes: bytes.clone(),
                }));
            }

            if this
                .want
                .iter()
                .all(|(_, state)| matches!(state, HaveWantState::BlockSent))
                || this.want.is_empty()
            {
                // Since we have no more peers who want the block, we will finalize the session
                this.state = HaveSessionState::Complete;
                this.want.clear();
                this.send_dont_have.clear();
                return Poll::Ready(Some(HaveSessionEvent::Cancelled));
            }

            this.state = HaveSessionState::Idle;
            return Poll::Pending;
        }

        loop {
            match &mut this.state {
                HaveSessionState::Idle => {
                    if let Some(have) = this.have {
                        if let Some((peer_id, state)) = this
                            .want
                            .iter_mut()
                            .find(|(_, state)| matches!(state, HaveWantState::Pending { .. }))
                        {
                            let peer_id = *peer_id;
                            *state = HaveWantState::Sent;
                            tracing::debug!(%peer_id, peer_state = ?state, have_block=have, session = %this.cid, "notifying peer of block status");
                            return match have {
                                true => Poll::Ready(Some(HaveSessionEvent::Have { peer_id })),
                                false => Poll::Ready(Some(HaveSessionEvent::DontHave { peer_id })),
                            };
                        }
                    }
                    this.waker.replace(cx.waker().clone());
                    return Poll::Pending;
                }
                HaveSessionState::ContainBlock { fut } => {
                    let have = ready!(fut.poll_unpin(cx)).unwrap_or_default();
                    this.have = Some(have);
                    this.state = HaveSessionState::Idle;
                }
                // Maybe we should have a lock on a single lock to prevent GC from cleaning it up or being removed while waiting for it to be
                // exchanged. This could probably be done through a temporary pin
                HaveSessionState::GetBlock { fut } => {
                    let result = ready!(fut.poll_unpin(cx));
                    let block = match result.as_ref() {
                        Ok(Some(block)) => block.data(),
                        Ok(None) => {
                            tracing::warn!(session = %this.cid, "block does not exist");
                            this.state = HaveSessionState::Idle;
                            this.have = Some(false);
                            continue;
                        }
                        Err(e) => {
                            tracing::error!(session = %this.cid, error = %e, "error obtaining block");
                            this.state = HaveSessionState::Idle;
                            this.have = Some(false);
                            continue;
                        }
                    };

                    // Note: `Bytes` is used to make it cheaper to handle the block data
                    let bytes = Bytes::copy_from_slice(block);
                    // In case we are sent a block request
                    this.have = Some(true);

                    this.state = HaveSessionState::Block { bytes };
                }
                HaveSessionState::Block { bytes } => {
                    // This will kick start the providing process to the peer
                    match this
                        .want
                        .iter_mut()
                        .find(|(_, state)| matches!(state, HaveWantState::Block))
                    {
                        Some((peer_id, state)) => {
                            *state = HaveWantState::BlockSent;
                            return Poll::Ready(Some(HaveSessionEvent::Block {
                                peer_id: *peer_id,
                                bytes: bytes.clone(),
                            }));
                        }
                        None => return Poll::Pending,
                    }
                }
                HaveSessionState::Complete => {
                    // Although this branch is unreachable, we should return `None` in case this is ever reached
                    // though any attempts in continuious polling after this point would be considered as UB
                    return Poll::Ready(None);
                }
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.want.len(), None)
    }
}

impl FusedStream for HaveSession {
    fn is_terminated(&self) -> bool {
        matches!(self.state, HaveSessionState::Complete)
    }
}
