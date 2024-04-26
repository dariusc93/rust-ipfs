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
use libipld::Cid;
use libp2p::PeerId;
use std::fmt::Debug;

use crate::{repo::Repo, Block};

const CAP_THRESHOLD: usize = 100;

pub enum WantSessionEvent {
    SendWant { peer_id: PeerId },
    SendCancels { peers: VecDeque<PeerId> },
    SendBlock { peer_id: PeerId },
    BlockStored,
    NeedBlock,
}

pub enum WantSessionState {
    Idle,
    NextBlock,
    NextBlockPending {
        timer: Delay,
    },
    PutBlock {
        fut: BoxFuture<'static, Result<Cid, anyhow::Error>>,
    },
    Complete,
}

impl Debug for WantSessionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SessionState")
    }
}

#[derive(Debug)]
pub struct WantSession {
    cid: Cid,
    sending_wants: VecDeque<PeerId>,
    sent_wants: VecDeque<PeerId>,
    have_block: VecDeque<PeerId>,
    failed_block: VecDeque<PeerId>,
    sent_have_block: Option<PeerId>,
    requested_block: bool,
    received: bool,
    waker: Option<Waker>,
    repo: Repo,
    state: WantSessionState,
    timeout: Option<Duration>,
}

impl WantSession {
    pub fn new(repo: &Repo, cid: Cid) -> Self {
        Self {
            cid,
            sending_wants: Default::default(),
            sent_wants: Default::default(),
            have_block: Default::default(),
            sent_have_block: Default::default(),
            failed_block: Default::default(),
            received: false,
            requested_block: false,
            repo: repo.clone(),
            waker: None,
            state: WantSessionState::Idle,
            timeout: None,
        }
    }

    pub fn send_have_block(&mut self, peer_id: PeerId) {
        if !self.sent_wants.contains(&peer_id)
            && !self.sending_wants.contains(&peer_id)
            && !self.have_block.contains(&peer_id)
        {
            tracing::trace!(session = %self.cid, %peer_id, name = "want_session", "send have block");
            self.requested_block = false;
            self.sending_wants.push_back(peer_id);
            if let Some(w) = self.waker.take() {
                w.wake();
            }
        }
    }

    pub fn has_block(&mut self, peer_id: PeerId) {
        tracing::debug!(session = %self.cid, %peer_id, name = "want_session", "have block");
        if !self.have_block.contains(&peer_id) {
            self.have_block.push_back(peer_id);
        }

        self.sending_wants.retain(|pid| *pid != peer_id);
        self.sent_wants.retain(|pid| *pid != peer_id);

        if !matches!(self.state, WantSessionState::NextBlock) {
            tracing::debug!(session = %self.cid, %peer_id, name = "want_session", "change state to next_block");
            self.state = WantSessionState::NextBlock;
        }

        if let Some(w) = self.waker.take() {
            w.wake();
        }
    }

    pub fn dont_have_block(&mut self, peer_id: PeerId) {
        tracing::trace!(session = %self.cid, %peer_id, name = "want_session", "dont have block");
        self.sending_wants.retain(|pid| *pid != peer_id);
        self.sent_wants.retain(|pid| *pid != peer_id);

        if !self.is_empty()
            && !matches!(
                self.state,
                WantSessionState::NextBlock | WantSessionState::NextBlockPending { .. }
            )
        {
            tracing::warn!(session = %self.cid, %peer_id, name = "want_session", "session is empty");
            return;
        }

        // change state to next block so it will perform another request if possible,
        // otherwise notify swarm if no request been sent
        tracing::debug!(session = %self.cid, %peer_id, name = "want_session", "next_block state");
        self.state = WantSessionState::NextBlock;
        if let Some(w) = self.waker.take() {
            w.wake();
        }
    }

    pub fn put_block(&mut self, peer_id: PeerId, block: Block) {
        if matches!(self.state, WantSessionState::PutBlock { .. }) {
            tracing::warn!(session = %self.cid, %peer_id, cid = %block.cid(), name = "want_session", "state already putting block into store");
            return;
        }

        tracing::info!(%peer_id, cid = %block.cid(), name = "want_session", "storing block");
        let fut = self.repo.put_block(block).into_future();
        self.state = WantSessionState::PutBlock { fut };

        if let Some(w) = self.waker.take() {
            w.wake();
        }
    }

    pub fn remove_peer(&mut self, peer_id: PeerId) {
        if self.is_empty() {
            return;
        }
        // tracing::debug!(session = %self.cid, %peer_id, name = "want_session", "removing peer from want_session");
        self.sending_wants.retain(|pid| *pid != peer_id);
        self.sent_wants.retain(|pid| *pid != peer_id);
        self.have_block.retain(|pid| *pid != peer_id);
        if matches!(self.sent_have_block, Some(p) if p == peer_id) {
            self.sent_have_block.take();
            self.state = WantSessionState::NextBlock;
        }
        if let Some(w) = self.waker.take() {
            w.wake();
        }
    }

    #[allow(dead_code)]
    fn is_empty(&self) -> bool {
        self.sending_wants.is_empty() && self.sent_wants.is_empty() && self.have_block.is_empty()
    }
}

impl Unpin for WantSession {}

impl Stream for WantSession {
    type Item = WantSessionEvent;

    #[tracing::instrument(level = "trace", name = "Session::poll_next", skip(self, cx))]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.received {
            return Poll::Ready(None);
        }

        if !matches!(self.state, WantSessionState::Complete) {
            if let Some(peer_id) = self.sending_wants.pop_front() {
                self.sent_wants.push_back(peer_id);
                tracing::debug!(session = %self.cid, %peer_id, name = "want_session", "sent want block");
                return Poll::Ready(Some(WantSessionEvent::SendWant { peer_id }));
            } else if self.sending_wants.capacity() > CAP_THRESHOLD {
                self.sending_wants.shrink_to_fit()
            }
        }

        loop {
            match &mut self.state {
                WantSessionState::Idle => {
                    self.waker = Some(cx.waker().clone());
                    return Poll::Pending;
                }
                WantSessionState::NextBlock => {
                    if let Some(peer_id) = self.sent_have_block.take() {
                        tracing::debug!(session = %self.cid, %peer_id, name = "want_session", "failed block");
                        self.failed_block.push_back(peer_id);
                    }

                    if let Some(next_peer_id) = self.have_block.pop_front() {
                        tracing::info!(session = %self.cid, %next_peer_id, name = "want_session", "sending block request to next peer");
                        self.requested_block = false;
                        self.sent_have_block = Some(next_peer_id);
                        let timeout = match self.timeout {
                            Some(timeout) if !timeout.is_zero() => timeout,
                            _ => Duration::from_secs(15),
                        };
                        let timer = Delay::new(timeout);
                        self.state = WantSessionState::NextBlockPending { timer };

                        return Poll::Ready(Some(WantSessionEvent::SendBlock {
                            peer_id: next_peer_id,
                        }));
                    }

                    tracing::debug!(session = %self.cid, name = "want_session", "session is idle");
                    self.state = WantSessionState::Idle;

                    if self.is_empty() && !self.requested_block {
                        self.requested_block = true;
                        return Poll::Ready(Some(WantSessionEvent::NeedBlock));
                    }
                }
                WantSessionState::NextBlockPending { timer } => {
                    ready!(timer.poll_unpin(cx));
                    tracing::warn!(session = %self.cid, name = "want_session", "request timeout attempting to get next block");
                    self.state = WantSessionState::NextBlock;
                }
                WantSessionState::PutBlock { fut } => match ready!(fut.poll_unpin(cx)) {
                    Ok(cid) => {
                        tracing::info!(session = %self.cid, block = %cid, name = "want_session", "block stored in block store");
                        self.state = WantSessionState::Complete;

                        cx.waker().wake_by_ref();
                        return Poll::Ready(Some(WantSessionEvent::BlockStored));
                    }
                    Err(e) => {
                        tracing::error!(session = %self.cid, error = %e, name = "want_session", "error storing block in store");
                        self.state = WantSessionState::NextBlock;
                    }
                },
                WantSessionState::Complete => {
                    self.received = true;
                    let mut peers = HashSet::new();
                    // although this should be empty by the time we reach here, its best to clear it anyway since nothing was ever sent
                    self.sending_wants.clear();

                    peers.extend(std::mem::take(&mut self.sent_wants));
                    peers.extend(std::mem::take(&mut self.have_block));
                    peers.extend(std::mem::take(&mut self.failed_block));

                    if let Some(peer_id) = self.sent_have_block.take() {
                        peers.insert(peer_id);
                    };

                    let peers = VecDeque::from_iter(peers);

                    tracing::info!(session = %self.cid, pending_cancellation = peers.len());

                    return Poll::Ready(Some(WantSessionEvent::SendCancels { peers }));
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

enum HaveWantState {
    Pending,
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
            send_dont_have: HashSet::new(),
            have: None,
            repo: repo.clone(),
            waker: None,
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
            tracing::warn!(session = %self.cid, %peer_id, "peer requested block");
            return;
        }

        tracing::info!(session = %self.cid, %peer_id, name = "have_session", "peer want block");

        if send_dont_have {
            self.send_dont_have.insert(peer_id);
        }
        self.want.insert(peer_id, HaveWantState::Pending);

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

        for state in self.want.values_mut() {
            *state = HaveWantState::Pending;
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
                            .find(|(_, state)| matches!(state, HaveWantState::Pending))
                        {
                            let peer_id = *peer_id;
                            *state = HaveWantState::Sent;

                            return match have {
                                true => Poll::Ready(Some(HaveSessionEvent::Have { peer_id })),
                                false => Poll::Ready(Some(HaveSessionEvent::DontHave { peer_id })),
                            };
                        }
                    }
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
                        _ => {
                            this.state = HaveSessionState::Idle;
                            this.have = Some(false);
                            continue;
                        }
                    };
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
