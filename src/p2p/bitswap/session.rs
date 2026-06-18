//! Per-peer bitswap session for the peer-keyed rewrite.

use std::collections::{HashMap, HashSet, VecDeque};
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, Stream, StreamExt};
use ipld_core::cid::Cid;

use super::message::{BitswapMessage, BitswapRequest, BitswapResponse, RequestType};
use super::wantlist::Wantlist;
use crate::Block;
use crate::repo::{DefaultStorage, Repo};

const MAX_INFLIGHT_SERVES: usize = 32;
const SERVE_BATCH_LIMIT: usize = 1 << 20;

#[derive(Debug)]
pub enum PeerSessionEvent {
    /// Deliver this message to the peer through its handler.
    Send(BitswapMessage),
    Have(Cid),
    /// A wanted block arrived and is stored; the behaviour cancels the want.
    Stored(Cid),
    /// The peer does not have this cid.
    DontHave(Cid),
}

#[derive(Debug, Default, Clone, Copy)]
pub struct Ledger {
    pub blocks_sent: u64,
    pub bytes_sent: u64,
    pub blocks_recv: u64,
    pub bytes_recv: u64,
}

pub struct PeerSession {
    wantlist: Wantlist,
    repo: Repo<DefaultStorage>,
    /// wants already sent to this peer, with the request type last sent.
    sent: HashMap<Cid, RequestType>,
    /// cids this peer has requested from us, with the request type they asked for.
    peer_wants: HashMap<Cid, RequestType>,
    outbound: VecDeque<PeerSessionEvent>,
    serve_backlog: VecDeque<BitswapRequest>,
    backlog_set: HashSet<Cid>,
    serving: FuturesUnordered<BoxFuture<'static, (BitswapRequest, Option<Block>)>>,
    storing: FuturesUnordered<BoxFuture<'static, Cid>>,
    ledger: Ledger,
    waker: Option<Waker>,
}

impl PeerSession {
    pub fn new(wantlist: Wantlist, repo: Repo<DefaultStorage>) -> Self {
        let mut session = Self {
            wantlist,
            repo,
            sent: HashMap::new(),
            peer_wants: HashMap::new(),
            outbound: VecDeque::new(),
            serve_backlog: VecDeque::new(),
            backlog_set: HashSet::new(),
            serving: FuturesUnordered::new(),
            storing: FuturesUnordered::new(),
            ledger: Ledger::default(),
            waker: None,
        };
        session.sync();
        session
    }

    /// cids this peer is currently asking us for.
    pub fn peer_wantlist(&self) -> Vec<Cid> {
        self.peer_wants.keys().copied().collect()
    }

    pub fn ledger(&self) -> Ledger {
        self.ledger
    }

    pub fn request_block(&mut self, cid: Cid) -> Option<BitswapMessage> {
        if self.sent.get(&cid) == Some(&RequestType::Block) {
            return None;
        }
        self.sent.insert(cid, RequestType::Block);
        Some(BitswapMessage::new(false).add_request(BitswapRequest::block(cid).send_dont_have(true)))
    }

    pub fn reset_block(&mut self, cid: Cid) {
        self.sent.remove(&cid);
    }

    fn wake(&mut self) {
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }

    fn queue(&mut self, event: PeerSessionEvent) {
        self.outbound.push_back(event);
        self.wake();
    }

    /// Reconcile what we have sent this peer against the shared wantlist, queueing wants and cancels.
    pub fn sync(&mut self) {
        let entries = self.wantlist.entries();
        let wanted: HashSet<Cid> = entries.iter().map(|(cid, _)| *cid).collect();
        let mut requests = Vec::new();

        for (cid, entry) in &entries {
            if self.sent.get(cid) == Some(&RequestType::Block) {
                continue;
            }
            let desired = entry.want_type;
            if self.sent.get(cid) == Some(&desired) {
                continue;
            }
            if desired == RequestType::Have && entry.has_provider {
                continue;
            }
            let request = match desired {
                RequestType::Have => BitswapRequest::have(*cid),
                RequestType::Block => BitswapRequest::block(*cid),
            }
            .send_dont_have(true)
            .set_priority(entry.priority);
            requests.push(request);
            self.sent.insert(*cid, desired);
        }

        let canceled: Vec<Cid> = self
            .sent
            .keys()
            .filter(|cid| !wanted.contains(cid))
            .copied()
            .collect();
        for cid in canceled {
            requests.push(BitswapRequest::cancel(cid));
            self.sent.remove(&cid);
        }

        if !requests.is_empty() {
            self.queue(PeerSessionEvent::Send(
                BitswapMessage::new(false).set_requests(requests),
            ));
        }
    }

    pub fn on_message(&mut self, message: BitswapMessage) {
        for request in message.requests {
            let cid = request.cid;
            if request.cancel {
                self.peer_wants.remove(&cid);
                continue;
            }
            self.peer_wants.insert(cid, request.ty);
            self.enqueue_serve(request);
        }

        for (cid, response) in message.responses {
            match response {
                BitswapResponse::Have(true) => {
                    self.wantlist.note_have(&cid);
                    if self.wantlist.contains(&cid) {
                        self.queue(PeerSessionEvent::Have(cid));
                    }
                }
                BitswapResponse::Have(false) => {
                    self.sent.remove(&cid);
                    self.queue(PeerSessionEvent::DontHave(cid));
                }
                BitswapResponse::Block(data) => {
                    self.sent.remove(&cid);
                    if !self.wantlist.contains(&cid) {
                        continue;
                    }
                    self.ledger.blocks_recv += 1;
                    self.ledger.bytes_recv += data.len() as u64;
                    match Block::new(cid, data) {
                        Ok(block) => {
                            let repo = self.repo.clone();
                            self.storing.push(
                                async move {
                                    let _ = repo.put_block(&block).await;
                                    cid
                                }
                                .boxed(),
                            );
                        }
                        Err(_) => self.queue(PeerSessionEvent::DontHave(cid)),
                    }
                }
            }
        }

        self.drain_serves();
        self.wake();
    }

    fn enqueue_serve(&mut self, request: BitswapRequest) {
        if self.backlog_set.insert(request.cid) {
            self.serve_backlog.push_back(request);
        }
    }

    fn drain_serves(&mut self) {
        while self.serving.len() < MAX_INFLIGHT_SERVES {
            let Some(mut request) = self.serve_backlog.pop_front() else {
                break;
            };
            self.backlog_set.remove(&request.cid);
            let Some(&ty) = self.peer_wants.get(&request.cid) else {
                continue;
            };
            request.ty = ty;
            let cid = request.cid;
            let repo = self.repo.clone();
            self.serving.push(
                async move {
                    let block = repo.get_block_now(cid).await.ok().flatten();
                    (request, block)
                }
                .boxed(),
            );
        }
    }

    /// Re-serve cids this peer previously asked for, now that we may hold them.
    pub fn serve_wanted(&mut self, cids: &[Cid]) {
        for cid in cids {
            let Some(&ty) = self.peer_wants.get(cid) else {
                continue;
            };
            let request = match ty {
                RequestType::Have => BitswapRequest::have(*cid),
                RequestType::Block => BitswapRequest::block(*cid),
            };
            self.enqueue_serve(request);
        }
        self.drain_serves();
        self.wake();
    }
}

fn serve_response(request: &BitswapRequest, block: Option<Block>) -> Option<(Cid, BitswapResponse)> {
    let response = match (request.ty, block) {
        (RequestType::Have, Some(_)) => BitswapResponse::Have(true),
        (RequestType::Block, Some(block)) => {
            BitswapResponse::Block(Bytes::copy_from_slice(block.data()))
        }
        (_, None) if request.send_dont_have => BitswapResponse::Have(false),
        (_, None) => return None,
    };
    Some((request.cid, response))
}

fn response_size(response: &BitswapResponse) -> usize {
    match response {
        BitswapResponse::Block(data) => data.len() + 64,
        BitswapResponse::Have(_) => 64,
    }
}

impl Stream for PeerSession {
    type Item = PeerSessionEvent;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        this.drain_serves();
        let mut batch: Vec<(Cid, BitswapResponse)> = Vec::new();
        let mut batch_size = 0;
        while let Poll::Ready(Some((request, block))) = this.serving.poll_next_unpin(cx) {
            if request.ty == RequestType::Block
                && let Some(block) = &block
            {
                this.ledger.blocks_sent += 1;
                this.ledger.bytes_sent += block.data().len() as u64;
            }
            if let Some((cid, response)) = serve_response(&request, block) {
                let size = response_size(&response);
                if !batch.is_empty() && batch_size + size > SERVE_BATCH_LIMIT {
                    this.outbound.push_back(PeerSessionEvent::Send(
                        BitswapMessage::new(false).set_responses(std::mem::take(&mut batch)),
                    ));
                    batch_size = 0;
                }
                batch.push((cid, response));
                batch_size += size;
            }
            this.drain_serves();
        }
        if !batch.is_empty() {
            this.outbound.push_back(PeerSessionEvent::Send(
                BitswapMessage::new(false).set_responses(batch),
            ));
        }

        while let Poll::Ready(Some(cid)) = this.storing.poll_next_unpin(cx) {
            this.outbound.push_back(PeerSessionEvent::Stored(cid));
        }

        if let Some(event) = this.outbound.pop_front() {
            return Poll::Ready(Some(event));
        }

        this.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use multihash_codetable::{Code, MultihashDigest};

    fn test_cid() -> Cid {
        Cid::new_v1(0x55, Code::Sha2_256.digest(b"sync gate"))
    }

    #[tokio::test]
    async fn sync_gates_want_have_when_provider_known() {
        let repo = Repo::new_memory();
        let cid = test_cid();

        let wantlist = Wantlist::default();
        wantlist.want(cid, RequestType::Have, 1, None);
        let mut session = PeerSession::new(wantlist, repo.clone());
        assert!(
            matches!(session.next().await, Some(PeerSessionEvent::Send(_))),
            "an unprovided want should be solicited"
        );

        let wantlist = Wantlist::default();
        wantlist.want(cid, RequestType::Have, 1, None);
        wantlist.note_have(&cid);
        let mut session = PeerSession::new(wantlist, repo);
        let polled = futures::poll!(std::pin::pin!(session.next()));
        assert!(
            polled.is_pending(),
            "a want with a known provider must not be re-solicited"
        );
    }

    #[tokio::test]
    async fn serves_coalesce_into_fewer_messages() {
        let repo = Repo::new_memory();
        let mut cids = Vec::new();
        for i in 0..8u32 {
            let data = format!("batch block {i}").into_bytes();
            let cid = Cid::new_v1(0x55, Code::Sha2_256.digest(&data));
            repo.put_block(&Block::new_unchecked(cid, data)).await.unwrap();
            cids.push(cid);
        }

        let mut session = PeerSession::new(Wantlist::default(), repo);
        let requests = cids.iter().map(|cid| BitswapRequest::block(*cid)).collect();
        session.on_message(BitswapMessage::new(false).set_requests(requests));

        let mut messages = 0;
        let mut served = 0;
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(500);
        while let Ok(Some(event)) = tokio::time::timeout_at(deadline, session.next()).await {
            if let PeerSessionEvent::Send(message) = event {
                messages += 1;
                served += message.responses.len();
            }
        }
        assert_eq!(served, 8, "all requested blocks served");
        assert!(messages < 8, "serves should coalesce, got {messages} messages");
    }
}
