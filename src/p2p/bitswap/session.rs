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

#[derive(Debug)]
pub enum PeerSessionEvent {
    /// Deliver this message to the peer through its handler.
    Send(BitswapMessage),
    /// A wanted block arrived and is stored; the behaviour cancels the want.
    Stored(Cid),
    /// The peer does not have this cid.
    DontHave(Cid),
}

enum RepoOp {
    Stored(Cid),
    Serve {
        request: BitswapRequest,
        block: Option<Block>,
    },
}

pub struct PeerSession {
    wantlist: Wantlist,
    repo: Repo<DefaultStorage>,
    /// cids this peer has signalled HAVE for.
    has: HashSet<Cid>,
    /// wants already sent to this peer, with the request type last sent.
    sent: HashMap<Cid, RequestType>,
    /// cids this peer has requested from us, with the request type they asked for.
    peer_wants: HashMap<Cid, RequestType>,
    outbound: VecDeque<PeerSessionEvent>,
    pending: FuturesUnordered<BoxFuture<'static, RepoOp>>,
    waker: Option<Waker>,
}

impl PeerSession {
    pub fn new(wantlist: Wantlist, repo: Repo<DefaultStorage>) -> Self {
        let mut session = Self {
            wantlist,
            repo,
            has: HashSet::new(),
            sent: HashMap::new(),
            peer_wants: HashMap::new(),
            outbound: VecDeque::new(),
            pending: FuturesUnordered::new(),
            waker: None,
        };
        session.sync();
        session
    }

    /// cids this peer is currently asking us for.
    pub fn peer_wantlist(&self) -> Vec<Cid> {
        self.peer_wants.keys().copied().collect()
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
            let desired = if self.has.contains(cid) {
                RequestType::Block
            } else {
                entry.want_type
            };
            if self.sent.get(cid) == Some(&desired) {
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
            self.has.remove(&cid);
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
            let repo = self.repo.clone();
            self.pending.push(
                async move {
                    let block = repo.get_block_now(cid).await.ok().flatten();
                    RepoOp::Serve { request, block }
                }
                .boxed(),
            );
        }

        for (cid, response) in message.responses {
            match response {
                BitswapResponse::Have(true) => {
                    self.has.insert(cid);
                    self.wantlist.note_have(&cid);
                    if self.wantlist.contains(&cid)
                        && self.sent.get(&cid) != Some(&RequestType::Block)
                    {
                        self.sent.insert(cid, RequestType::Block);
                        self.queue(PeerSessionEvent::Send(
                            BitswapMessage::new(false).add_request(BitswapRequest::block(cid)),
                        ));
                    }
                }
                BitswapResponse::Have(false) => {
                    self.has.remove(&cid);
                    self.queue(PeerSessionEvent::DontHave(cid));
                }
                BitswapResponse::Block(data) => {
                    self.sent.remove(&cid);
                    self.has.remove(&cid);
                    match Block::new(cid, data) {
                        Ok(block) => {
                            let repo = self.repo.clone();
                            self.pending.push(
                                async move {
                                    let _ = repo.put_block(&block).await;
                                    RepoOp::Stored(cid)
                                }
                                .boxed(),
                            );
                        }
                        Err(_) => self.queue(PeerSessionEvent::DontHave(cid)),
                    }
                }
            }
        }

        self.wake();
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
            let repo = self.repo.clone();
            self.pending.push(
                async move {
                    let block = repo.get_block_now(request.cid).await.ok().flatten();
                    RepoOp::Serve { request, block }
                }
                .boxed(),
            );
        }
        self.wake();
    }
}

fn serve_response(request: &BitswapRequest, block: Option<Block>) -> Option<BitswapMessage> {
    let response = match (request.ty, block) {
        (RequestType::Have, Some(_)) => BitswapResponse::Have(true),
        (RequestType::Block, Some(block)) => {
            BitswapResponse::Block(Bytes::copy_from_slice(block.data()))
        }
        (_, None) if request.send_dont_have => BitswapResponse::Have(false),
        (_, None) => return None,
    };
    Some(BitswapMessage::new(false).add_response(request.cid, response))
}

impl Stream for PeerSession {
    type Item = PeerSessionEvent;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        while let Poll::Ready(Some(op)) = this.pending.poll_next_unpin(cx) {
            match op {
                RepoOp::Stored(cid) => this.outbound.push_back(PeerSessionEvent::Stored(cid)),
                RepoOp::Serve { request, block } => {
                    if let Some(message) = serve_response(&request, block) {
                        this.outbound.push_back(PeerSessionEvent::Send(message));
                    }
                }
            }
        }

        if let Some(event) = this.outbound.pop_front() {
            return Poll::Ready(Some(event));
        }

        this.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}
