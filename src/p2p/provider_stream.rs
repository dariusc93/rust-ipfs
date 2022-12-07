use std::{
    collections::HashSet,
    pin::Pin,
    task::{Context, Poll},
};

use futures::{channel::mpsc::UnboundedReceiver, stream::FusedStream, Stream, StreamExt};
use libp2p::PeerId;

#[derive(Debug)]
pub struct ProviderStream {
    finished: bool,
    rx: UnboundedReceiver<HashSet<PeerId>>,
}

impl ProviderStream {
    pub fn new(rx: UnboundedReceiver<HashSet<PeerId>>) -> Self {
        Self {
            finished: false,
            rx,
        }
    }
}

impl Stream for ProviderStream {
    type Item = HashSet<PeerId>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.rx).poll_next_unpin(cx) {
            Poll::Ready(None) => {
                self.finished = true;
                Poll::Ready(None)
            }
            next => next,
        }
    }
}

impl FusedStream for ProviderStream {
    fn is_terminated(&self) -> bool {
        self.finished
    }
}
