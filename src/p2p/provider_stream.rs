use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures::{channel::mpsc::UnboundedReceiver, Stream, StreamExt, stream::FusedStream};
use libp2p::PeerId;

#[derive(Debug)]
pub struct ProviderStream {
    finished: bool,
    rx: UnboundedReceiver<PeerId>,
}

impl ProviderStream {
    pub fn new(rx: UnboundedReceiver<PeerId>) -> Self {
        Self {
            finished: false,
            rx
        }
    }
}

impl Stream for ProviderStream {
    type Item = PeerId;

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