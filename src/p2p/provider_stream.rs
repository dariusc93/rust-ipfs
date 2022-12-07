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
    providers: HashSet<PeerId>,
}

impl ProviderStream {
    pub fn new(rx: UnboundedReceiver<HashSet<PeerId>>) -> Self {
        Self {
            providers: Default::default(),
            finished: false,
            rx,
        }
    }
}

impl Stream for ProviderStream {
    type Item = HashSet<PeerId>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.rx).poll_next_unpin(cx) {
            Poll::Ready(Some(providers)) => {
                let providers = providers
                    .difference(&self.providers)
                    .copied()
                    .collect::<HashSet<_>>();

                self.providers.extend(providers.clone());
                Poll::Ready(Some(providers))
            }
            Poll::Ready(None) => {
                self.finished = true;
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl FusedStream for ProviderStream {
    fn is_terminated(&self) -> bool {
        self.finished
    }
}
