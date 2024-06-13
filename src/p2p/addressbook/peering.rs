use futures::Stream;
use futures_timer::Delay;
use libp2p::PeerId;
use rand::Rng;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

const MAX_BACKOFF: Duration = Duration::from_secs(5 * 60);
const WATERMARK_BOFF: u64 = 5;
const DELAY: Duration = Duration::from_secs(5);

pub struct Peering {
    peer_id: PeerId,
    delay: Option<Delay>,
    next_backoff: Duration,
    connected: bool,
}

impl Peering {
    pub fn new(peer_id: PeerId, initial: Option<Duration>) -> Self {
        let duration = initial.unwrap_or(DELAY);
        Self {
            peer_id,
            delay: Some(Delay::new(duration)),
            next_backoff: duration,
            connected: false,
        }
    }

    pub fn peer_id(&self) -> &PeerId {
        &self.peer_id
    }

    pub fn connected(&mut self) {
        self.connected = true;
        self.delay = None;
    }

    pub fn disconnected(&mut self) {
        let backoff = self.next_backoff();
        self.connected = false;
        self.delay.replace(Delay::new(backoff));
    }

    pub fn connection_failed(&mut self) {
        let backoff = self.next_backoff();
        self.delay.replace(Delay::new(backoff));
    }

    fn next_backoff(&mut self) -> Duration {
        let mut rng = rand::thread_rng();
        if self.next_backoff < MAX_BACKOFF {
            self.next_backoff += (self.next_backoff / 2)
                + Duration::from_secs(rng.gen_range(0..self.next_backoff.as_secs()));
        }

        if self.next_backoff >= MAX_BACKOFF {
            self.next_backoff = MAX_BACKOFF;
            let val = rng.gen_range(0..(MAX_BACKOFF.as_secs() * WATERMARK_BOFF / 100));
            self.next_backoff -= Duration::from_secs(val);
        }

        self.next_backoff
    }
}

impl Stream for Peering {
    type Item = PeerId;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let Some(timer) = self.delay.as_mut() else {
            return Poll::Pending;
        };

        futures::ready!(Pin::new(timer).poll(cx));
        return Poll::Ready(Some(self.peer_id));
    }
}
