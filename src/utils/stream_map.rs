use std::{
    pin::Pin,
    task::{Context, Poll, Waker},
};

use futures::{
    stream::{FusedStream, SelectAll},
    Stream,
};

pub struct StreamMap<K, I> {
    stream: SelectAll<InnerStreamMap<K, I>>,
    waker: Option<Waker>,
}

impl<K, I> Default for StreamMap<K, I>
where
    K: Clone + Unpin,
    I: Stream + Unpin,
{
    fn default() -> Self {
        Self {
            stream: Default::default(),
            waker: None,
        }
    }
}

impl<K, I> StreamMap<K, I>
where
    K: Clone + Unpin,
    I: Stream + Unpin,
{
    pub fn new() -> Self {
        Self::default()
    }
}

impl<K, I> StreamMap<K, I>
where
    K: Clone + PartialEq + Send + Unpin + 'static,
    I: Stream + Unpin,
{
    pub fn insert(&mut self, key: K, stream: I) -> Option<I> {
        let item = self.remove(&key);
        self.stream.push(InnerStreamMap::new(key, stream));
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
        item
    }

    pub fn contains_key(&self, key: &K) -> bool {
        self.stream.iter().any(|stream| stream.key.eq(key))
    }

    pub fn iter(&self) -> impl Iterator<Item = (&K, &I)> {
        self.stream
            .iter()
            .map(|inner| (&inner.key, inner.stream.as_ref().expect("valid stream")))
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = (K, &mut I)> {
        self.stream.iter_mut().map(|inner| {
            (
                inner.key.clone(),
                inner.stream.as_mut().expect("valid stream"),
            )
        })
    }

    pub fn keys(&self) -> impl Iterator<Item = &K> {
        self.stream.iter().map(|stream| &stream.key)
    }

    #[allow(dead_code)]
    pub fn values(&self) -> impl Iterator<Item = &I> {
        self.stream.iter().filter_map(|inner| inner.stream.as_ref())
    }

    pub fn values_mut(&mut self) -> impl Iterator<Item = &mut I> {
        self.stream
            .iter_mut()
            .filter_map(|inner| inner.stream.as_mut())
    }

    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.stream.len()
    }

    pub fn is_empty(&self) -> bool {
        self.stream.is_empty()
    }

    #[allow(dead_code)]
    pub fn get(&self, key: &K) -> Option<&I> {
        let inner = self.stream.iter().find(|s| s.key == *key)?;
        inner.stream.as_ref()
    }

    pub fn get_mut(&mut self, key: &K) -> Option<&mut I> {
        let inner = self.stream.iter_mut().find(|s| s.key == *key)?;
        inner.stream.as_mut()
    }

    pub fn remove(&mut self, key: &K) -> Option<I> {
        let inner_stream = self.stream.iter_mut().find(|s| s.key == *key)?;
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
        inner_stream.inner_stream_mut().take()
    }
}

impl<K, I> Stream for StreamMap<K, I>
where
    K: Clone + PartialEq + Send + Unpin + 'static,
    I: Stream + Unpin,
{
    type Item = (K, I::Item);
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        match Pin::new(&mut this.stream).poll_next(cx) {
            Poll::Ready(Some((key, item))) => return Poll::Ready(Some((key, item))),
            Poll::Ready(None) | Poll::Pending => {
                // Note: In normal streams, `Poll::Ready(None)` would indicate an end or termination of a stream
                //       however, since `SelectAll` may be empty, it would likely return such value, so instead of
                //       passing it on, we will treat it as `Poll::Pending` since the stream may be available later
                this.waker.replace(cx.waker().clone());
            }
        }

        Poll::Pending
    }
}

struct InnerStreamMap<K, S> {
    key: K,
    stream: Option<S>,
}

impl<K, S> InnerStreamMap<K, S> {
    pub fn new(key: K, stream: S) -> Self {
        Self {
            key,
            stream: Some(stream),
        }
    }

    pub fn inner_stream_mut(&mut self) -> &mut Option<S> {
        &mut self.stream
    }
}

impl<K, S> Stream for InnerStreamMap<K, S>
where
    K: Clone + Unpin,
    S: Stream + Unpin,
{
    type Item = (K, S::Item);
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let Some(stream) = self.stream.as_mut() else {
            return Poll::Ready(None);
        };

        match Pin::new(stream).poll_next(cx) {
            Poll::Ready(Some(item)) => Poll::Ready(Some((self.key.clone(), item))),
            Poll::Ready(None) => {
                self.stream.take();
                Poll::Ready(None)
            }
            Poll::Pending => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
}

impl<K, S> FusedStream for InnerStreamMap<K, S>
where
    K: Clone + Unpin,
    S: Stream + Unpin,
{
    fn is_terminated(&self) -> bool {
        self.stream.is_none()
    }
}
