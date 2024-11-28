#[allow(unused_imports)]
use futures::future::Abortable;
use std::fmt::{Debug, Formatter};

use futures::future::{AbortHandle, Aborted};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

pub struct JoinHandle<T> {
    inner: InnerJoinHandle<T>,
}

impl<T> Debug for JoinHandle<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JoinHandle").finish()
    }
}

enum InnerJoinHandle<T> {
    #[cfg(not(target_arch = "wasm32"))]
    TokioHandle(tokio::task::JoinHandle<T>),
    #[allow(dead_code)]
    CustomHandle {
        inner: Option<futures::channel::oneshot::Receiver<Result<T, Aborted>>>,
        handle: AbortHandle,
    },
    Empty,
}

impl<T> Default for InnerJoinHandle<T> {
    fn default() -> Self {
        Self::Empty
    }
}

impl<T> JoinHandle<T> {
    pub(crate) fn empty() -> Self {
        JoinHandle {
            inner: InnerJoinHandle::Empty,
        }
    }
}

impl<T> JoinHandle<T> {
    #[allow(dead_code)]
    pub fn abort(&self) {
        match self.inner {
            #[cfg(not(target_arch = "wasm32"))]
            InnerJoinHandle::TokioHandle(ref handle) => handle.abort(),
            InnerJoinHandle::CustomHandle { ref handle, .. } => handle.abort(),
            InnerJoinHandle::Empty => {}
        }
    }

    #[allow(dead_code)]
    pub fn is_finished(&self) -> bool {
        match self.inner {
            #[cfg(not(target_arch = "wasm32"))]
            InnerJoinHandle::TokioHandle(ref handle) => handle.is_finished(),
            InnerJoinHandle::CustomHandle {
                ref handle,
                ref inner,
            } => handle.is_aborted() || inner.is_none(),
            InnerJoinHandle::Empty => true,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn replace(&mut self, mut handle: JoinHandle<T>) {
        self.inner = std::mem::take(&mut handle.inner);
    }

    #[allow(dead_code)]
    pub(crate) fn replace_mut(&mut self, handle: &mut JoinHandle<T>) {
        self.inner = std::mem::take(&mut handle.inner);
    }
}

impl<T> Future for JoinHandle<T> {
    type Output = std::io::Result<T>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let inner = &mut self.inner;
        match inner {
            #[cfg(not(target_arch = "wasm32"))]
            InnerJoinHandle::TokioHandle(handle) => {
                let fut = futures::ready!(Pin::new(handle).poll(cx));

                match fut {
                    Ok(val) => Poll::Ready(Ok(val)),
                    Err(e) => {
                        let e = std::io::Error::other(e);
                        Poll::Ready(Err(e))
                    }
                }
            }
            InnerJoinHandle::CustomHandle { inner, .. } => {
                let Some(this) = inner.as_mut() else {
                    unreachable!("cannot poll completed future");
                };

                let fut = futures::ready!(Pin::new(this).poll(cx));
                inner.take();

                match fut {
                    Ok(Ok(val)) => Poll::Ready(Ok(val)),
                    Ok(Err(e)) => {
                        let e = std::io::Error::other(e);
                        Poll::Ready(Err(e))
                    }
                    Err(e) => {
                        let e = std::io::Error::other(e);
                        Poll::Ready(Err(e))
                    }
                }
            }
            InnerJoinHandle::Empty => {
                Poll::Ready(Err(std::io::Error::from(std::io::ErrorKind::Other)))
            }
        }
    }
}

#[derive(Clone)]
pub struct AbortableJoinHandle<T> {
    handle: Arc<InnerHandle<T>>,
}

impl<T> Debug for AbortableJoinHandle<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AbortableJoinHandle").finish()
    }
}

impl<T> From<JoinHandle<T>> for AbortableJoinHandle<T> {
    fn from(handle: JoinHandle<T>) -> Self {
        AbortableJoinHandle {
            handle: Arc::new(InnerHandle {
                inner: parking_lot::Mutex::new(handle),
            }),
        }
    }
}

impl<T> AbortableJoinHandle<T> {
    pub(crate) fn empty() -> Self {
        Self {
            handle: Arc::new(InnerHandle {
                inner: parking_lot::Mutex::new(JoinHandle::empty()),
            }),
        }
    }
}

impl<T> AbortableJoinHandle<T> {
    #[allow(dead_code)]
    pub fn abort(&self) {
        self.handle.inner.lock().abort();
    }

    #[allow(dead_code)]
    pub fn is_finished(&self) -> bool {
        self.handle.inner.lock().is_finished()
    }

    pub(crate) fn replace(&mut self, inner: AbortableJoinHandle<T>) {
        let current_handle = &mut *self.handle.inner.lock();
        let inner_handle = &mut *inner.handle.inner.lock();
        current_handle.replace_mut(inner_handle);
    }
}

struct InnerHandle<T> {
    pub inner: parking_lot::Mutex<JoinHandle<T>>,
}

impl<T> Drop for InnerHandle<T> {
    fn drop(&mut self) {
        self.inner.lock().abort();
    }
}

impl<T> Future for AbortableJoinHandle<T> {
    type Output = std::io::Result<T>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let inner = &mut *self.handle.inner.lock();
        Pin::new(inner).poll(cx).map_err(std::io::Error::other)
    }
}

pub trait Executor {
    /// Spawns a new asynchronous task in the background, returning an Future ['JoinHandle'] for it.
    fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static;

    /// Spawns a new asynchronous task in the background, returning an abortable handle that will cancel the task
    /// once the handle is dropped.
    ///
    /// Note: This function is used if the task is expected to run until the handle is dropped. It is recommended to use
    /// [`Executor::spawn`] or [`Executor::dispatch`] otherwise.
    fn spawn_abortable<F>(&self, future: F) -> AbortableJoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let handle = self.spawn(future);
        handle.into()
    }

    /// Spawns a new asynchronous task in the background without an handle.
    /// Basically the same as [`Executor::spawn`].
    fn dispatch<F>(&self, future: F)
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.spawn(future);
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone, Copy, Debug, PartialOrd, PartialEq, Eq)]
pub struct TokioExecutor;

#[cfg(not(target_arch = "wasm32"))]
impl Executor for TokioExecutor {
    fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let handle = tokio::task::spawn(future);
        let inner = InnerJoinHandle::TokioHandle(handle);
        JoinHandle { inner }
    }
}

#[cfg(target_arch = "wasm32")]
#[derive(Clone, Copy, Debug, PartialOrd, PartialEq, Eq)]
pub struct WasmExecutor;

#[cfg(target_arch = "wasm32")]
impl Executor for WasmExecutor {
    fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let (abort_handle, abort_registration) = AbortHandle::new_pair();
        let future = Abortable::new(future, abort_registration);
        let (tx, rx) = futures::channel::oneshot::channel();
        let fut = async {
            let val = future.await;
            _ = tx.send(val);
        };

        wasm_bindgen_futures::spawn_local(fut);
        let inner = InnerJoinHandle::CustomHandle {
            inner: Some(rx),
            handle: abort_handle,
        };
        JoinHandle { inner }
    }
}

#[derive(Clone, Copy, Debug, PartialOrd, PartialEq, Eq)]
pub struct ExecutorSwitch;

impl Executor for ExecutorSwitch {
    fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        #[cfg(not(target_arch = "wasm32"))]
        let executor = TokioExecutor;
        #[cfg(target_arch = "wasm32")]
        let executor = WasmExecutor;

        executor.spawn(future)
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn default_abortable_task() {
    let executor = ExecutorSwitch;

    let (tx, rx) = futures::channel::oneshot::channel::<()>();

    let handle = executor.spawn_abortable(async {
        futures_timer::Delay::new(std::time::Duration::from_secs(5)).await;
        let _ = tx.send(());
        unreachable!();
    });

    drop(handle);
    let result = rx.await;
    assert!(result.is_err());
}

#[test]
fn custom_abortable_task() {
    use futures::future::Abortable;
    struct FuturesExecutor {
        pool: futures::executor::ThreadPool,
    }

    impl Default for FuturesExecutor {
        fn default() -> Self {
            Self {
                pool: futures::executor::ThreadPool::new().unwrap(),
            }
        }
    }

    impl Executor for FuturesExecutor {
        fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
        where
            F: Future + Send + 'static,
            F::Output: Send + 'static,
        {
            let (abort_handle, abort_registration) = AbortHandle::new_pair();
            let future = Abortable::new(future, abort_registration);
            let (tx, rx) = futures::channel::oneshot::channel();
            let fut = async {
                let val = future.await;
                let _ = tx.send(val);
            };

            self.pool.spawn_ok(fut);
            let inner = InnerJoinHandle::CustomHandle {
                inner: Some(rx),
                handle: abort_handle,
            };

            JoinHandle { inner }
        }
    }

    futures::executor::block_on(async move {
        let executor = FuturesExecutor::default();

        let (tx, rx) = futures::channel::oneshot::channel::<()>();

        let handle = executor.spawn_abortable(async {
            futures_timer::Delay::new(std::time::Duration::from_secs(5)).await;
            let _ = tx.send(());
            unreachable!();
        });

        drop(handle);
        let result = rx.await;
        assert!(result.is_err());
    });
}
