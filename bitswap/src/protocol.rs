use crate::error::BitswapError;
/// Reperesents a prototype for an upgrade to handle the bitswap protocol.
///
/// The protocol works the following way:
///
/// - TODO
use crate::ledger::Message;
use core::iter;
use futures::{
    future::BoxFuture,
    io::{AsyncRead, AsyncWrite},
    AsyncWriteExt,
};
use libp2p_core::{upgrade, InboundUpgrade, OutboundUpgrade, UpgradeInfo};
use std::io;

const MAX_BUF_SIZE: usize = 2_097_152;

type FutureResult<T, E> = BoxFuture<'static, Result<T, E>>;

#[derive(Clone, Copy, Debug, Default)]
pub struct BitswapConfig;

impl UpgradeInfo for BitswapConfig {
    type Info = &'static [u8];
    type InfoIter = iter::Once<Self::Info>;

    fn protocol_info(&self) -> Self::InfoIter {
        // b"/ipfs/bitswap", b"/ipfs/bitswap/1.0.0"
        iter::once(b"/ipfs/bitswap/1.1.0")
    }
}

impl<TSocket> InboundUpgrade<TSocket> for BitswapConfig
where
    TSocket: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    type Output = Message;
    type Error = BitswapError;
    type Future = FutureResult<Self::Output, Self::Error>;

    #[inline]
    fn upgrade_inbound(self, mut socket: TSocket, _info: Self::Info) -> Self::Future {
        Box::pin(async move {
            let packet = upgrade::read_length_prefixed(&mut socket, MAX_BUF_SIZE).await?;
            let message = Message::from_bytes(&packet)?;
            Ok(message)
        })
    }
}

impl UpgradeInfo for Message {
    type Info = &'static [u8];
    type InfoIter = iter::Once<Self::Info>;

    fn protocol_info(&self) -> Self::InfoIter {
        // b"/ipfs/bitswap", b"/ipfs/bitswap/1.0.0"
        iter::once(b"/ipfs/bitswap/1.1.0")
    }
}

impl<TSocket> OutboundUpgrade<TSocket> for Message
where
    TSocket: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    type Output = ();
    type Error = io::Error;
    type Future = FutureResult<Self::Output, Self::Error>;

    #[inline]
    fn upgrade_outbound(self, mut socket: TSocket, _info: Self::Info) -> Self::Future {
        Box::pin(async move {
            let bytes = self.to_bytes();
            upgrade::write_length_prefixed(&mut socket, bytes).await?;
            socket.close().await
        })
    }
}

/// An object to facilitate communication between the `OneShotHandler` and the `BitswapHandler`.
#[derive(Debug)]
pub enum MessageWrapper {
    /// We received a `Message` from a remote.
    Rx(Message),
    /// We successfully sent a `Message`.
    Tx,
}

impl From<Message> for MessageWrapper {
    #[inline]
    fn from(message: Message) -> Self {
        Self::Rx(message)
    }
}

impl From<()> for MessageWrapper {
    #[inline]
    fn from(_: ()) -> Self {
        Self::Tx
    }
}
