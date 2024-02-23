use std::iter;

use asynchronous_codec::{FramedRead, FramedWrite};
use futures::{future::BoxFuture, AsyncRead, AsyncWrite, SinkExt, StreamExt};
use libp2p::{core::UpgradeInfo, InboundUpgrade, OutboundUpgrade, StreamProtocol};

use super::bitswap_pb;

const PROTOCOL: StreamProtocol = StreamProtocol::new("/ipfs/bitswap/1.2.0");
const MAX_SIZE: usize = 2 * 1024 * 1024;

#[derive(Debug, Clone, Default)]
pub struct BitswapProtocol;

impl UpgradeInfo for BitswapProtocol {
    type Info = StreamProtocol;
    type InfoIter = iter::Once<Self::Info>;

    fn protocol_info(&self) -> Self::InfoIter {
        iter::once(PROTOCOL)
    }
}

impl<TSocket> InboundUpgrade<TSocket> for BitswapProtocol
where
    TSocket: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    type Output = bitswap_pb::Message;
    type Error = std::io::Error;
    type Future = BoxFuture<'static, Result<Self::Output, Self::Error>>;

    fn upgrade_inbound(self, socket: TSocket, _: Self::Info) -> Self::Future {
        Box::pin(async move {
            let mut framed = FramedRead::new(
                socket,
                quick_protobuf_codec::Codec::<bitswap_pb::Message>::new(MAX_SIZE),
            );

            let message = framed
                .next()
                .await
                .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::UnexpectedEof))?
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::UnexpectedEof, e))?;

            Ok(message)
        })
    }
}

impl UpgradeInfo for bitswap_pb::Message {
    type Info = StreamProtocol;
    type InfoIter = iter::Once<Self::Info>;

    fn protocol_info(&self) -> Self::InfoIter {
        iter::once(PROTOCOL)
    }
}

impl<TSocket> OutboundUpgrade<TSocket> for bitswap_pb::Message
where
    TSocket: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    type Output = ();
    type Error = std::io::Error;
    type Future = BoxFuture<'static, Result<Self::Output, Self::Error>>;

    #[inline]
    fn upgrade_outbound(self, socket: TSocket, _info: Self::Info) -> Self::Future {
        Box::pin(async move {
            let mut framed = FramedWrite::new(
                socket,
                quick_protobuf_codec::Codec::<bitswap_pb::Message>::new(MAX_SIZE),
            );

            framed.send(self).await?;
            framed.close().await?;
            Ok(())
        })
    }
}

#[derive(Debug)]
pub enum Message {
    Receive { message: bitswap_pb::Message },
    Sent,
}

impl From<bitswap_pb::Message> for Message {
    #[inline]
    fn from(message: bitswap_pb::Message) -> Self {
        Message::Receive { message }
    }
}

impl From<()> for Message {
    #[inline]
    fn from(_: ()) -> Self {
        Message::Sent
    }
}
