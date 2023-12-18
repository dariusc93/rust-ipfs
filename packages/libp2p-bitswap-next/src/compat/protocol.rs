// use crate::compat::message::CompatMessageCodec;
use crate::compat::{other, CompatMessage};
// use asynchronous_codec::Framed;
use futures::future::BoxFuture;
use futures::io::{AsyncRead, AsyncWrite};
use futures::{AsyncReadExt, AsyncWriteExt};
use libp2p::core::{InboundUpgrade, OutboundUpgrade, UpgradeInfo};
use libp2p::StreamProtocol;
use std::{io, iter};
// use unsigned_varint::codec;

// 2MB Block Size according to the specs at https://github.com/ipfs/specs/blob/main/BITSWAP.md
const MAX_BUF_SIZE: usize = 2_097_152;

#[derive(Clone, Debug, Default)]
pub struct CompatProtocol;

impl UpgradeInfo for CompatProtocol {
    type Info = StreamProtocol;
    type InfoIter = iter::Once<Self::Info>;

    fn protocol_info(&self) -> Self::InfoIter {
        iter::once(StreamProtocol::new("/ipfs/bitswap/1.2.0"))
    }
}

//TODO: Migrate to using `Framed`

// impl<TSocket> InboundUpgrade<TSocket> for CompatProtocol
// where
//     TSocket: AsyncRead + AsyncWrite + Send + Unpin + 'static,
// {
//     type Output = InboundMessage;
//     type Error = io::Error;
//     type Future = BoxFuture<'static, Result<Self::Output, Self::Error>>;

//     fn upgrade_inbound(self, socket: TSocket, _info: Self::Info) -> Self::Future {
//         Box::pin(async move {
//             let mut length_codec = codec::UviBytes::default();
//             length_codec.set_max_len(MAX_BUF_SIZE);
//             tracing::trace!("upgrading inbound");
//             let mut framed = Framed::new(socket, CompatMessageCodec::new(length_codec));

//             let data = framed
//                 .next()
//                 .await
//                 .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::UnexpectedEof))??;

//             Ok(data)
//         })
//     }
// }

impl UpgradeInfo for CompatMessage {
    type Info = StreamProtocol;
    type InfoIter = iter::Once<Self::Info>;

    fn protocol_info(&self) -> Self::InfoIter {
        iter::once(StreamProtocol::new("/ipfs/bitswap/1.2.0"))
    }
}

// impl<TSocket> OutboundUpgrade<TSocket> for CompatMessage
// where
//     TSocket: AsyncRead + AsyncWrite + Send + Unpin + 'static,
// {
//     type Output = ();
//     type Error = io::Error;
//     type Future = BoxFuture<'static, Result<Self::Output, Self::Error>>;

//     fn upgrade_outbound(self, socket: TSocket, _info: Self::Info) -> Self::Future {
//         Box::pin(async move {
//             let mut length_codec = codec::UviBytes::default();
//             length_codec.set_max_len(MAX_BUF_SIZE);

//             let mut framed = Framed::new(socket, CompatMessageCodec::new(length_codec));

//             framed.send(self).await?;
//             framed.close().await?;
//             Ok(())
//         })
//     }
// }

impl<TSocket> InboundUpgrade<TSocket> for CompatProtocol
where
    TSocket: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    type Output = InboundMessage;
    type Error = io::Error;
    type Future = BoxFuture<'static, Result<Self::Output, Self::Error>>;

    fn upgrade_inbound(self, mut socket: TSocket, _info: Self::Info) -> Self::Future {
        Box::pin(async move {
            tracing::trace!("upgrading inbound");
            let packet = read_length_prefixed(&mut socket, MAX_BUF_SIZE)
                .await
                .map_err(|err| {
                    tracing::debug!(%err, "inbound upgrade error");
                    other(err)
                })?;
            socket.close().await?;
            tracing::trace!("inbound upgrade done, closing");
            let message = CompatMessage::from_bytes(&packet).map_err(|e| {
                tracing::debug!(%e, "inbound upgrade error");
                e
            })?;
            tracing::trace!("inbound upgrade closed");
            Ok(InboundMessage(message))
        })
    }
}

impl<TSocket> OutboundUpgrade<TSocket> for CompatMessage
where
    TSocket: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    type Output = ();
    type Error = io::Error;
    type Future = BoxFuture<'static, Result<Self::Output, Self::Error>>;

    fn upgrade_outbound(self, mut socket: TSocket, _info: Self::Info) -> Self::Future {
        Box::pin(async move {
            let bytes = self.to_bytes()?;
            write_length_prefixed(&mut socket, bytes).await?;
            socket.close().await?;
            Ok(())
        })
    }
}

#[derive(Debug)]
pub struct InboundMessage(pub Vec<CompatMessage>);

impl From<()> for InboundMessage {
    fn from(_: ()) -> Self {
        Self(Default::default())
    }
}

pub async fn write_length_prefixed(
    socket: &mut (impl AsyncWrite + Unpin),
    data: impl AsRef<[u8]>,
) -> Result<(), io::Error> {
    write_varint(socket, data.as_ref().len()).await?;
    socket.write_all(data.as_ref()).await?;
    socket.flush().await?;

    Ok(())
}

pub async fn write_varint(
    socket: &mut (impl AsyncWrite + Unpin),
    len: usize,
) -> Result<(), io::Error> {
    let mut len_data = unsigned_varint::encode::usize_buffer();
    let encoded_len = unsigned_varint::encode::usize(len, &mut len_data).len();
    socket.write_all(&len_data[..encoded_len]).await?;

    Ok(())
}

pub async fn read_varint(socket: &mut (impl AsyncRead + Unpin)) -> Result<usize, io::Error> {
    let mut buffer = unsigned_varint::encode::usize_buffer();
    let mut buffer_len = 0;

    loop {
        match socket.read(&mut buffer[buffer_len..buffer_len + 1]).await? {
            0 => {
                // Reaching EOF before finishing to read the length is an error, unless the EOF is
                // at the very beginning of the substream, in which case we assume that the data is
                // empty.
                if buffer_len == 0 {
                    return Ok(0);
                } else {
                    return Err(io::ErrorKind::UnexpectedEof.into());
                }
            }
            n => debug_assert_eq!(n, 1),
        }

        buffer_len += 1;

        match unsigned_varint::decode::usize(&buffer[..buffer_len]) {
            Ok((len, _)) => return Ok(len),
            Err(unsigned_varint::decode::Error::Overflow) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "overflow in variable-length integer",
                ));
            }
            // TODO: why do we have a `__Nonexhaustive` variant in the error? I don't know how to process it
            // Err(unsigned_varint::decode::Error::Insufficient) => {}
            Err(_) => {}
        }
    }
}

pub async fn read_length_prefixed(
    socket: &mut (impl AsyncRead + Unpin),
    max_size: usize,
) -> io::Result<Vec<u8>> {
    let len = read_varint(socket).await?;
    if len > max_size {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Received data size ({len} bytes) exceeds maximum ({max_size} bytes)"),
        ));
    }

    let mut buf = vec![0; len];
    socket.read_exact(&mut buf).await?;

    Ok(buf)
}

// TODO:
// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::protocol::{BitswapRequest, RequestType};
//     use async_std::net::{TcpListener, TcpStream};
//     use futures::prelude::*;
//     use libipld::Cid;
//     use libp2p::core::upgrade;

//     #[async_std::test]
//     async fn test_upgrade() {
//         let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
//         let listener_addr = listener.local_addr().unwrap();

//         let server = async move {
//             let incoming = listener.incoming().into_future().await.0.unwrap().unwrap();
//             upgrade::apply_inbound(incoming, CompatProtocol)
//                 .await
//                 .unwrap();
//         };

//         let client = async move {
//             let stream = TcpStream::connect(&listener_addr).await.unwrap();
//             upgrade::apply_outbound(
//                 stream,
//                 CompatMessage::Request(BitswapRequest {
//                     ty: RequestType::Have,
//                     cid: Cid::default(),
//                 }),
//                 upgrade::Version::V1,
//             )
//             .await
//             .unwrap();
//         };

//         future::select(Box::pin(server), Box::pin(client)).await;
//     }
// }
