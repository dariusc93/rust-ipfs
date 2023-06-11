use std::fmt;
use std::future::Future;
use std::pin::Pin;

use asynchronous_codec::{Decoder, Encoder, Framed};
use bytes::{Bytes, BytesMut};
use futures::future;
use futures::io::{AsyncRead, AsyncWrite};
use libp2p::core::{InboundUpgrade, OutboundUpgrade, UpgradeInfo};
use libp2p::StreamProtocol;
use prost::Message;
use unsigned_varint::codec;

use crate::{handler::BitswapHandlerError, message::BitswapMessage};

const MAX_BUF_SIZE: usize = 1024 * 1024 * 2;

#[derive(Clone, Debug, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ProtocolId {
    Legacy = 0,
    Bitswap100 = 1,
    Bitswap110 = 2,
    Bitswap120 = 3,
}

const BITSWAP_LEGACY: StreamProtocol = StreamProtocol::new("/ipfs/bitswap");
const BITSWAP_100: StreamProtocol = StreamProtocol::new("/ipfs/bitswap/1.0.0");
const BITSWAP_110: StreamProtocol = StreamProtocol::new("/ipfs/bitswap/1.1.0");
const BITSWAP_120: StreamProtocol = StreamProtocol::new("/ipfs/bitswap/1.2.0");

impl ProtocolId {
    pub fn try_from(value: impl AsRef<str>) -> Option<Self> {
        let value = value.as_ref();

        if value == BITSWAP_LEGACY {
            Some(ProtocolId::Legacy)
        } else if value == BITSWAP_100 {
            Some(ProtocolId::Bitswap100)
        } else if value == BITSWAP_110 {
            Some(ProtocolId::Bitswap110)
        } else if value == BITSWAP_120 {
            Some(ProtocolId::Bitswap120)
        } else {
            None
        }
    }
}

impl AsRef<str> for ProtocolId {
    fn as_ref(&self) -> &str {
        match *self {
            ProtocolId::Legacy => todo!(),
            ProtocolId::Bitswap100 => todo!(),
            ProtocolId::Bitswap110 => todo!(),
            ProtocolId::Bitswap120 => todo!(),
        }
    }
}

impl From<ProtocolId> for StreamProtocol {
    fn from(protocol: ProtocolId) -> Self {
        match protocol {
            ProtocolId::Legacy => BITSWAP_LEGACY,
            ProtocolId::Bitswap100 => BITSWAP_100,
            ProtocolId::Bitswap110 => BITSWAP_110,
            ProtocolId::Bitswap120 => BITSWAP_120,
        }
    }
}

impl ProtocolId {
    pub fn supports_have(self) -> bool {
        matches!(self, ProtocolId::Bitswap120)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProtocolConfig {
    /// The bitswap protocols to listen on.
    pub protocol_ids: Vec<ProtocolId>,
    /// Maximum size of a packet.
    pub max_transmit_size: usize,
}

impl Default for ProtocolConfig {
    fn default() -> Self {
        ProtocolConfig {
            protocol_ids: vec![
                ProtocolId::Bitswap120,
                ProtocolId::Bitswap110,
                ProtocolId::Bitswap100,
                ProtocolId::Legacy,
            ],
            max_transmit_size: MAX_BUF_SIZE,
        }
    }
}

impl UpgradeInfo for ProtocolConfig {
    type Info = ProtocolId;
    type InfoIter = Vec<Self::Info>;

    fn protocol_info(&self) -> Self::InfoIter {
        self.protocol_ids.clone()
    }
}

impl<TSocket> InboundUpgrade<TSocket> for ProtocolConfig
where
    TSocket: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    type Output = Framed<TSocket, BitswapCodec>;
    type Error = BitswapHandlerError;
    #[allow(clippy::type_complexity)]
    type Future = Pin<Box<dyn Future<Output = Result<Self::Output, Self::Error>> + Send>>;

    #[inline]
    fn upgrade_inbound(self, socket: TSocket, protocol_id: Self::Info) -> Self::Future {
        let mut length_codec = codec::UviBytes::default();
        length_codec.set_max_len(self.max_transmit_size);
        Box::pin(future::ok(Framed::new(
            socket,
            BitswapCodec::new(length_codec, protocol_id),
        )))
    }
}

impl<TSocket> OutboundUpgrade<TSocket> for ProtocolConfig
where
    TSocket: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    type Output = Framed<TSocket, BitswapCodec>;
    type Error = BitswapHandlerError;
    #[allow(clippy::type_complexity)]
    type Future = Pin<Box<dyn Future<Output = Result<Self::Output, Self::Error>> + Send>>;

    #[inline]
    fn upgrade_outbound(self, socket: TSocket, protocol_id: Self::Info) -> Self::Future {
        let mut length_codec = codec::UviBytes::default();
        length_codec.set_max_len(self.max_transmit_size);
        Box::pin(future::ok(Framed::new(
            socket,
            BitswapCodec::new(length_codec, protocol_id),
        )))
    }
}

/// Bitswap codec for the framing
pub struct BitswapCodec {
    /// Codec to encode/decode the Unsigned varint length prefix of the frames.
    pub length_codec: codec::UviBytes,
    pub protocol: ProtocolId,
}

impl fmt::Debug for BitswapCodec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BitswapCodec")
            .field("length_codec", &"unsigned_varint::codec::UviBytes")
            .field("protocol", &self.protocol)
            .finish()
    }
}

impl BitswapCodec {
    pub fn new(length_codec: codec::UviBytes, protocol: ProtocolId) -> Self {
        BitswapCodec {
            length_codec,
            protocol,
        }
    }
}

impl Encoder for BitswapCodec {
    type Item = BitswapMessage;
    type Error = BitswapHandlerError;

    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
        tracing::trace!("sending message protocol: {:?}\n{:?}", self.protocol, item);

        let message = match self.protocol {
            ProtocolId::Legacy | ProtocolId::Bitswap100 => item.encode_as_proto_v0(),
            ProtocolId::Bitswap110 | ProtocolId::Bitswap120 => item.encode_as_proto_v1(),
        };
        let mut buf = BytesMut::with_capacity(message.encoded_len());
        message.encode(&mut buf).expect("fixed target");

        // length prefix the protobuf message, ensuring the max limit is not hit
        self.length_codec
            .encode(Bytes::from(buf), dst)
            .map_err(|_| BitswapHandlerError::MaxTransmissionSize)
    }
}

impl Decoder for BitswapCodec {
    type Item = (BitswapMessage, ProtocolId);
    type Error = BitswapHandlerError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let packet = match self.length_codec.decode(src).map_err(|e| {
            if let std::io::ErrorKind::PermissionDenied = e.kind() {
                BitswapHandlerError::MaxTransmissionSize
            } else {
                BitswapHandlerError::Io(e)
            }
        })? {
            Some(p) => p,
            None => return Ok(None),
        };

        let message = BitswapMessage::try_from(packet.freeze())?;

        Ok(Some((message, self.protocol)))
    }
}

#[cfg(test)]
mod tests {
    // use futures::prelude::*;
    // use libp2p::core::upgrade;
    // use tokio::net::{TcpListener, TcpStream};
    // use tokio_util::compat::*;

    use super::*;

    //TODO: Redesign due to https://github.com/libp2p/rust-libp2p/pull/3915
    // #[tokio::test]
    // async fn test_upgrade() {
    //     let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    //     let listener_addr = listener.local_addr().unwrap();

    //     let server = async move {
    //         let (incoming, _) = listener.accept().await.unwrap();
    //         upgrade::apply_inbound(incoming.compat(), ProtocolConfig::default())
    //             .await
    //             .unwrap();
    //     };

    //     let client = async move {
    //         let stream = TcpStream::connect(&listener_addr).await.unwrap();
    //         upgrade::apply_outbound(
    //             stream.compat(),
    //             ProtocolConfig::default(),
    //             upgrade::Version::V1Lazy,
    //         )
    //         .await
    //         .unwrap();
    //     };

    //     future::select(Box::pin(server), Box::pin(client)).await;
    // }

    #[test]
    fn test_ord() {
        let mut protocols = [
            ProtocolId::Bitswap120,
            ProtocolId::Bitswap100,
            ProtocolId::Legacy,
        ];
        protocols.sort();
        assert_eq!(
            protocols,
            [
                ProtocolId::Legacy,
                ProtocolId::Bitswap100,
                ProtocolId::Bitswap120
            ]
        );
    }
}
