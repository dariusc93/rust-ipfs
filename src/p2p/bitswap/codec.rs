use asynchronous_codec::{FramedRead, FramedWrite};
use bitswap_pb::message::BlockPresenceType;
use futures::{AsyncRead, AsyncWrite, AsyncWriteExt, SinkExt, StreamExt};
use libipld::Cid;
use libp2p::request_response;
use std::io;

use super::{
    bitswap_pb,
    message::{BitswapMessage, BitswapRequest, BitswapResponse},
    prefix::Prefix,
};

const MAX_BUF_SIZE: usize = 2_097_152;

#[derive(Default, Debug, Clone)]
pub struct Codec;

#[async_trait::async_trait]
impl request_response::Codec for Codec {
    type Protocol = &'static str;
    type Request = Vec<BitswapMessage>;
    type Response = ();

    async fn read_request<T>(
        &mut self,
        _: &Self::Protocol,
        socket: &mut T,
    ) -> io::Result<Self::Request>
    where
        T: AsyncRead + Send + Unpin,
    {
        let message: bitswap_pb::Message = FramedRead::new(
            socket,
            quick_protobuf_codec::Codec::<bitswap_pb::Message>::new(MAX_BUF_SIZE),
        )
        .next()
        .await
        .ok_or(std::io::ErrorKind::UnexpectedEof)??;

        let mut parts = vec![];
        if let Some(list) = message.wantlist {
            for entry in list.entries {
                let cid = Cid::try_from(entry.block).map_err(io::Error::other)?;
                parts.push(BitswapMessage::Request(BitswapRequest {
                    ty: entry.wantType.into(),
                    cid,
                    send_dont_have: entry.sendDontHave,
                    cancel: entry.cancel,
                }));
            }
        }

        for payload in message.payload {
            let prefix = Prefix::new(&payload.prefix).map_err(io::Error::other)?;
            let cid = prefix.to_cid(&payload.data).map_err(io::Error::other)?;
            parts.push(BitswapMessage::Response(
                cid,
                BitswapResponse::Block(payload.data),
            ));
        }

        for presence in message.blockPresences {
            let cid = Cid::try_from(presence.cid).map_err(io::Error::other)?;
            let have = presence.type_pb == BlockPresenceType::Have;
            parts.push(BitswapMessage::Response(cid, BitswapResponse::Have(have)));
        }

        Ok(parts)
    }

    async fn read_response<T>(
        &mut self,
        _: &Self::Protocol,
        _: &mut T,
    ) -> io::Result<Self::Response>
    where
        T: AsyncRead + Send + Unpin,
    {
        Ok(())
    }

    async fn write_request<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
        mut messages: Self::Request,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Send + Unpin,
    {
        assert_eq!(
            messages.len(),
            1
        );

        let data = messages.swap_remove(0).into_proto()?;
        let mut framed = FramedWrite::new(
            io,
            quick_protobuf_codec::Codec::<bitswap_pb::Message>::new(MAX_BUF_SIZE),
        );
        framed.send(data).await?;
        framed.flush().await?;
        framed.close().await?;

        Ok(())
    }

    async fn write_response<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
        _: Self::Response,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Send + Unpin,
    {
        io.close().await?;
        Ok(())
    }
}
