use async_trait::async_trait;
use futures::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use libipld::cid::Cid;
use libipld::store::StoreParams;
use libp2p::request_response::Codec;
use libp2p::StreamProtocol;
use std::convert::TryFrom;
use std::io::{self, Write};
use std::marker::PhantomData;
use thiserror::Error;
use unsigned_varint::{aio, io::ReadError};

// version codec hash size (u64 varint is max 10 bytes) + digest
const MAX_CID_SIZE: usize = 4 * 10 + 64;

pub(crate) const LIBP2P_BITSWAP_PROTOCOL: StreamProtocol =
    StreamProtocol::new("/ipfs-embed/bitswap/1.0.0");

#[derive(Clone)]
pub struct BitswapCodec<P> {
    _marker: PhantomData<P>,
    buffer: Vec<u8>,
}

impl<P: StoreParams> Default for BitswapCodec<P> {
    fn default() -> Self {
        let capacity = usize::max(P::MAX_BLOCK_SIZE, MAX_CID_SIZE) + 1;
        debug_assert!(capacity <= u32::MAX as usize);
        Self {
            _marker: PhantomData,
            buffer: Vec::with_capacity(capacity),
        }
    }
}

#[async_trait]
impl<P: StoreParams> Codec for BitswapCodec<P> {
    type Protocol = StreamProtocol;
    type Request = BitswapRequest;
    type Response = BitswapResponse;

    async fn read_request<T>(&mut self, _: &Self::Protocol, io: &mut T) -> io::Result<Self::Request>
    where
        T: AsyncRead + Send + Unpin,
    {
        let msg_len = u32_to_usize(aio::read_u32(&mut *io).await.map_err(|e| match e {
            ReadError::Io(e) => e,
            err => other(err),
        })?);
        if msg_len > MAX_CID_SIZE + 1 {
            return Err(invalid_data(MessageTooLarge(msg_len)));
        }
        self.buffer.resize(msg_len, 0);
        io.read_exact(&mut self.buffer).await?;
        let request = BitswapRequest::from_bytes(&self.buffer).map_err(invalid_data)?;
        Ok(request)
    }

    async fn read_response<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
    ) -> io::Result<Self::Response>
    where
        T: AsyncRead + Send + Unpin,
    {
        let msg_len = u32_to_usize(aio::read_u32(&mut *io).await.map_err(|e| match e {
            ReadError::Io(e) => e,
            err => other(err),
        })?);
        if msg_len > P::MAX_BLOCK_SIZE + 1 {
            return Err(invalid_data(MessageTooLarge(msg_len)));
        }
        self.buffer.resize(msg_len, 0);
        io.read_exact(&mut self.buffer).await?;
        let response = BitswapResponse::from_bytes(&self.buffer).map_err(invalid_data)?;
        Ok(response)
    }

    async fn write_request<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
        req: Self::Request,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Send + Unpin,
    {
        self.buffer.clear();
        req.write_to(&mut self.buffer)?;
        if self.buffer.len() > MAX_CID_SIZE + 1 {
            return Err(invalid_data(MessageTooLarge(self.buffer.len())));
        }
        let mut buf = unsigned_varint::encode::u32_buffer();
        let msg_len = unsigned_varint::encode::u32(self.buffer.len() as u32, &mut buf);
        io.write_all(msg_len).await?;
        io.write_all(&self.buffer).await?;
        Ok(())
    }

    async fn write_response<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
        res: Self::Response,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Send + Unpin,
    {
        self.buffer.clear();
        res.write_to(&mut self.buffer)?;
        if self.buffer.len() > P::MAX_BLOCK_SIZE + 1 {
            return Err(invalid_data(MessageTooLarge(self.buffer.len())));
        }
        let mut buf = unsigned_varint::encode::u32_buffer();
        let msg_len = unsigned_varint::encode::u32(self.buffer.len() as u32, &mut buf);
        io.write_all(msg_len).await?;
        io.write_all(&self.buffer).await?;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequestType {
    Have,
    Block,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BitswapRequest {
    pub ty: RequestType,
    pub cid: Cid,
}

impl BitswapRequest {
    pub fn write_to<W: Write>(&self, w: &mut W) -> io::Result<()> {
        match self {
            BitswapRequest {
                ty: RequestType::Have,
                cid,
            } => {
                w.write_all(&[0])?;
                cid.write_bytes(&mut *w).map_err(other)?;
            }
            BitswapRequest {
                ty: RequestType::Block,
                cid,
            } => {
                w.write_all(&[1])?;
                cid.write_bytes(&mut *w).map_err(other)?;
            }
        }
        Ok(())
    }

    pub fn from_bytes(bytes: &[u8]) -> io::Result<Self> {
        let ty = match bytes[0] {
            0 => RequestType::Have,
            1 => RequestType::Block,
            c => return Err(invalid_data(UnknownMessageType(c))),
        };
        let cid = Cid::try_from(&bytes[1..]).map_err(invalid_data)?;
        Ok(Self { ty, cid })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BitswapResponse {
    Have(bool),
    Block(Vec<u8>),
}

impl BitswapResponse {
    pub fn write_to<W: Write>(&self, w: &mut W) -> io::Result<()> {
        match self {
            BitswapResponse::Have(have) => {
                if *have {
                    w.write_all(&[0])?;
                } else {
                    w.write_all(&[2])?;
                }
            }
            BitswapResponse::Block(data) => {
                w.write_all(&[1])?;
                w.write_all(data)?;
            }
        };
        Ok(())
    }

    pub fn from_bytes(bytes: &[u8]) -> io::Result<Self> {
        let res = match bytes[0] {
            0 | 2 => BitswapResponse::Have(bytes[0] == 0),
            1 => BitswapResponse::Block(bytes[1..].to_vec()),
            c => return Err(invalid_data(UnknownMessageType(c))),
        };
        Ok(res)
    }
}

fn invalid_data<E: std::error::Error + Send + Sync + 'static>(e: E) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, e)
}

fn other<E: std::error::Error + Send + Sync + 'static>(e: E) -> io::Error {
    io::Error::new(io::ErrorKind::Other, e)
}

#[cfg(any(target_pointer_width = "64", target_pointer_width = "32"))]
fn u32_to_usize(n: u32) -> usize {
    n as usize
}

#[derive(Debug, Error)]
#[error("unknown message type {0}")]
pub struct UnknownMessageType(u8);

#[derive(Debug, Error)]
#[error("message too large {0}")]
pub struct MessageTooLarge(usize);

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use libipld::multihash::Code;
    use multihash::MultihashDigest;

    pub fn create_cid(bytes: &[u8]) -> Cid {
        let digest = Code::Blake3_256.digest(bytes);
        Cid::new_v1(0x55, digest)
    }

    #[test]
    fn test_request_encode_decode() {
        let requests = [
            BitswapRequest {
                ty: RequestType::Have,
                cid: create_cid(&b"have_request"[..]),
            },
            BitswapRequest {
                ty: RequestType::Block,
                cid: create_cid(&b"block_request"[..]),
            },
        ];
        let mut buf = Vec::with_capacity(MAX_CID_SIZE + 1);
        for request in &requests {
            buf.clear();
            request.write_to(&mut buf).unwrap();
            assert_eq!(&BitswapRequest::from_bytes(&buf).unwrap(), request);
        }
    }

    #[test]
    fn test_response_encode_decode() {
        let responses = [
            BitswapResponse::Have(true),
            BitswapResponse::Have(false),
            BitswapResponse::Block(b"block_response".to_vec()),
        ];
        let mut buf = Vec::with_capacity(13 + 1);
        for response in &responses {
            buf.clear();
            response.write_to(&mut buf).unwrap();
            assert_eq!(&BitswapResponse::from_bytes(&buf).unwrap(), response);
        }
    }
}
