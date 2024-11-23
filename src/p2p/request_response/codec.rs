use async_trait::async_trait;
use bytes::Bytes;
use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use libp2p::StreamProtocol;

#[derive(Debug, Copy, Clone)]
pub struct Codec {
    max_request_size: usize,
    max_response_size: usize,
}

impl Codec {
    pub fn new(max_request_size: usize, max_response_size: usize) -> Self {
        Self {
            max_response_size,
            max_request_size,
        }
    }
}

#[async_trait]
impl libp2p::request_response::Codec for Codec {
    type Protocol = StreamProtocol;
    type Request = Bytes;
    type Response = Bytes;

    async fn read_request<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
    ) -> std::io::Result<Self::Request>
    where
        T: AsyncRead + Unpin + Send,
    {
        let mut buffer = Vec::new();
        io.take(self.max_request_size as u64)
            .read_to_end(&mut buffer)
            .await?;
        Ok(Bytes::from(buffer))
    }

    async fn read_response<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
    ) -> std::io::Result<Self::Response>
    where
        T: AsyncRead + Unpin + Send,
    {
        let mut buffer = Vec::new();
        io.take(self.max_response_size as u64)
            .read_to_end(&mut buffer)
            .await?;
        Ok(Bytes::from(buffer))
    }

    async fn write_request<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
        req: Self::Request,
    ) -> std::io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        if req.len() > self.max_request_size {
            return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof));
        }

        io.write_all(&req).await?;
        Ok(())
    }

    async fn write_response<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
        res: Self::Response,
    ) -> std::io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        if res.len() > self.max_response_size {
            return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof));
        }
        io.write_all(&res).await?;
        Ok(())
    }
}
