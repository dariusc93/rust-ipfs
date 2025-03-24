use crate::error::Error;
use crate::{
    repo::{BlockPut, BlockStore},
    Block,
};
use async_trait::async_trait;
use either::Either;
use futures::stream::BoxStream;
use ipld_core::cid::Cid;

#[async_trait]
impl<L: BlockStore, R: BlockStore> BlockStore for Either<L, R> {
    async fn init(&self) -> Result<(), Error> {
        match self {
            Either::Left(ref blockstore) => blockstore.init().await,
            Either::Right(ref blockstore) => blockstore.init().await,
        }
    }

    async fn contains(&self, cid: &Cid) -> Result<bool, Error> {
        match self {
            Either::Left(ref blockstore) => blockstore.contains(cid).await,
            Either::Right(ref blockstore) => blockstore.contains(cid).await,
        }
    }

    async fn get(&self, cid: &Cid) -> Result<Option<Block>, Error> {
        match self {
            Either::Left(ref blockstore) => blockstore.get(cid).await,
            Either::Right(ref blockstore) => blockstore.get(cid).await,
        }
    }

    async fn size(&self, cid: &[Cid]) -> Result<Option<usize>, Error> {
        match self {
            Either::Left(ref blockstore) => blockstore.size(cid).await,
            Either::Right(ref blockstore) => blockstore.size(cid).await,
        }
    }

    async fn total_size(&self) -> Result<usize, Error> {
        match self {
            Either::Left(ref blockstore) => blockstore.total_size().await,
            Either::Right(ref blockstore) => blockstore.total_size().await,
        }
    }

    async fn put(&self, block: &Block) -> Result<(Cid, BlockPut), Error> {
        match self {
            Either::Left(ref blockstore) => blockstore.put(block).await,
            Either::Right(ref blockstore) => blockstore.put(block).await,
        }
    }

    async fn remove(&self, cid: &Cid) -> Result<(), Error> {
        match self {
            Either::Left(ref blockstore) => blockstore.remove(cid).await,
            Either::Right(ref blockstore) => blockstore.remove(cid).await,
        }
    }

    async fn remove_many(&self, blocks: BoxStream<'static, Cid>) -> BoxStream<'static, Cid> {
        match self {
            Either::Left(ref blockstore) => blockstore.remove_many(blocks).await,
            Either::Right(ref blockstore) => blockstore.remove_many(blocks).await,
        }
    }

    async fn list(&self) -> BoxStream<'static, Cid> {
        match self {
            Either::Left(ref blockstore) => blockstore.list().await,
            Either::Right(ref blockstore) => blockstore.list().await,
        }
    }
}
