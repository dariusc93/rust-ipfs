use crate::error::Error;
use crate::{
    repo::{BlockPut, BlockStore},
    Block,
};
use either::Either;
use futures::stream::BoxStream;
use ipld_core::cid::Cid;

impl<L: BlockStore, R: BlockStore> BlockStore for Either<L, R> {
    async fn init(&self) -> Result<(), Error> {
        match self {
            Either::Left(blockstore) => blockstore.init().await,
            Either::Right(blockstore) => blockstore.init().await,
        }
    }

    async fn contains(&self, cid: &Cid) -> Result<bool, Error> {
        match self {
            Either::Left(blockstore) => blockstore.contains(cid).await,
            Either::Right(blockstore) => blockstore.contains(cid).await,
        }
    }

    async fn get(&self, cid: &Cid) -> Result<Option<Block>, Error> {
        match self {
            Either::Left(blockstore) => blockstore.get(cid).await,
            Either::Right(blockstore) => blockstore.get(cid).await,
        }
    }

    async fn size(&self, cid: &[Cid]) -> Result<Option<usize>, Error> {
        match self {
            Either::Left(blockstore) => blockstore.size(cid).await,
            Either::Right(blockstore) => blockstore.size(cid).await,
        }
    }

    async fn total_size(&self) -> Result<usize, Error> {
        match self {
            Either::Left(blockstore) => blockstore.total_size().await,
            Either::Right(blockstore) => blockstore.total_size().await,
        }
    }

    async fn put(&self, block: &Block) -> Result<(Cid, BlockPut), Error> {
        match self {
            Either::Left(blockstore) => blockstore.put(block).await,
            Either::Right(blockstore) => blockstore.put(block).await,
        }
    }

    async fn remove(&self, cid: &Cid) -> Result<(), Error> {
        match self {
            Either::Left(blockstore) => blockstore.remove(cid).await,
            Either::Right(blockstore) => blockstore.remove(cid).await,
        }
    }

    async fn remove_many(&self, blocks: BoxStream<'static, Cid>) -> BoxStream<'static, Cid> {
        match self {
            Either::Left(blockstore) => blockstore.remove_many(blocks).await,
            Either::Right(blockstore) => blockstore.remove_many(blocks).await,
        }
    }

    async fn list(&self) -> BoxStream<'static, Cid> {
        match self {
            Either::Left(blockstore) => blockstore.list().await,
            Either::Right(blockstore) => blockstore.list().await,
        }
    }
}
