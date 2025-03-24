use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::BoxStream;
use ipld_core::cid::Cid;

use crate::repo::{BlockPut, BlockStore};

use crate::error::Error;
use crate::Block;

#[async_trait]
impl<B: BlockStore> BlockStore for Arc<B> {
    async fn init(&self) -> Result<(), Error> {
        (**self).init().await
    }
    async fn contains(&self, cid: &Cid) -> Result<bool, Error> {
        (**self).contains(cid).await
    }
    async fn get(&self, cid: &Cid) -> Result<Option<Block>, Error> {
        (**self).get(cid).await
    }
    async fn size(&self, cid: &[Cid]) -> Result<Option<usize>, Error> {
        (**self).size(cid).await
    }
    async fn total_size(&self) -> Result<usize, Error> {
        (**self).total_size().await
    }
    async fn put(&self, block: &Block) -> Result<(Cid, BlockPut), Error> {
        (**self).put(block).await
    }
    async fn remove(&self, cid: &Cid) -> Result<(), Error> {
        (**self).remove(cid).await
    }
    async fn remove_many(&self, blocks: BoxStream<'static, Cid>) -> BoxStream<'static, Cid> {
        (**self).remove_many(blocks).await
    }
    async fn list(&self) -> BoxStream<'static, Cid> {
        (**self).list().await
    }
}
