use crate::error::Error;
use crate::repo::{DataStore, PinStore, References};
use crate::{
    repo::{BlockPut, BlockStore},
    Block, PinKind, PinMode,
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

    async fn put_many(&self, blocks: &[Block]) -> Result<Vec<(Cid, BlockPut)>, Error> {
        match self {
            Either::Left(blockstore) => blockstore.put_many(blocks).await,
            Either::Right(blockstore) => blockstore.put_many(blocks).await,
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

impl<L: DataStore, R: DataStore> DataStore for Either<L, R> {
    async fn init(&self) -> Result<(), Error> {
        match self {
            Either::Left(datastore) => datastore.init().await,
            Either::Right(datastore) => datastore.init().await,
        }
    }

    async fn contains(&self, key: &[u8]) -> Result<bool, Error> {
        match self {
            Either::Left(datastore) => datastore.contains(key).await,
            Either::Right(datastore) => datastore.contains(key).await,
        }
    }

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        match self {
            Either::Left(datastore) => datastore.get(key).await,
            Either::Right(datastore) => datastore.get(key).await,
        }
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        match self {
            Either::Left(datastore) => datastore.put(key, value).await,
            Either::Right(datastore) => datastore.put(key, value).await,
        }
    }

    async fn remove(&self, key: &[u8]) -> Result<(), Error> {
        match self {
            Either::Left(datastore) => datastore.remove(key).await,
            Either::Right(datastore) => datastore.remove(key).await,
        }
    }

    async fn iter(&self) -> BoxStream<'static, (Vec<u8>, Vec<u8>)> {
        match self {
            Either::Left(datastore) => datastore.iter().await,
            Either::Right(datastore) => datastore.iter().await,
        }
    }
}

impl<L: PinStore, R: PinStore> PinStore for Either<L, R> {
    async fn is_pinned(&self, block: &Cid) -> Result<bool, Error> {
        match self {
            Either::Left(datastore) => datastore.is_pinned(block).await,
            Either::Right(datastore) => datastore.is_pinned(block).await,
        }
    }

    async fn insert_direct_pin(&self, target: &Cid) -> Result<(), Error> {
        match self {
            Either::Left(datastore) => datastore.insert_direct_pin(target).await,
            Either::Right(datastore) => datastore.insert_direct_pin(target).await,
        }
    }

    async fn insert_recursive_pin(
        &self,
        target: &Cid,
        referenced: References<'_>,
    ) -> Result<(), Error> {
        match self {
            Either::Left(datastore) => datastore.insert_recursive_pin(target, referenced).await,
            Either::Right(datastore) => datastore.insert_recursive_pin(target, referenced).await,
        }
    }

    async fn remove_direct_pin(&self, target: &Cid) -> Result<(), Error> {
        match self {
            Either::Left(datastore) => datastore.remove_direct_pin(target).await,
            Either::Right(datastore) => datastore.remove_direct_pin(target).await,
        }
    }

    async fn remove_recursive_pin(
        &self,
        target: &Cid,
        referenced: References<'_>,
    ) -> Result<(), Error> {
        match self {
            Either::Left(datastore) => datastore.remove_recursive_pin(target, referenced).await,
            Either::Right(datastore) => datastore.remove_recursive_pin(target, referenced).await,
        }
    }

    async fn list(
        &self,
        mode: Option<PinMode>,
    ) -> BoxStream<'static, Result<(Cid, PinMode), Error>> {
        match self {
            Either::Left(datastore) => datastore.list(mode).await,
            Either::Right(datastore) => datastore.list(mode).await,
        }
    }

    async fn query(
        &self,
        ids: Vec<Cid>,
        requirement: Option<PinMode>,
    ) -> Result<Vec<(Cid, PinKind<Cid>)>, Error> {
        match self {
            Either::Left(datastore) => datastore.query(ids, requirement).await,
            Either::Right(datastore) => datastore.query(ids, requirement).await,
        }
    }
}
