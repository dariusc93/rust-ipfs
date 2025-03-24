use async_trait::async_trait;
use either::Either;
use futures::stream::BoxStream;
use ipld_core::cid::Cid;

use crate::error::Error;
use crate::repo::{DataStore, PinStore, References};
use crate::{PinKind, PinMode};

#[async_trait]
impl<L: DataStore, R: DataStore> DataStore for Either<L, R> {
    async fn init(&self) -> Result<(), Error> {
        match self {
            Either::Left(ref datastore) => datastore.init().await,
            Either::Right(ref datastore) => datastore.init().await,
        }
    }

    async fn contains(&self, key: &[u8]) -> Result<bool, Error> {
        match self {
            Either::Left(ref datastore) => datastore.contains(key).await,
            Either::Right(ref datastore) => datastore.contains(key).await,
        }
    }

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        match self {
            Either::Left(ref datastore) => datastore.get(key).await,
            Either::Right(ref datastore) => datastore.get(key).await,
        }
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        match self {
            Either::Left(ref datastore) => datastore.put(key, value).await,
            Either::Right(ref datastore) => datastore.put(key, value).await,
        }
    }

    async fn remove(&self, key: &[u8]) -> Result<(), Error> {
        match self {
            Either::Left(ref datastore) => datastore.remove(key).await,
            Either::Right(ref datastore) => datastore.remove(key).await,
        }
    }

    async fn iter(&self) -> BoxStream<'static, (Vec<u8>, Vec<u8>)> {
        match self {
            Either::Left(ref datastore) => datastore.iter().await,
            Either::Right(ref datastore) => datastore.iter().await,
        }
    }
}

#[async_trait]
impl<L: PinStore, R: PinStore> PinStore for Either<L, R> {
    async fn is_pinned(&self, block: &Cid) -> Result<bool, Error> {
        match self {
            Either::Left(ref datastore) => datastore.is_pinned(block).await,
            Either::Right(ref datastore) => datastore.is_pinned(block).await,
        }
    }

    async fn insert_direct_pin(&self, target: &Cid) -> Result<(), Error> {
        match self {
            Either::Left(ref datastore) => datastore.insert_direct_pin(target).await,
            Either::Right(ref datastore) => datastore.insert_direct_pin(target).await,
        }
    }

    async fn insert_recursive_pin(
        &self,
        target: &Cid,
        referenced: References<'_>,
    ) -> Result<(), Error> {
        match self {
            Either::Left(ref datastore) => datastore.insert_recursive_pin(target, referenced).await,
            Either::Right(ref datastore) => {
                datastore.insert_recursive_pin(target, referenced).await
            }
        }
    }

    async fn remove_direct_pin(&self, target: &Cid) -> Result<(), Error> {
        match self {
            Either::Left(ref datastore) => datastore.remove_direct_pin(target).await,
            Either::Right(ref datastore) => datastore.remove_direct_pin(target).await,
        }
    }

    async fn remove_recursive_pin(
        &self,
        target: &Cid,
        referenced: References<'_>,
    ) -> Result<(), Error> {
        match self {
            Either::Left(ref datastore) => datastore.remove_recursive_pin(target, referenced).await,
            Either::Right(ref datastore) => {
                datastore.remove_recursive_pin(target, referenced).await
            }
        }
    }

    async fn list(
        &self,
        mode: Option<PinMode>,
    ) -> BoxStream<'static, Result<(Cid, PinMode), Error>> {
        match self {
            Either::Left(ref datastore) => datastore.list(mode).await,
            Either::Right(ref datastore) => datastore.list(mode).await,
        }
    }

    async fn query(
        &self,
        ids: Vec<Cid>,
        requirement: Option<PinMode>,
    ) -> Result<Vec<(Cid, PinKind<Cid>)>, Error> {
        match self {
            Either::Left(ref datastore) => datastore.query(ids, requirement).await,
            Either::Right(ref datastore) => datastore.query(ids, requirement).await,
        }
    }
}
