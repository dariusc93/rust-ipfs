use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::BoxStream;
use ipld_core::cid::Cid;
use crate::repo::{DataStore, PinStore, References};
use crate::error::Error;
use crate::{PinKind, PinMode};

#[async_trait]
impl<D: DataStore> DataStore for Arc<D> {
    async fn init(&self) -> Result<(), Error> {
        (**self).init().await
    }

    async fn contains(&self, key: &[u8]) -> Result<bool, Error> {
        (**self).contains(key).await
    }

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        (**self).get(key).await
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        (**self).put(key, value).await
    }

    async fn remove(&self, key: &[u8]) -> Result<(), Error> {
        (**self).remove(key).await
    }

    async fn iter(&self) -> BoxStream<'static, (Vec<u8>, Vec<u8>)> {
        (**self).iter().await
    }
}

#[async_trait]
impl<P: PinStore> PinStore for Arc<P> {
    async fn is_pinned(&self, block: &Cid) -> Result<bool, Error> {
        (**self).is_pinned(block).await
    }
    
    async fn insert_direct_pin(&self, target: &Cid) -> Result<(), Error> {
        (**self).insert_direct_pin(target).await
    }

    async fn insert_recursive_pin(&self, target: &Cid, referenced: References<'_>) -> Result<(), Error> {
        (**self).insert_recursive_pin(target, referenced).await
    }

    async fn remove_direct_pin(&self, target: &Cid) -> Result<(), Error> {
        (**self).remove_direct_pin(target).await
    }

    async fn remove_recursive_pin(&self, target: &Cid, referenced: References<'_>) -> Result<(), Error> {
        (**self).remove_recursive_pin(target, referenced).await
    }

    async fn list(&self, mode: Option<PinMode>) -> BoxStream<'static, Result<(Cid, PinMode), Error>> {
        (**self).list(mode).await
    }

    async fn query(&self, ids: Vec<Cid>, requirement: Option<PinMode>) -> Result<Vec<(Cid, PinKind<Cid>)>, Error> {
        (**self).query(ids, requirement).await
    }
}