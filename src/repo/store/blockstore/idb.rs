use std::{rc::Rc, str::FromStr, sync::OnceLock};

use crate::repo::store::idb::finish_transaction;
use crate::{
    Block, Error,
    repo::{BlockPut, BlockStore},
};
use futures::{StreamExt, TryStreamExt, stream::BoxStream};
use idb::{Database, DatabaseEvent, Factory, ObjectStoreParams, TransactionMode};
use ipld_core::cid::Cid;
use send_wrapper::SendWrapper;
use wasm_bindgen_futures::wasm_bindgen::JsValue;

const NAMESPACE: &str = "rust-block-store";

#[derive(Debug)]
pub struct IdbBlockStore {
    factory: send_wrapper::SendWrapper<Rc<Factory>>,
    database: OnceLock<send_wrapper::SendWrapper<Rc<Database>>>,
    namespace: String,
}

impl IdbBlockStore {
    pub fn new(namespace: Option<String>) -> Self {
        let namespace = match namespace {
            Some(ns) => format!("{NAMESPACE}-{ns}"),
            None => NAMESPACE.to_string(),
        };

        let factory = Factory::new().unwrap();

        Self {
            factory: SendWrapper::new(Rc::new(factory)),
            database: OnceLock::new(),
            namespace,
        }
    }

    pub fn get_db(&self) -> &send_wrapper::SendWrapper<Rc<Database>> {
        self.database.get().expect("initialized")
    }
}

impl BlockStore for IdbBlockStore {
    async fn init(&self) -> Result<(), Error> {
        SendWrapper::new(async move {
            let mut request = self.factory.open(&self.namespace, None)?;

            request.on_upgrade_needed(|event| {
                let db = event.database().unwrap();
                db.create_object_store("blockstore", ObjectStoreParams::new())
                    .unwrap();
            });

            let database = request.await?;

            self.database
                .get_or_init(|| SendWrapper::new(Rc::new(database)));
            Ok(())
        })
        .await
        .map_err(|e: Box<dyn std::error::Error>| anyhow::anyhow!("{e}"))
    }

    async fn contains(&self, cid: &Cid) -> Result<bool, Error> {
        SendWrapper::new(async move {
            let database = self.get_db();

            let transaction = database.transaction(&["blockstore"], TransactionMode::ReadOnly)?;

            let store = transaction.object_store("blockstore")?;

            let cid = JsValue::from_str(&cid.to_string());

            let val = store.get(cid)?.await?;
            finish_transaction(transaction).await?;
            Ok(val.is_some())
        })
        .await
        .map_err(|e: Box<dyn std::error::Error>| anyhow::anyhow!("{e}"))
    }

    async fn get(&self, cid: &Cid) -> Result<Option<Block>, Error> {
        SendWrapper::new(async move {
            let database = self.get_db();
            let transaction = database.transaction(&["blockstore"], TransactionMode::ReadOnly)?;

            let store = transaction.object_store("blockstore")?;

            let cid_val = JsValue::from_str(&cid.to_string());

            let block = store
                .get(cid_val)?
                .await?
                .map(|value| {
                    let bytes: Vec<u8> = serde_wasm_bindgen::from_value(value)?;
                    Ok::<_, Box<dyn std::error::Error>>(Block::new(*cid, bytes)?)
                })
                .transpose()?;

            finish_transaction(transaction).await?;
            Ok(block)
        })
        .await
        .map_err(|e: Box<dyn std::error::Error>| anyhow::anyhow!("{e}"))
    }

    async fn size(&self, cid: &[Cid]) -> Result<Option<usize>, Error> {
        SendWrapper::new(async move {
            let database = self.get_db();
            let transaction = database.transaction(&["blockstore"], TransactionMode::ReadOnly)?;

            let store = transaction.object_store("blockstore")?;

            let mut size: usize = 0;

            for cid in cid {
                let cid_val = JsValue::from_str(&cid.to_string());

                let block_size = store
                    .get(cid_val)?
                    .await?
                    .map(|value| {
                        let bytes: Vec<u8> = serde_wasm_bindgen::from_value(value)?;
                        Ok::<_, Box<dyn std::error::Error>>(Block::new(*cid, bytes)?.len())
                    })
                    .transpose()?;

                if let Some(b_size) = block_size {
                    size = size.saturating_add(b_size);
                }
            }

            finish_transaction(transaction).await?;

            Ok(Some(size))
        })
        .await
        .map_err(|e: Box<dyn std::error::Error>| anyhow::anyhow!("{e}"))
    }

    async fn total_size(&self) -> Result<usize, Error> {
        self.list_with_sizes()
            .await
            .try_fold(0usize, |total, (_, size)| {
                futures::future::ready(Ok(total.saturating_add(size)))
            })
            .await
    }

    async fn put(&self, block: &Block) -> Result<(Cid, BlockPut), Error> {
        if self.contains(block.cid()).await? {
            return Ok((*block.cid(), BlockPut::Existed));
        }

        SendWrapper::new(async move {
            let database = self.get_db();
            let transaction = database.transaction(&["blockstore"], TransactionMode::ReadWrite)?;

            let store = transaction.object_store("blockstore")?;

            let block_val = serde_wasm_bindgen::to_value(block.data())?;

            let cid_val = JsValue::from_str(&block.cid().to_string());

            store.put(&block_val, Some(&cid_val))?.await?;

            finish_transaction(transaction.commit()?).await?;

            Ok((*block.cid(), BlockPut::NewBlock))
        })
        .await
        .map_err(|e: Box<dyn std::error::Error>| anyhow::anyhow!("{e}"))
    }

    async fn remove(&self, cid: &Cid) -> Result<(), Error> {
        SendWrapper::new(async move {
            let database = self.get_db();
            let transaction = database.transaction(&["blockstore"], TransactionMode::ReadWrite)?;

            let store = transaction.object_store("blockstore")?;

            let cid_val = JsValue::from_str(&cid.to_string());

            store.delete(cid_val)?.await?;

            finish_transaction(transaction.commit()?).await?;

            Ok(())
        })
        .await
        .map_err(|e: Box<dyn std::error::Error>| anyhow::anyhow!("{e}"))
    }

    async fn remove_many(
        &self,
        blocks: BoxStream<'static, Cid>,
    ) -> BoxStream<'_, Result<Cid, Error>> {
        let cids = blocks.collect::<Vec<_>>().await;
        if cids.is_empty() {
            return futures::stream::empty().boxed();
        }

        let removed = SendWrapper::new(async move {
            let database = self.get_db();
            let transaction = database.transaction(&["blockstore"], TransactionMode::ReadWrite)?;
            let store = transaction.object_store("blockstore")?;

            let result = async {
                for cid in &cids {
                    let key = JsValue::from_str(&cid.to_string());
                    if store.count(Some(key.clone().into()))?.await? == 0 {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::NotFound,
                            format!("missing block {cid}"),
                        )
                        .into());
                    }
                    store.delete(key)?.await?;
                }
                Ok::<_, Box<dyn std::error::Error>>(())
            }
            .await;

            if let Err(error) = result {
                if let Ok(abort) = transaction.abort() {
                    let _ = abort.await;
                }
                return Err(error);
            }

            finish_transaction(transaction.commit()?).await?;
            Ok(cids)
        })
        .await
        .map_err(|error| anyhow::anyhow!("{error}"));

        match removed {
            Ok(cids) => futures::stream::iter(cids.into_iter().map(Ok)).boxed(),
            Err(error) => futures::stream::once(futures::future::ready(Err(error))).boxed(),
        }
    }

    async fn list(&self) -> BoxStream<'static, Cid> {
        let database = self.get_db().clone();
        SendWrapper::new(async_stream::stream! {
            let Ok(transaction) = database.transaction(&["blockstore"], TransactionMode::ReadOnly)
            else {
                return;
            };
            let Ok(store) = transaction.object_store("blockstore") else {
                return;
            };
            let Ok(request) = store.get_all_keys(None, None) else {
                return;
            };
            for cid in request
                .await
                .unwrap_or_default()
                .into_iter()
                .filter_map(|val| val.as_string())
                .filter_map(|cid_val| Cid::from_str(&cid_val).ok())
            {
                yield cid;
            }
        })
        .boxed()
    }

    async fn list_with_sizes(&self) -> BoxStream<'_, Result<(Cid, usize), Error>> {
        let entries = SendWrapper::new(async move {
            let database = self.get_db();
            let transaction = database.transaction(&["blockstore"], TransactionMode::ReadOnly)?;
            let store = transaction.object_store("blockstore")?;
            let keys = store.get_all_keys(None, None)?.await?;
            let mut entries = Vec::with_capacity(keys.len());

            for key in keys {
                let cid = key
                    .as_string()
                    .ok_or_else(|| std::io::Error::other("invalid block key"))?;
                let cid = Cid::from_str(&cid)?;
                let value = store
                    .get(key)?
                    .await?
                    .ok_or_else(|| std::io::Error::other(format!("missing block {cid}")))?;
                let data: Vec<u8> = serde_wasm_bindgen::from_value(value)?;
                entries.push((cid, data.len()));
            }

            finish_transaction(transaction).await?;
            Ok::<_, Box<dyn std::error::Error>>(entries)
        })
        .await
        .map_err(|error| anyhow::anyhow!("{error}"));

        match entries {
            Ok(entries) => futures::stream::iter(entries.into_iter().map(Ok)).boxed(),
            Err(error) => futures::stream::once(futures::future::ready(Err(error))).boxed(),
        }
    }
}
