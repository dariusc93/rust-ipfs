use std::{rc::Rc, str::FromStr, sync::OnceLock};

use crate::{
    repo::{BlockPut, BlockStore}, Block,
    Error,
};
use futures::{stream::BoxStream, StreamExt};
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
            transaction.await?;
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

            let block = store.get(cid_val)?.await.map(|val| {
                val.and_then(|val| {
                    let bytes: Vec<u8> = serde_wasm_bindgen::from_value(val).ok()?;
                    Block::new(*cid, bytes).ok()
                })
            })?;

            transaction.await?;
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

                let block_size = store.get(cid_val)?.await.map(|val| {
                    val.and_then(|val| {
                        let bytes: Vec<u8> = serde_wasm_bindgen::from_value(val).ok()?;
                        Block::new(*cid, bytes).map(|block| block.len()).ok()
                    })
                })?;

                if let Some(b_size) = block_size {
                    size += b_size;
                }
            }

            transaction.await?;

            Ok(Some(size))
        })
        .await
        .map_err(|e: Box<dyn std::error::Error>| anyhow::anyhow!("{e}"))
    }

    async fn total_size(&self) -> Result<usize, Error> {
        SendWrapper::new(async move {
            let mut block_list = self.list().await;
            let database = self.get_db();
            let transaction = database.transaction(&["blockstore"], TransactionMode::ReadOnly)?;

            let store = transaction.object_store("blockstore")?;

            let mut size: usize = 0;

            while let Some(cid) = block_list.next().await {
                let cid_val = JsValue::from_str(&cid.to_string());

                let block_size = store.get(cid_val)?.await.map(|val| {
                    val.and_then(|val| {
                        let bytes: Vec<u8> = serde_wasm_bindgen::from_value(val).ok()?;
                        Block::new(cid, bytes).map(|block| block.data().len()).ok()
                    })
                })?;

                if let Some(b_size) = block_size {
                    size += b_size;
                }
            }

            transaction.await?;

            Ok::<_, Box<dyn std::error::Error>>(size)
        })
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))
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

            transaction.commit()?.await?;

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

            transaction.commit()?.await?;

            Ok(())
        })
        .await
        .map_err(|e: Box<dyn std::error::Error>| anyhow::anyhow!("{e}"))
    }

    async fn remove_many(&self, mut blocks: BoxStream<'static, Cid>) -> BoxStream<'static, Cid> {
        let database = self.get_db().clone();
        SendWrapper::new(async_stream::stream! {
            let Ok(transaction) =
                database.transaction(&["blockstore"], TransactionMode::ReadWrite)
            else {
                return;
            };
            let Ok(store) = transaction.object_store("blockstore") else {
                return;
            };

            while let Some(cid) = blocks.next().await {
                let cid_val = JsValue::from_str(&cid.to_string());

                let Ok(request) = store.delete(cid_val) else {
                    continue;
                };

                if request.await.is_err() {
                    continue;
                }

                yield cid;
            }

            if let Ok(commit) = transaction.commit() {
                let _ = commit.await;
            }
        })
        .boxed()
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
}
