use std::{rc::Rc, str::FromStr, sync::OnceLock};

use crate::{
    repo::{BlockPut, BlockStore},
    Block, Error,
};
use async_trait::async_trait;
use futures::{channel::oneshot, stream::BoxStream, SinkExt, StreamExt};
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

#[async_trait]
impl BlockStore for IdbBlockStore {
    async fn init(&self) -> Result<(), Error> {
        let factory = self.factory.clone();
        let namespace = self.namespace.clone();
        let (tx, rx) = oneshot::channel();
        wasm_bindgen_futures::spawn_local(async move {
            let res = async {
                let mut request = factory.open(&namespace, None)?;

                request.on_upgrade_needed(|event| {
                    let db = event.database().unwrap();
                    db.create_object_store("blockstore", ObjectStoreParams::new())
                        .unwrap();
                });

                let database = request.await?;
                Ok::<_, Box<dyn std::error::Error>>(SendWrapper::new(Rc::new(database)))
            }
            .await
            .map_err(|e| anyhow::anyhow!("{e}"));
            _ = tx.send(res);
        });

        let db = rx.await??;
        self.database.get_or_init(|| db);
        Ok(())
    }

    async fn open(&self) -> Result<(), Error> {
        Ok(())
    }

    async fn contains(&self, cid: &Cid) -> Result<bool, Error> {
        let database = self.get_db().clone();
        let (tx, rx) = oneshot::channel();
        let cid = *cid;
        wasm_bindgen_futures::spawn_local(async move {
            let res = async {
                let transaction =
                    database.transaction(&["blockstore"], TransactionMode::ReadOnly)?;

                let store = transaction.object_store("blockstore")?;

                let cid = JsValue::from_str(&cid.to_string());

                let val = store.get(cid)?.await?;
                transaction.await?;
                Ok::<_, Box<dyn std::error::Error>>(val.is_some())
            }
            .await
            .map_err(|e| anyhow::anyhow!("{e}"));

            _ = tx.send(res);
        });

        rx.await?
    }

    async fn get(&self, cid: &Cid) -> Result<Option<Block>, Error> {
        let database = self.get_db().clone();
        let (tx, rx) = oneshot::channel();
        let cid = *cid;
        wasm_bindgen_futures::spawn_local(async move {
            let res = async {
                let transaction =
                    database.transaction(&["blockstore"], TransactionMode::ReadOnly)?;

                let store = transaction.object_store("blockstore")?;

                let cid_val = JsValue::from_str(&cid.to_string());

                let block = store.get(cid_val)?.await.map(|val| {
                    val.and_then(|val| {
                        let bytes: Vec<u8> = serde_wasm_bindgen::from_value(val).ok()?;
                        Block::new(cid, bytes).ok()
                    })
                })?;

                transaction.await?;
                Ok(block)
            }
            .await
            .map_err(|e: Box<dyn std::error::Error>| anyhow::anyhow!("{e}"));

            _ = tx.send(res);
        });

        rx.await?
    }

    async fn size(&self, cid: &[Cid]) -> Result<Option<usize>, Error> {
        let database = self.get_db().clone();
        let (tx, rx) = oneshot::channel();
        let cid = cid.to_vec();
        wasm_bindgen_futures::spawn_local(async move {
            let res = async {
                let transaction =
                    database.transaction(&["blockstore"], TransactionMode::ReadOnly)?;

                let store = transaction.object_store("blockstore")?;

                let mut size: usize = 0;

                for cid in cid {
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

                Ok((size > 0).then_some(size))
            }
            .await
            .map_err(|e: Box<dyn std::error::Error>| anyhow::anyhow!("{e}"));
            _ = tx.send(res);
        });

        rx.await?
    }

    async fn total_size(&self) -> Result<usize, Error> {
        let mut block_list = self.list().await;
        let database = self.get_db().clone();
        let (tx, rx) = oneshot::channel();
        wasm_bindgen_futures::spawn_local(async move {
            let res = async {
                let transaction =
                    database.transaction(&["blockstore"], TransactionMode::ReadOnly)?;

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
            }
            .await
            .map_err(|e| anyhow::anyhow!("{e}"));
            _ = tx.send(res);
        });

        rx.await?
    }

    async fn put(&self, block: &Block) -> Result<(Cid, BlockPut), Error> {
        let block = block.clone();
        if self.contains(block.cid()).await? {
            return Ok((*block.cid(), BlockPut::Existed));
        }

        let database = self.get_db().clone();
        let (tx, rx) = oneshot::channel();
        wasm_bindgen_futures::spawn_local(async move {
            let res = async {
                let transaction =
                    database.transaction(&["blockstore"], TransactionMode::ReadWrite)?;

                let store = transaction.object_store("blockstore")?;

                let block_val = serde_wasm_bindgen::to_value(block.data())?;

                let cid_val = JsValue::from_str(&block.cid().to_string());

                store.put(&block_val, Some(&cid_val))?.await?;

                transaction.commit()?.await?;

                Ok((*block.cid(), BlockPut::NewBlock))
            }
            .await
            .map_err(|e: Box<dyn std::error::Error>| anyhow::anyhow!("{e}"));

            _ = tx.send(res);
        });
        rx.await?
    }

    async fn remove(&self, cid: &Cid) -> Result<(), Error> {
        let database = self.get_db().clone();
        let (tx, rx) = oneshot::channel();
        let cid = *cid;
        wasm_bindgen_futures::spawn_local(async move {
            let res = async {
                let transaction =
                    database.transaction(&["blockstore"], TransactionMode::ReadWrite)?;

                let store = transaction.object_store("blockstore")?;

                let cid_val = JsValue::from_str(&cid.to_string());

                store.delete(cid_val)?.await?;

                transaction.commit()?.await?;

                Ok(())
            }
            .await
            .map_err(|e: Box<dyn std::error::Error>| anyhow::anyhow!("{e}"));

            _ = tx.send(res);
        });

        rx.await?
    }

    async fn remove_many(&self, mut blocks: BoxStream<'static, Cid>) -> BoxStream<'static, Cid> {
        let database = self.get_db().clone();
        let (mut tx, rx) = futures::channel::mpsc::channel(10);
        wasm_bindgen_futures::spawn_local(async move {
            let transaction = database
                .transaction(&["blockstore"], TransactionMode::ReadWrite)
                .unwrap();
            let store = transaction.object_store("blockstore").unwrap();

            while let Some(cid) = blocks.next().await {
                let cid_val = JsValue::from_str(&cid.to_string());

                let Ok(request) = store.delete(cid_val) else {
                    continue;
                };

                if request.await.is_err() {
                    continue;
                }

                _ = tx.send(cid).await;
            }

            transaction.commit().unwrap().await.unwrap();
        });
        rx.boxed()
    }

    async fn list(&self) -> BoxStream<'static, Cid> {
        let database = self.get_db().clone();
        let (mut tx, rx) = futures::channel::mpsc::channel(10);
        wasm_bindgen_futures::spawn_local(async move {
            let transaction = database
                .transaction(&["blockstore"], TransactionMode::ReadOnly)
                .unwrap();
            let store = transaction.object_store("blockstore").unwrap();
            let res = store.get_all_keys(None, None).unwrap();
            for cid in res
                .await
                .unwrap_or_default()
                .into_iter()
                .filter_map(|val| val.as_string())
                .filter_map(|cid_val| Cid::from_str(&cid_val).ok())
            {
                _ = tx.send(cid).await;
            }
        });
        rx.boxed()
    }
}
