use std::{rc::Rc, str::FromStr};

use async_trait::async_trait;

use crate::{
    repo::{BlockPut, BlockStore},
    Block, Error,
};
use futures::{
    channel::{
        mpsc::{Receiver, Sender},
        oneshot,
    },
    stream::BoxStream,
    SinkExt, StreamExt,
};
use idb::{Database, DatabaseEvent, Factory, ObjectStoreParams, TransactionMode};
use libipld::Cid;
use wasm_bindgen_futures::wasm_bindgen::JsValue;

const NAMESPACE: &str = "rust-block-store";

#[derive(Debug)]
pub struct IdbBlockStore {
    tx: Sender<IdbCommand>,
}

impl IdbBlockStore {
    pub fn new(namespace: Option<String>) -> Self {
        let (tx, rx) = futures::channel::mpsc::channel(0);

        wasm_bindgen_futures::spawn_local(async move {
            let task = IdbTask::new(&namespace.unwrap_or_else(|| NAMESPACE.to_string()), rx).await;
            task.run().await
        });

        Self { tx }
    }
}

#[async_trait]
impl BlockStore for IdbBlockStore {
    async fn init(&self) -> Result<(), Error> {
        Ok(())
    }

    async fn open(&self) -> Result<(), Error> {
        Ok(())
    }

    async fn contains(&self, cid: &Cid) -> Result<bool, Error> {
        let (tx, rx) = oneshot::channel();
        _ = self
            .tx
            .clone()
            .send(IdbCommand::Contains {
                cid: *cid,
                response: tx,
            })
            .await;
        let res = rx.await.unwrap();
        Ok(res)
    }

    async fn get(&self, cid: &Cid) -> Result<Option<Block>, Error> {
        let (tx, rx) = oneshot::channel();
        _ = self
            .tx
            .clone()
            .send(IdbCommand::Get {
                cid: *cid,
                response: tx,
            })
            .await;
        rx.await.unwrap()
    }

    async fn size(&self, cid: &[Cid]) -> Result<Option<usize>, Error> {
        let (tx, rx) = oneshot::channel();
        _ = self
            .tx
            .clone()
            .send(IdbCommand::Size {
                cid: cid.to_owned(),
                response: tx,
            })
            .await;
        rx.await.unwrap()
    }

    async fn total_size(&self) -> Result<usize, Error> {
        let (tx, rx) = oneshot::channel();
        _ = self
            .tx
            .clone()
            .send(IdbCommand::TotalSize { response: tx })
            .await;
        Ok(rx.await.unwrap())
    }

    async fn put(&self, block: Block) -> Result<(Cid, BlockPut), Error> {
        let (tx, rx) = oneshot::channel();
        _ = self
            .tx
            .clone()
            .send(IdbCommand::Put {
                block,
                response: tx,
            })
            .await;
        rx.await.unwrap()
    }

    async fn remove(&self, cid: &Cid) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        _ = self
            .tx
            .clone()
            .send(IdbCommand::Remove {
                cid: *cid,
                response: tx,
            })
            .await;
        rx.await.unwrap()
    }

    async fn remove_many(&self, blocks: BoxStream<'static, Cid>) -> BoxStream<'static, Cid> {
        let (tx, rx) = oneshot::channel();
        _ = self
            .tx
            .clone()
            .send(IdbCommand::RemoveMany {
                blocks,
                response: tx,
            })
            .await;
        rx.await.unwrap()
    }

    async fn list(&self) -> BoxStream<'static, Cid> {
        let (tx, rx) = oneshot::channel();
        _ = self
            .tx
            .clone()
            .send(IdbCommand::List { response: tx })
            .await;
        rx.await.unwrap()
    }
}

enum IdbCommand {
    Contains {
        cid: Cid,
        response: oneshot::Sender<bool>,
    },
    Get {
        cid: Cid,
        response: oneshot::Sender<Result<Option<Block>, Error>>,
    },
    Size {
        cid: Vec<Cid>,
        response: oneshot::Sender<Result<Option<usize>, Error>>,
    },
    TotalSize {
        response: oneshot::Sender<usize>,
    },
    Put {
        block: Block,
        response: oneshot::Sender<Result<(Cid, BlockPut), Error>>,
    },
    Remove {
        cid: Cid,
        response: oneshot::Sender<Result<(), Error>>,
    },
    RemoveMany {
        blocks: BoxStream<'static, Cid>,
        response: oneshot::Sender<BoxStream<'static, Cid>>,
    },
    List {
        response: oneshot::Sender<BoxStream<'static, Cid>>,
    },
}

struct IdbTask {
    database: Rc<Database>,
    command_rx: Receiver<IdbCommand>,
}

impl IdbTask {
    pub async fn new(namespace: &str, rx: Receiver<IdbCommand>) -> Self {
        let factory = Factory::new().unwrap();
        let mut request = factory.open(namespace, None).unwrap();

        request.on_upgrade_needed(|event| {
            let db = event.database().unwrap();
            db.create_object_store("blockstore", ObjectStoreParams::new())
                .unwrap();
        });

        let db = request.await.unwrap();

        let task = Self {
            database: Rc::new(db),
            command_rx: rx,
        };
        task
    }

    async fn run(mut self) {
        while let Some(command) = self.command_rx.next().await {
            match command {
                IdbCommand::Contains { cid, response } => {
                    _ = response.send(self.contains(cid).await.unwrap_or_default());
                }
                IdbCommand::Get { cid, response } => {
                    _ = response.send(self.get(cid).await.map_err(|e| anyhow::anyhow!("{e}")));
                }
                IdbCommand::Size { cid, response } => {
                    _ = response.send(self.size(cid).await.map_err(|e| anyhow::anyhow!("{e}")));
                }
                IdbCommand::TotalSize { response } => {
                    _ = response.send(self.total_size().await.unwrap_or_default());
                }
                IdbCommand::Put { block, response } => {
                    _ = response.send(self.put(block).await.map_err(|e| anyhow::anyhow!("{e}")));
                }
                IdbCommand::Remove { cid, response } => {
                    _ = response.send(self.remove(cid).await.map_err(|e| anyhow::anyhow!("{e}")));
                }
                IdbCommand::RemoveMany { blocks, response } => {
                    _ = response.send(self.remove_many(blocks).await);
                }
                IdbCommand::List { response } => {
                    _ = response.send(self.list().await);
                }
            }
        }
    }

    async fn contains(&self, cid: Cid) -> Result<bool, Box<dyn std::error::Error>> {
        let transaction = self
            .database
            .transaction(&["blockstore"], TransactionMode::ReadOnly)?;

        let store = transaction.object_store("blockstore")?;

        let cid = JsValue::from_str(&cid.to_string());

        let val = store.get(cid)?.await?;
        transaction.await?;
        Ok(val.is_some())
    }

    async fn get(&self, cid: Cid) -> Result<Option<Block>, Box<dyn std::error::Error>> {
        let transaction = self
            .database
            .transaction(&["blockstore"], TransactionMode::ReadOnly)?;

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

    async fn size(&self, cid: Vec<Cid>) -> Result<Option<usize>, Box<dyn std::error::Error>> {
        let transaction = self
            .database
            .transaction(&["blockstore"], TransactionMode::ReadOnly)?;

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

    async fn total_size(&self) -> Result<usize, Box<dyn std::error::Error>> {
        unimplemented!()
    }

    async fn put(&self, block: Block) -> Result<(Cid, BlockPut), Box<dyn std::error::Error>> {
        if self.contains(*block.cid()).await? {
            return Ok((*block.cid(), BlockPut::Existed));
        }

        let transaction = self
            .database
            .transaction(&["blockstore"], TransactionMode::ReadWrite)?;

        let store = transaction.object_store("blockstore")?;

        let block_val = serde_wasm_bindgen::to_value(block.data())?;

        let cid_val = JsValue::from_str(&block.cid().to_string());

        store.put(&block_val, Some(&cid_val))?.await?;

        transaction.commit()?.await?;

        Ok((*block.cid(), BlockPut::NewBlock))
    }

    async fn remove(&self, cid: Cid) -> Result<(), Box<dyn std::error::Error>> {
        let transaction = self
            .database
            .transaction(&["blockstore"], TransactionMode::ReadWrite)?;

        let store = transaction.object_store("blockstore")?;

        let cid_val = JsValue::from_str(&cid.to_string());

        store.delete(cid_val)?.await?;

        transaction.commit()?.await?;

        Ok(())
    }

    async fn remove_many(&self, mut blocks: BoxStream<'static, Cid>) -> BoxStream<'static, Cid> {
        let database = self.database.clone();
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
        let database = self.database.clone();
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
