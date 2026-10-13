use std::{
    collections::BTreeSet,
    rc::Rc,
    sync::{
        OnceLock,
        atomic::{AtomicBool, Ordering},
    },
};

use crate::repo::store::idb::{WriteTransaction, finish_transaction, finish_write_transaction};
use crate::{
    Error, PinKind, PinMode,
    repo::{BlockStore, DataStore, PinStore, References},
};
use futures::{StreamExt, TryStreamExt, stream::BoxStream};
use idb::{Database, DatabaseEvent, Factory, ObjectStoreParams, TransactionMode};
use ipld_core::cid::Cid;
use send_wrapper::SendWrapper;
mod pins;
use pins::{
    collect_block_references, delete_pin, read_indirect, read_pin, read_pin_records, write_direct,
    write_indirect, write_pin_records, write_recursive,
};

const NAMESPACE: &str = "rust-datastore-store";

#[derive(Debug)]
pub struct IdbDataStore {
    factory: send_wrapper::SendWrapper<Rc<Factory>>,
    database: OnceLock<send_wrapper::SendWrapper<Rc<Database>>>,
    namespace: String,
    pins_ready: AtomicBool,
}

impl IdbDataStore {
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
            pins_ready: AtomicBool::new(false),
        }
    }

    pub fn get_db(&self) -> &send_wrapper::SendWrapper<Rc<Database>> {
        self.database.get().expect("initialized")
    }

    /// Rebuilds pin ownership after both stores are initialized.
    /// Note that should run before pin operations or GC.
    pub async fn rebuild_pins(&self, block_store: &impl BlockStore) -> Result<(), Error> {
        self.pins_ready.store(false, Ordering::SeqCst);
        SendWrapper::new(async move {
            let db = self.get_db();
            for _ in 0..3 {
                let transaction = db.transaction(&["pinstore"], TransactionMode::ReadOnly)?;
                let store = transaction.object_store("pinstore")?;
                let snapshot = read_pin_records(&store, true).await?;
                finish_transaction(transaction).await?;

                let mut rebuilt = snapshot.clone();
                for (root, references) in &mut rebuilt.recursive {
                    if references.is_none() {
                        *references = Some(collect_block_references(block_store, *root).await?);
                    }
                }
                rebuilt.indirect = rebuilt.owners()?;
                rebuilt.legacy = false;

                let transaction = WriteTransaction::new(
                    db.transaction(&["pinstore"], TransactionMode::ReadWrite)?,
                );
                let store = transaction.object_store("pinstore")?;
                let result = async {
                    if read_pin_records(&store, true).await? != snapshot {
                        return Ok(false);
                    }
                    if rebuilt != snapshot {
                        write_pin_records(&store, &rebuilt).await?;
                    }
                    Ok::<_, Box<dyn std::error::Error>>(true)
                }
                .await;

                if finish_write_transaction(transaction, result).await? {
                    self.pins_ready.store(true, Ordering::SeqCst);
                    return Ok(());
                }
            }
            Err(std::io::Error::other("pins changed while rebuilding ownership").into())
        })
        .await
        .map_err(|e: Box<dyn std::error::Error>| anyhow::anyhow!("{e}"))
    }

    fn ensure_pins_ready(&self) -> Result<(), Error> {
        if !self.pins_ready.load(Ordering::SeqCst) {
            anyhow::bail!("pin ownership has not been rebuilt");
        }
        Ok(())
    }
}

impl DataStore for IdbDataStore {
    async fn init(&self) -> Result<(), Error> {
        SendWrapper::new(async move {
            let mut request = self.factory.open(&self.namespace, None)?;

            request.on_upgrade_needed(|event| {
                let db = event.database().unwrap();
                db.create_object_store("datastore", ObjectStoreParams::new())
                    .unwrap();
                db.create_object_store("pinstore", ObjectStoreParams::new())
                    .unwrap();
            });

            let database = request.await?;

            self.database
                .get_or_init(|| SendWrapper::new(Rc::new(database)));
            let transaction = self
                .get_db()
                .transaction(&["pinstore"], TransactionMode::ReadOnly)?;
            let store = transaction.object_store("pinstore")?;
            let pins = read_pin_records(&store, true).await?;
            finish_transaction(transaction).await?;
            self.pins_ready
                .store(pins.validate().is_ok(), Ordering::SeqCst);
            Ok(())
        })
        .await
        .map_err(|e: Box<dyn std::error::Error>| anyhow::anyhow!("{e}"))
    }

    /// Checks if a key is present in the datastore.
    async fn contains(&self, key: &[u8]) -> Result<bool, Error> {
        SendWrapper::new(async move {
            let database = self.get_db();
            let transaction = database.transaction(&["datastore"], TransactionMode::ReadOnly)?;

            let store = transaction.object_store("datastore")?;

            let key = serde_wasm_bindgen::to_value(&key)?;

            let val = store.get(key)?.await?;
            finish_transaction(transaction).await?;
            Ok::<_, Box<dyn std::error::Error>>(val.is_some())
        })
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))
    }

    /// Returns the value associated with a key from the datastore.
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        SendWrapper::new(async move {
            let database = self.get_db();
            let transaction = database.transaction(&["datastore"], TransactionMode::ReadOnly)?;

            let store = transaction.object_store("datastore")?;

            let key = serde_wasm_bindgen::to_value(&key)?;

            let block = store
                .get(key)?
                .await?
                .map(serde_wasm_bindgen::from_value::<Vec<u8>>)
                .transpose()?;

            finish_transaction(transaction).await?;
            Ok::<_, Box<dyn std::error::Error>>(block)
        })
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))
    }

    /// Puts the value under the key in the datastore.
    async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        SendWrapper::new(async move {
            let database = self.get_db();
            let transaction = database.transaction(&["datastore"], TransactionMode::ReadWrite)?;

            let store = transaction.object_store("datastore")?;

            let key = serde_wasm_bindgen::to_value(&key)?;
            let val = serde_wasm_bindgen::to_value(&value)?;

            store.put(&val, Some(&key))?.await?;

            finish_transaction(transaction.commit()?).await?;

            Ok::<_, Box<dyn std::error::Error>>(())
        })
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))
    }

    /// Removes a key-value pair from the datastore.
    async fn remove(&self, key: &[u8]) -> Result<(), Error> {
        SendWrapper::new(async move {
            let database = self.get_db();
            let transaction = database.transaction(&["datastore"], TransactionMode::ReadWrite)?;

            let store = transaction.object_store("datastore")?;

            let key = serde_wasm_bindgen::to_value(&key)?;

            store.delete(key)?.await?;

            finish_transaction(transaction.commit()?).await?;

            Ok::<_, Box<dyn std::error::Error>>(())
        })
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))
    }

    async fn iter(&self) -> BoxStream<'static, (Vec<u8>, Vec<u8>)> {
        let database = self.get_db().clone();
        SendWrapper::new(async_stream::stream! {
            let Ok(transaction) = database.transaction(&["datastore"], TransactionMode::ReadOnly)
            else {
                return;
            };
            let Ok(store) = transaction.object_store("datastore") else {
                return;
            };
            let Ok(key_request) = store.get_all_keys(None, None) else {
                return;
            };
            let Ok(value_request) = store.get_all(None, None) else {
                return;
            };
            let keys = key_request.await.unwrap_or_default();
            let values = value_request.await.unwrap_or_default();

            for (raw_key, raw_value) in keys.into_iter().zip(values) {
                let (Ok(key), Ok(value)) = (
                    serde_wasm_bindgen::from_value::<Vec<u8>>(raw_key),
                    serde_wasm_bindgen::from_value::<Vec<u8>>(raw_value),
                ) else {
                    continue;
                };
                yield (key, value);
            }
        })
        .boxed()
    }
}

impl PinStore for IdbDataStore {
    async fn is_pinned(&self, cid: &Cid) -> Result<bool, Error> {
        self.ensure_pins_ready()?;
        SendWrapper::new(async move {
            let transaction = self
                .get_db()
                .transaction(&["pinstore"], TransactionMode::ReadOnly)?;
            let store = transaction.object_store("pinstore")?;
            let pinned = read_pin(&store, cid).await?.kind(None).is_some();
            finish_transaction(transaction).await?;
            Ok::<_, Box<dyn std::error::Error>>(pinned)
        })
        .await
        .map_err(|error| anyhow::anyhow!("{error}"))
    }

    async fn insert_direct_pin(&self, target: &Cid) -> Result<(), Error> {
        self.ensure_pins_ready()?;
        SendWrapper::new(async move {
            let transaction = WriteTransaction::new(
                self.get_db()
                    .transaction(&["pinstore"], TransactionMode::ReadWrite)?,
            );
            let store = transaction.object_store("pinstore")?;
            let result = async {
                let pin = read_pin(&store, target).await?;
                if pin.recursive.is_some() {
                    return Err(std::io::Error::other("already pinned recursively").into());
                }
                if !pin.direct {
                    write_direct(&store, target).await?;
                }
                Ok::<_, Box<dyn std::error::Error>>(())
            }
            .await;
            finish_write_transaction(transaction, result).await
        })
        .await
        .map_err(|error| anyhow::anyhow!("{error}"))
    }

    async fn insert_recursive_pin(
        &self,
        target: &Cid,
        referenced: References<'_>,
    ) -> Result<(), Error> {
        self.ensure_pins_ready()?;
        let mut references = referenced.try_collect::<BTreeSet<_>>().await?;
        references.remove(target);

        SendWrapper::new(async move {
            let transaction = WriteTransaction::new(
                self.get_db()
                    .transaction(&["pinstore"], TransactionMode::ReadWrite)?,
            );
            let store = transaction.object_store("pinstore")?;
            let result = async {
                let pin = read_pin(&store, target).await?;
                if pin.recursive.is_some() {
                    return Ok(());
                }
                if pin.direct {
                    delete_pin(&store, target, PinMode::Direct).await?;
                }
                write_recursive(&store, target, &references).await?;
                for cid in &references {
                    let mut roots = read_indirect(&store, cid).await?;
                    roots.insert(*target);
                    write_indirect(&store, cid, &roots).await?;
                }
                Ok::<_, Box<dyn std::error::Error>>(())
            }
            .await;
            finish_write_transaction(transaction, result).await
        })
        .await
        .map_err(|error| anyhow::anyhow!("{error}"))
    }

    async fn remove_direct_pin(&self, target: &Cid) -> Result<(), Error> {
        self.ensure_pins_ready()?;
        SendWrapper::new(async move {
            let transaction = WriteTransaction::new(
                self.get_db()
                    .transaction(&["pinstore"], TransactionMode::ReadWrite)?,
            );
            let store = transaction.object_store("pinstore")?;
            let result = async {
                let pin = read_pin(&store, target).await?;
                if pin.recursive.is_some() {
                    return Err(std::io::Error::other("is pinned recursively").into());
                }
                if !pin.direct {
                    return Err(std::io::Error::other("not pinned or pinned indirectly").into());
                }
                delete_pin(&store, target, PinMode::Direct).await
            }
            .await;
            finish_write_transaction(transaction, result).await
        })
        .await
        .map_err(|error| anyhow::anyhow!("{error}"))
    }

    async fn remove_recursive_pin(
        &self,
        target: &Cid,
        _referenced: References<'_>,
    ) -> Result<(), Error> {
        self.ensure_pins_ready()?;
        SendWrapper::new(async move {
            let transaction = WriteTransaction::new(
                self.get_db()
                    .transaction(&["pinstore"], TransactionMode::ReadWrite)?,
            );
            let store = transaction.object_store("pinstore")?;
            let result = async {
                let pin = read_pin(&store, target).await?;
                let Some(references) = pin.recursive else {
                    if pin.direct {
                        return delete_pin(&store, target, PinMode::Direct).await;
                    }
                    return Err(std::io::Error::other("not pinned or pinned indirectly").into());
                };
                if pin.direct {
                    delete_pin(&store, target, PinMode::Direct).await?;
                }
                delete_pin(&store, target, PinMode::Recursive).await?;
                for cid in &references {
                    let mut roots = read_indirect(&store, cid).await?;
                    if !roots.remove(target) {
                        return Err(std::io::Error::other(format!(
                            "missing pin owner {target} for {cid}"
                        ))
                        .into());
                    }
                    write_indirect(&store, cid, &roots).await?;
                }
                Ok::<_, Box<dyn std::error::Error>>(())
            }
            .await;
            finish_write_transaction(transaction, result).await
        })
        .await
        .map_err(|error| anyhow::anyhow!("{error}"))
    }

    async fn list(
        &self,
        requirement: Option<PinMode>,
    ) -> BoxStream<'static, Result<(Cid, PinMode), Error>> {
        let pins = SendWrapper::new(async move {
            self.ensure_pins_ready()?;
            let transaction = self
                .get_db()
                .transaction(&["pinstore"], TransactionMode::ReadOnly)?;
            let store = transaction.object_store("pinstore")?;
            let pins = read_pin_records(&store, false).await?;
            pins.validate()?;
            finish_transaction(transaction).await?;
            Ok::<_, Box<dyn std::error::Error>>(pins.list(requirement))
        })
        .await
        .map_err(|error| anyhow::anyhow!("{error}"));

        match pins {
            Ok(pins) => futures::stream::iter(pins.into_iter().map(Ok)).boxed(),
            Err(error) => futures::stream::once(futures::future::ready(Err(error))).boxed(),
        }
    }

    async fn query(
        &self,
        ids: Vec<Cid>,
        requirement: Option<PinMode>,
    ) -> Result<Vec<(Cid, PinKind<Cid>)>, Error> {
        self.ensure_pins_ready()?;
        SendWrapper::new(async move {
            let transaction = self
                .get_db()
                .transaction(&["pinstore"], TransactionMode::ReadOnly)?;
            let store = transaction.object_store("pinstore")?;
            let mut pins = Vec::with_capacity(ids.len());
            for cid in ids {
                if let Some(kind) = read_pin(&store, &cid).await?.kind(requirement) {
                    pins.push((cid, kind));
                }
            }
            finish_transaction(transaction).await?;
            Ok::<_, Box<dyn std::error::Error>>(pins)
        })
        .await
        .map_err(|error| anyhow::anyhow!("{error}"))
    }
}
