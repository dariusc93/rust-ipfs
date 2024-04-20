use std::{collections::BTreeSet, rc::Rc, str::FromStr, sync::OnceLock};

use async_trait::async_trait;

use crate::{
    repo::{DataStore, PinModeRequirement, PinStore, References},
    Error, PinKind, PinMode,
};
use futures::{channel::oneshot, stream::BoxStream, SinkExt, StreamExt, TryStreamExt};
use idb::{
    Database, DatabaseEvent, Factory, ObjectStore, ObjectStoreParams, Transaction, TransactionMode,
};
use libipld::Cid;
use send_wrapper::SendWrapper;
use wasm_bindgen_futures::wasm_bindgen::JsValue;

const NAMESPACE: &str = "rust-datastore-store";

#[derive(Debug)]
pub struct IdbDataStore {
    factory: send_wrapper::SendWrapper<Rc<Factory>>,
    database: OnceLock<send_wrapper::SendWrapper<Rc<Database>>>,
    namespace: String,
}

impl IdbDataStore {
    pub fn new(namespace: Option<String>) -> Self {
        let namespace = namespace.unwrap_or_else(|| NAMESPACE.to_string());

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
impl DataStore for IdbDataStore {
    async fn init(&self) -> Result<(), Error> {
        let factory = self.factory.clone();
        let namespace = self.namespace.clone();
        let (tx, rx) = oneshot::channel();
        wasm_bindgen_futures::spawn_local(async move {
            let res = async {
                let mut request = factory.open(&namespace, None)?;

                request.on_upgrade_needed(|event| {
                    let db = event.database().unwrap();
                    db.create_object_store("datastore", ObjectStoreParams::new())
                        .unwrap();
                    db.create_object_store("pinstore", ObjectStoreParams::new())
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

    /// Checks if a key is present in the datastore.
    async fn contains(&self, key: &[u8]) -> Result<bool, Error> {
        let database = self.get_db().to_owned();
        let key = key.to_owned();
        let (tx, rx) = oneshot::channel();
        wasm_bindgen_futures::spawn_local(async move {
            let res = async {
                let transaction =
                    database.transaction(&["datastore"], TransactionMode::ReadOnly)?;

                let store = transaction.object_store("datastore")?;

                let key = serde_wasm_bindgen::to_value(&key)?;

                let val = store.get(key)?.await?;
                transaction.await?;
                Ok::<_, Box<dyn std::error::Error>>(val.is_some())
            }
            .await
            .map_err(|e| anyhow::anyhow!("{e}"));

            _ = tx.send(res);
        });

        rx.await?
    }

    /// Returns the value associated with a key from the datastore.
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        let database = self.get_db().to_owned();
        let key = key.to_owned();
        let (tx, rx) = oneshot::channel();
        wasm_bindgen_futures::spawn_local(async move {
            let res = async {
                let transaction =
                    database.transaction(&["datastore"], TransactionMode::ReadOnly)?;

                let store = transaction.object_store("datastore")?;

                let key = serde_wasm_bindgen::to_value(&key)?;

                let block = store.get(key)?.await.map(|val| {
                    val.and_then(|val| {
                        let bytes: Vec<u8> = serde_wasm_bindgen::from_value(val).ok()?;
                        Some(bytes)
                    })
                })?;

                transaction.await?;
                Ok::<_, Box<dyn std::error::Error>>(block)
            }
            .await
            .map_err(|e| anyhow::anyhow!("{e}"));

            _ = tx.send(res);
        });

        rx.await?
    }

    /// Puts the value under the key in the datastore.
    async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        let database = self.get_db().to_owned();
        let key = key.to_owned();
        let value = value.to_owned();
        let (tx, rx) = oneshot::channel();
        wasm_bindgen_futures::spawn_local(async move {
            let res = async {
                let transaction =
                    database.transaction(&["datastore"], TransactionMode::ReadWrite)?;

                let store = transaction.object_store("datastore")?;

                let key = serde_wasm_bindgen::to_value(&key)?;
                let val = serde_wasm_bindgen::to_value(&value)?;

                store.put(&val, Some(&key))?.await?;

                transaction.commit()?.await?;

                Ok::<_, Box<dyn std::error::Error>>(())
            }
            .await
            .map_err(|e| anyhow::anyhow!("{e}"));

            _ = tx.send(res);
        });

        rx.await?
    }

    /// Removes a key-value pair from the datastore.
    async fn remove(&self, key: &[u8]) -> Result<(), Error> {
        let database = self.get_db().to_owned();
        let key = key.to_owned();
        let (tx, rx) = oneshot::channel();
        wasm_bindgen_futures::spawn_local(async move {
            let res = async {
                let transaction =
                    database.transaction(&["datastore"], TransactionMode::ReadWrite)?;

                let store = transaction.object_store("datastore")?;

                let key = serde_wasm_bindgen::to_value(&key)?;

                store.delete(key)?.await?;

                transaction.commit()?.await?;

                Ok::<_, Box<dyn std::error::Error>>(())
            }
            .await
            .map_err(|e| anyhow::anyhow!("{e}"));

            _ = tx.send(res);
        });

        rx.await?
    }

    async fn iter(&self) -> BoxStream<'static, (Vec<u8>, Vec<u8>)> {
        let database = self.get_db().clone();
        let (mut tx, rx) = futures::channel::mpsc::channel(10);
        wasm_bindgen_futures::spawn_local(async move {
            let transaction = database
                .transaction(&["datastore"], TransactionMode::ReadOnly)
                .unwrap();
            let store = transaction.object_store("datastore").unwrap();
            let key_res = store
                .get_all_keys(None, None)
                .unwrap()
                .await
                .unwrap()
                .into_iter()
                .filter_map(|val| serde_wasm_bindgen::from_value::<Vec<u8>>(val).ok())
                .collect::<Vec<_>>();

            let res = store
                .get_all(None, None)
                .unwrap()
                .await
                .unwrap()
                .into_iter()
                .filter_map(|val| serde_wasm_bindgen::from_value::<Vec<u8>>(val).ok())
                .collect::<Vec<_>>();

            for kv in key_res.into_iter().zip(res) {
                _ = tx.send(kv).await;
            }
        });
        rx.boxed()
    }
}

// in the transactional parts of the [`Infallible`] is used to signal there is no additional
// custom error, not that the transaction was infallible in itself.

#[async_trait]
impl PinStore for IdbDataStore {
    async fn is_pinned(&self, cid: &Cid) -> Result<bool, Error> {
        let cid = cid.to_owned();
        let database = self.get_db().to_owned();
        let (tx, rx) = oneshot::channel();
        wasm_bindgen_futures::spawn_local(async move {
            let res = async {
                let transaction =
                    database.transaction(&["pinstore"], TransactionMode::ReadOnly)?;

                let store = transaction.object_store("pinstore")?;

                let key = JsValue::from_str(&cid.to_string());

                let val = store.get(key)?.await?;
                transaction.await?;
                Ok::<_, Box<dyn std::error::Error>>(val.is_some())
            }
            .await
            .map_err(|e| anyhow::anyhow!("{e}"));

            _ = tx.send(res);
        });

        rx.await?
    }

    async fn insert_direct_pin(&self, target: &Cid) -> Result<(), Error> {
        let target = target.to_owned();
        let db = self.get_db().to_owned();
        let (tx, rx) = oneshot::channel();
        wasm_bindgen_futures::spawn_local(async move {
            let res = async {
                let transaction = db.transaction(&["pinstore"], TransactionMode::ReadWrite)?;

                let store = transaction.object_store("pinstore")?;

                let already_pinned = get_pinned_mode(&transaction, &store, &target).await?;

                match already_pinned {
                    Some((PinMode::Direct, _)) => return Ok(()),
                    Some((PinMode::Recursive, _)) => {
                        let r = || Err(anyhow::anyhow!("already pinned recursively"));
                        r()?;
                    }
                    Some((PinMode::Indirect, key)) => {
                        // TODO: I think the direct should live alongside the indirect?
                        let key = serde_wasm_bindgen::to_value(&key)?;
                        store.delete(key)?.await?;
                    }
                    None => {}
                }

                let direct_key = get_pin_key(&target, &PinMode::Direct);
                let key = serde_wasm_bindgen::to_value(&direct_key).unwrap();
                let val = serde_wasm_bindgen::to_value(direct_value()).unwrap();

                store.put(&val, Some(&key))?.await?;
                transaction.commit()?.await?;

                Ok::<_, Box<dyn std::error::Error>>(())
            }
            .await
            .map_err(|e| anyhow::anyhow!("{e}"));

            _ = tx.send(res)
        });

        rx.await?
    }

    async fn insert_recursive_pin(
        &self,
        target: &Cid,
        referenced: References<'_>,
    ) -> Result<(), Error> {
        // since the transaction can be retried multiple times, we need to collect these and keep
        // iterating it until there is no conflict.
        let set = referenced.try_collect::<BTreeSet<_>>().await?;

        let target = target.to_owned();
        let db = self.get_db().to_owned();
        let (tx, rx) = oneshot::channel();

        // the transaction is not infallible but there is no additional error we return
        wasm_bindgen_futures::spawn_local(async move {
            let res = async {
                let transaction = db.transaction(&["pinstore"], TransactionMode::ReadWrite)?;

                let store = transaction.object_store("pinstore")?;

                let already_pinned = get_pinned_mode(&transaction, &store, &target).await?;

                match already_pinned {
                    Some((PinMode::Recursive, _)) => return Ok(()),
                    Some((PinMode::Direct, key)) | Some((PinMode::Indirect, key)) => {
                        // FIXME: this is probably another lapse in tests that both direct and
                        // indirect can be removed when inserting recursive?
                        let key = serde_wasm_bindgen::to_value(&key)?;
                        store.delete(key)?.await?;
                    }
                    None => {}
                }

                let recursive_key = get_pin_key(&target, &PinMode::Recursive);
                let key = serde_wasm_bindgen::to_value(&recursive_key)?;
                let val = serde_wasm_bindgen::to_value(recursive_value())?;
                store.put(&val, Some(&key))?.await?;

                let target_value = indirect_value(&target);

                // cannot use into_iter here as the transactions are retryable
                for cid in set.iter() {
                    let indirect_key = get_pin_key(cid, &PinMode::Indirect);

                    if get_pinned_mode(&transaction, &store, cid).await?.is_some() {
                        // TODO: quite costly to do the get_pinned_mode here
                        continue;
                    }

                    let indirect_key = serde_wasm_bindgen::to_value(&indirect_key)?;
                    let target_value = serde_wasm_bindgen::to_value(&target_value)?;
                    store.put(&target_value, Some(&indirect_key))?.await?;
                }

                transaction.commit()?.await?;
                Ok::<_, Box<dyn std::error::Error>>(())
            }
            .await
            .map_err(|e| anyhow::anyhow!("{e}"));

            _ = tx.send(res);
        });

        rx.await?
    }

    async fn remove_direct_pin(&self, target: &Cid) -> Result<(), Error> {
        let target = target.to_owned();
        let db = self.get_db().to_owned();

        let (tx, rx) = oneshot::channel();

        wasm_bindgen_futures::spawn_local(async move {
            let res = async {
                let transaction = db.transaction(&["pinstore"], TransactionMode::ReadWrite)?;

                let store = transaction.object_store("pinstore")?;

                if is_not_pinned_or_pinned_indirectly(&transaction, &store, &target).await? {
                    let r = || Err(anyhow::anyhow!("not pinned or pinned indirectly"));
                    r()?;
                }

                let key = get_pin_key(&target, &PinMode::Direct);
                let key = serde_wasm_bindgen::to_value(&key)?;

                store.delete(key)?.await?;

                transaction.commit()?.await?;
                Ok::<_, Box<dyn std::error::Error>>(())
            }
            .await
            .map_err(|e| anyhow::anyhow!("{e}"));

            _ = tx.send(res)
        });

        rx.await?
    }

    async fn remove_recursive_pin(
        &self,
        target: &Cid,
        referenced: References<'_>,
    ) -> Result<(), Error> {
        // TODO: is this "in the same transaction" as the batch which is created?
        let set = referenced.try_collect::<BTreeSet<_>>().await?;

        let target = target.to_owned();
        let db = self.get_db().to_owned();
        let (tx, rx) = oneshot::channel();

        wasm_bindgen_futures::spawn_local(async move {
            let res = async {
                let transaction = db.transaction(&["pinstore"], TransactionMode::ReadWrite)?;

                let store = transaction.object_store("pinstore")?;

                if is_not_pinned_or_pinned_indirectly(&transaction, &store, &target).await? {
                    let r = || Err(anyhow::anyhow!("not pinned or pinned indirectly"));
                    r()?;
                }

                let key = get_pin_key(&target, &PinMode::Recursive);
                let key = serde_wasm_bindgen::to_value(&key)?;

                store.delete(key)?.await?;

                for cid in &set {
                    let already_pinned = get_pinned_mode(&transaction, &store, cid).await?;

                    match already_pinned {
                        Some((PinMode::Recursive, _)) | Some((PinMode::Direct, _)) => continue, // this should be unreachable
                        Some((PinMode::Indirect, key)) => {
                            // FIXME: not really sure of this but it might be that recursive removed
                            // the others...?
                            let key = serde_wasm_bindgen::to_value(&key)?;
                            store.delete(key)?.await?;
                        }
                        None => {}
                    }
                }

                transaction.commit()?.await?;

                Ok::<_, Box<dyn std::error::Error>>(())
            }
            .await
            .map_err(|e| anyhow::anyhow!("{e}"));

            _ = tx.send(res);
        });

        rx.await?
    }

    async fn list(
        &self,
        requirement: Option<PinMode>,
    ) -> BoxStream<'static, Result<(Cid, PinMode), Error>> {
        let db = self.get_db().to_owned();

        let (mut tx, rx) = futures::channel::mpsc::channel(1);
        let requirement = PinModeRequirement::from(requirement);

        wasm_bindgen_futures::spawn_local(async move {
            let transaction = db
                .transaction(&["datastore"], TransactionMode::ReadOnly)
                .unwrap();
            let store = transaction.object_store("datastore").unwrap();

            let res = store.get_all_keys(None, None).unwrap().await.unwrap();

            for k in res.into_iter().filter_map(|val| val.as_string()) {
                let k = k.as_bytes();
                if !k.starts_with(b"pin.") || k.len() < 7 {
                    let _ = tx.send(Err(anyhow::anyhow!("invalid pin: {:?}", k))).await;
                    return;
                }

                let mode = match k[4] {
                    b'd' => PinMode::Direct,
                    b'r' => PinMode::Recursive,
                    b'i' => PinMode::Indirect,
                    x => {
                        _ = tx
                            .send(Err(anyhow::anyhow!("invalid pinmode: {}", x as char)))
                            .await;
                        return;
                    }
                };

                if !requirement.matches(&mode) {
                    continue;
                } else {
                    let cid = std::str::from_utf8(&k[6..])
                        .map_err(Error::from)
                        .and_then(|x| Cid::from_str(x).map_err(Error::from))
                        .map_err(|e| e.context("failed to read pin:".to_string()))
                        .map(move |cid| (cid, mode));

                    _ = tx.send(cid).await;
                }
            }
        });

        rx.boxed()
    }

    async fn query(
        &self,
        ids: Vec<Cid>,
        requirement: Option<PinMode>,
    ) -> Result<Vec<(Cid, PinKind<Cid>)>, Error> {
        let requirement = PinModeRequirement::from(requirement);

        let db = self.get_db().to_owned();
        let (tx, rx) = oneshot::channel();
        wasm_bindgen_futures::spawn_local(async move {
            let res = async {
                let transaction = db.transaction(&["pinstore"], TransactionMode::ReadOnly)?;

                let store = transaction.object_store("pinstore")?;

                let mut modes = Vec::with_capacity(ids.len());

                for id in ids.iter() {
                    let mode_and_key = get_pinned_mode(&transaction, &store, id).await?;

                    let matched = match mode_and_key {
                        Some((pin_mode, key)) if requirement.matches(&pin_mode) => match pin_mode {
                            PinMode::Direct => Some(PinKind::Direct),
                            PinMode::Recursive => Some(PinKind::Recursive(0)),
                            PinMode::Indirect => {
                                let key = serde_wasm_bindgen::to_value(&key)?;
                                store
                                    .get(key)?
                                    .await?
                                    .and_then(|root| {
                                        serde_wasm_bindgen::from_value::<Vec<u8>>(root).ok()
                                    })
                                    .map(|root| {
                                        cid_from_indirect_value(&root)
                                            .map(PinKind::IndirectFrom)
                                            .map_err(|e| {
                                                e.context(format!(
                                                    "failed to read indirect pin source: {:?}",
                                                    String::from_utf8_lossy(root.as_ref()).as_ref(),
                                                ))
                                            })
                                    })
                                    .transpose()?
                            }
                        },
                        Some(_) | None => None,
                    };

                    // this might be None, or Some(PinKind); it's important there are as many cids
                    // as there are modes
                    modes.push(matched);
                }

                Ok::<_, Box<dyn std::error::Error>>(
                    ids.into_iter()
                        .zip(modes.into_iter())
                        .filter_map(|(cid, mode)| mode.map(move |mode| (cid, mode)))
                        .collect::<Vec<_>>(),
                )
            }
            .await
            .map_err(|e| anyhow::anyhow!("{e}"));

            _ = tx.send(res);
        });

        rx.await?
    }
}

/// Name the empty value stored for direct pins; the pin key itself describes the mode and the cid.
fn direct_value() -> &'static [u8] {
    Default::default()
}

/// Name the empty value stored for recursive pins at the top.
fn recursive_value() -> &'static [u8] {
    Default::default()
}

/// Name the value stored for indirect pins, currently only the most recent recursive pin.
fn indirect_value(recursively_pinned: &Cid) -> String {
    recursively_pinned.to_string()
}

/// Inverse of [`indirect_value`].
fn cid_from_indirect_value(bytes: &[u8]) -> Result<Cid, Error> {
    std::str::from_utf8(bytes)
        .map_err(Error::from)
        .and_then(|s| Cid::from_str(s).map_err(Error::from))
}

fn pin_mode_literal(pin_mode: &PinMode) -> &'static str {
    match pin_mode {
        PinMode::Direct => "d",
        PinMode::Indirect => "i",
        PinMode::Recursive => "r",
    }
}

fn get_pin_key(cid: &Cid, pin_mode: &PinMode) -> String {
    // TODO: get_pinned_mode could be range query if the pin modes were suffixes, keys would need
    // to be cid.to_bytes().push(pin_mode_literal(pin_mode))? ... since the cid bytes
    // representation already contains the length we should be good to go in all cases.
    //
    // for storing multiple targets then the last could be found by doing a query as well. in the
    // case of multiple indirect pins they'd have to be with another suffix.
    //
    // TODO: check if such representation would really order properly
    format!("pin.{}.{}", pin_mode_literal(pin_mode), cid)
}

/// Returns a tuple of the parsed mode and the key used
async fn get_pinned_mode(
    _: &Transaction,
    tree: &ObjectStore,
    block: &Cid,
) -> Result<Option<(PinMode, String)>, Box<dyn std::error::Error>> {
    for mode in &[PinMode::Direct, PinMode::Recursive, PinMode::Indirect] {
        let key = get_pin_key(block, mode);

        let key_val = serde_wasm_bindgen::to_value(&key)?;

        let val = tree.get(key_val)?.await?;

        if val.is_some() {
            return Ok(Some((*mode, key)));
        }
    }

    Ok(None)
}

async fn is_not_pinned_or_pinned_indirectly(
    tx: &Transaction,
    tree: &ObjectStore,
    block: &Cid,
) -> Result<bool, Box<dyn std::error::Error>> {
    match get_pinned_mode(tx, tree, block).await? {
        Some((PinMode::Indirect, _)) | None => Ok(true),
        _ => Ok(false),
    }
}
