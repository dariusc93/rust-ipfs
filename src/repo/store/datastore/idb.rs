use std::{collections::BTreeSet, rc::Rc, str::FromStr, sync::OnceLock};

use crate::repo::store::idb::finish_transaction;
use crate::{
    Error, PinKind, PinMode,
    repo::{DataStore, PinModeRequirement, PinStore, References},
};
use futures::{StreamExt, TryStreamExt, stream::BoxStream};
use idb::{
    Database, DatabaseEvent, Factory, ObjectStore, ObjectStoreParams, Transaction, TransactionMode,
};
use ipld_core::cid::Cid;
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

// in the transactional parts of the [`Infallible`] is used to signal there is no additional
// custom error, not that the transaction was infallible in itself.

impl PinStore for IdbDataStore {
    async fn is_pinned(&self, cid: &Cid) -> Result<bool, Error> {
        SendWrapper::new(async move {
            let database = self.get_db();
            let transaction = database.transaction(&["pinstore"], TransactionMode::ReadOnly)?;

            let store = transaction.object_store("pinstore")?;

            let pinned = get_pinned_mode(&transaction, &store, cid).await?.is_some();
            finish_transaction(transaction).await?;
            Ok::<_, Box<dyn std::error::Error>>(pinned)
        })
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))
    }

    async fn insert_direct_pin(&self, target: &Cid) -> Result<(), Error> {
        SendWrapper::new(async move {
            let db = self.get_db();
            let transaction = db.transaction(&["pinstore"], TransactionMode::ReadWrite)?;

            let store = transaction.object_store("pinstore")?;

            let already_pinned = get_pinned_mode(&transaction, &store, target).await?;

            match already_pinned {
                Some((PinMode::Direct, _)) => {
                    finish_transaction(transaction).await?;
                    return Ok(());
                }
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

            let direct_key = get_pin_key(target, &PinMode::Direct);
            let key = serde_wasm_bindgen::to_value(&direct_key)?;
            let val = serde_wasm_bindgen::to_value(direct_value())?;

            store.put(&val, Some(&key))?.await?;
            finish_transaction(transaction.commit()?).await?;

            Ok::<_, Box<dyn std::error::Error>>(())
        })
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))
    }

    async fn insert_recursive_pin(
        &self,
        target: &Cid,
        referenced: References<'_>,
    ) -> Result<(), Error> {
        SendWrapper::new(async move {
            // since the transaction can be retried multiple times, we need to collect these and keep
            // iterating it until there is no conflict.
            let set = referenced.try_collect::<BTreeSet<_>>().await?;

            // the transaction is not infallible but there is no additional error we return
            let db = self.get_db();
            let transaction = db.transaction(&["pinstore"], TransactionMode::ReadWrite)?;

            let store = transaction.object_store("pinstore")?;

            let already_pinned = get_pinned_mode(&transaction, &store, target).await?;

            match already_pinned {
                Some((PinMode::Recursive, _)) => {
                    finish_transaction(transaction).await?;
                    return Ok(());
                }
                Some((PinMode::Direct, key)) | Some((PinMode::Indirect, key)) => {
                    // FIXME: this is probably another lapse in tests that both direct and
                    // indirect can be removed when inserting recursive?
                    let key = serde_wasm_bindgen::to_value(&key)?;
                    store.delete(key)?.await?;
                }
                None => {}
            }

            let recursive_key = get_pin_key(target, &PinMode::Recursive);
            let key = serde_wasm_bindgen::to_value(&recursive_key)?;
            let val = serde_wasm_bindgen::to_value(recursive_value())?;
            store.put(&val, Some(&key))?.await?;

            let target_value = indirect_value(target);

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

            finish_transaction(transaction.commit()?).await?;
            Ok::<_, Box<dyn std::error::Error>>(())
        })
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))
    }

    async fn remove_direct_pin(&self, target: &Cid) -> Result<(), Error> {
        SendWrapper::new(async move {
            let db = self.get_db();
            let transaction = db.transaction(&["pinstore"], TransactionMode::ReadWrite)?;

            let store = transaction.object_store("pinstore")?;

            if is_not_pinned_or_pinned_indirectly(&transaction, &store, target).await? {
                let r = || Err(anyhow::anyhow!("not pinned or pinned indirectly"));
                r()?;
            }

            let key = get_pin_key(target, &PinMode::Direct);
            let key = serde_wasm_bindgen::to_value(&key)?;

            store.delete(key)?.await?;

            finish_transaction(transaction.commit()?).await?;
            Ok::<_, Box<dyn std::error::Error>>(())
        })
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))
    }

    async fn remove_recursive_pin(
        &self,
        target: &Cid,
        referenced: References<'_>,
    ) -> Result<(), Error> {
        SendWrapper::new(async move {
            // TODO: is this "in the same transaction" as the batch which is created?
            let set = referenced.try_collect::<BTreeSet<_>>().await?;
            let db = self.get_db();
            let transaction = db.transaction(&["pinstore"], TransactionMode::ReadWrite)?;

            let store = transaction.object_store("pinstore")?;

            if is_not_pinned_or_pinned_indirectly(&transaction, &store, target).await? {
                let r = || Err(anyhow::anyhow!("not pinned or pinned indirectly"));
                r()?;
            }

            let key = get_pin_key(target, &PinMode::Recursive);
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

            finish_transaction(transaction.commit()?).await?;

            Ok::<_, Box<dyn std::error::Error>>(())
        })
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))
    }

    async fn list(
        &self,
        requirement: Option<PinMode>,
    ) -> BoxStream<'static, Result<(Cid, PinMode), Error>> {
        let db = self.get_db().to_owned();
        let requirement = PinModeRequirement::from(requirement);

        let pins = SendWrapper::new(async move {
            let transaction = db.transaction(&["pinstore"], TransactionMode::ReadOnly)?;
            let store = transaction.object_store("pinstore")?;
            let key_request = store.get_all_keys(None, None)?;
            let value_request = store.get_all(None, None)?;
            let keys = key_request.await?;
            let values = value_request.await?;
            if keys.len() != values.len() {
                return Err(std::io::Error::other("pin keys and values do not match").into());
            }

            let mut pins = Vec::with_capacity(keys.len());
            for (key, value) in keys.into_iter().zip(values) {
                let key = key
                    .as_string()
                    .ok_or_else(|| std::io::Error::other("invalid pin key"))?;
                let (cid, mode) = parse_pin_key(&key)?;
                validate_pin_value(mode, value)
                    .map_err(|error| error.context(format!("invalid pin value for {cid}")))?;
                if requirement.matches(&mode) {
                    pins.push((cid, mode));
                }
            }

            finish_transaction(transaction).await?;
            Ok::<_, Box<dyn std::error::Error>>(pins)
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
        let requirement = PinModeRequirement::from(requirement);

        SendWrapper::new(async move {
            let db = self.get_db();
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
                            let value = store.get(key)?.await?.ok_or_else(|| {
                                std::io::Error::other(format!("missing pin {id}"))
                            })?;
                            validate_pin_value(pin_mode, value)
                                .map_err(|error| {
                                    error.context(format!("invalid pin value for {id}"))
                                })?
                                .map(PinKind::IndirectFrom)
                        }
                    },
                    Some(_) | None => None,
                };

                // this might be None, or Some(PinKind); it's important there are as many cids
                // as there are modes
                modes.push(matched);
            }

            finish_transaction(transaction).await?;
            Ok::<_, Box<dyn std::error::Error>>(
                ids.into_iter()
                    .zip(modes.into_iter())
                    .filter_map(|(cid, mode)| mode.map(move |mode| (cid, mode)))
                    .collect::<Vec<_>>(),
            )
        })
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))
    }
}

/// The empty value stored for direct pins.
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

fn parse_pin_key(key: &str) -> Result<(Cid, PinMode), Error> {
    let (mode, cid) = key
        .strip_prefix("pin.")
        .and_then(|suffix| suffix.split_once('.'))
        .ok_or_else(|| anyhow::anyhow!("invalid pin key {key}"))?;
    let mode = match mode {
        "d" => PinMode::Direct,
        "r" => PinMode::Recursive,
        "i" => PinMode::Indirect,
        _ => anyhow::bail!("invalid pin mode in key {key}"),
    };
    let cid = Cid::from_str(cid)
        .map_err(|error| Error::from(error).context(format!("invalid CID in pin key {key}")))?;
    Ok((cid, mode))
}

fn validate_pin_value(mode: PinMode, value: JsValue) -> Result<Option<Cid>, Error> {
    match mode {
        PinMode::Indirect => {
            let root: String = serde_wasm_bindgen::from_value(value)
                .map_err(|error| anyhow::anyhow!("{error}"))?;
            Ok(Some(cid_from_indirect_value(root.as_bytes())?))
        }
        PinMode::Direct | PinMode::Recursive => {
            let value: Vec<u8> = serde_wasm_bindgen::from_value(value)
                .map_err(|error| anyhow::anyhow!("{error}"))?;
            if !value.is_empty() {
                anyhow::bail!("invalid value for {mode:?} pin");
            }
            Ok(None)
        }
    }
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

        if let Some(value) = val {
            validate_pin_value(*mode, value)
                .map_err(|error| error.context(format!("invalid pin value for {block}")))?;
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
