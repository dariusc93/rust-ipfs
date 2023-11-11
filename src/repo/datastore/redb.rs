use crate::error::Error;
use crate::repo::{DataStore, PinModeRequirement};
use crate::repo::{PinKind, PinMode, PinStore, References};
use async_trait::async_trait;
use either::Either;
use futures::stream::{StreamExt, TryStreamExt};
use libipld::cid::Cid;
use redb::{Database, ReadableTable, TableDefinition};
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::str::{self, FromStr};
use std::sync::{Arc, OnceLock};

const DATATABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("data");
const PINTABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("pin");

#[derive(Debug)]
pub struct RedbDataStore {
    path: PathBuf,
    // it is a trick for not modifying the Data:init
    db: OnceLock<Arc<Database>>,
}

impl RedbDataStore {
    pub fn new(root: PathBuf) -> RedbDataStore {
        RedbDataStore {
            path: root,
            db: Default::default(),
        }
    }

    fn get_db(&self) -> Arc<Database> {
        let db = self.db.get().cloned();
        db.expect("Datastore to be initialized")
    }
}

#[async_trait]
impl DataStore for RedbDataStore {
    async fn init(&self) -> Result<(), Error> {
        let db = Arc::new(Database::create(self.path.join("ipfs_datastore.db"))?);
        tokio::task::spawn_blocking({
            let db = db.clone();
            move || {
                let initial_tx = db.begin_write()?;
                {
                    _ = initial_tx.open_table(DATATABLE)?;
                }
                {
                    _ = initial_tx.open_table(PINTABLE)?;
                }
                initial_tx.commit()?;
                Ok::<_, Error>(())
            }
        })
        .await??;
        match self.db.set(db) {
            Ok(()) => Ok(()),
            Err(_) => Err(anyhow::anyhow!("failed to init redb")),
        }
    }

    async fn open(&self) -> Result<(), Error> {
        Ok(())
    }

    /// Checks if a key is present in the datastore.
    async fn contains(&self, key: &[u8]) -> Result<bool, Error> {
        let db = self.get_db();
        let key = key.to_owned();
        tokio::task::spawn_blocking(move || {
            let read_tx = db.begin_read()?;
            let table = read_tx.open_table(DATATABLE)?;
            let item = table.get(key.as_slice())?;
            Ok::<_, anyhow::Error>(item.is_some())
        })
        .await?
    }

    /// Returns the value associated with a key from the datastore.
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        let db = self.get_db();
        let key = key.to_owned();
        tokio::task::spawn_blocking(move || {
            let read_tx = db.begin_read()?;
            let table = read_tx.open_table(DATATABLE)?;
            let item = table.get(key.as_slice())?;
            Ok::<_, anyhow::Error>(item.map(|item| item.value().to_vec()))
        })
        .await?
    }

    /// Puts the value under the key in the datastore.
    async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        let key = key.to_owned();
        let value = value.to_owned();
        let db = self.get_db();
        tokio::task::spawn_blocking(move || {
            let tx = db.begin_write()?;
            {
                let mut table = tx.open_table(DATATABLE)?;
                table.insert(key.as_slice(), value.as_slice())?;
            }
            tx.commit()?;
            Ok::<_, anyhow::Error>(())
        })
        .await?
    }

    /// Removes a key-value pair from the datastore.
    async fn remove(&self, key: &[u8]) -> Result<(), Error> {
        let key = key.to_owned();
        let db = self.get_db();
        tokio::task::spawn_blocking(move || {
            let tx = db.begin_write()?;
            {
                let mut table = tx.open_table(DATATABLE)?;
                table.remove(key.as_slice())?;
            }
            tx.commit()?;
            Ok::<_, anyhow::Error>(())
        })
        .await?
    }

    async fn iter(&self) -> futures::stream::BoxStream<'static, (Vec<u8>, Vec<u8>)> {
        use tokio_stream::wrappers::UnboundedReceiverStream;
        let span = tracing::Span::current();
        let db = self.get_db();

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        let _t = tokio::task::spawn_blocking(move || {
            let span = tracing::trace_span!(parent: &span, "blocking");
            let _g = span.enter();
            let read_tx = match db.begin_read() {
                Ok(r) => r,
                Err(_) => {
                    return;
                }
            };
            let table = match read_tx.open_table(DATATABLE) {
                Ok(r) => r,
                Err(_) => {
                    return;
                }
            };

            let iter = match table.iter() {
                Ok(r) => r,
                Err(_) => {
                    return;
                }
            };

            for (k, v) in iter.filter_map(|res| res.ok()) {
                let (key, val) = (k.value(), v.value());
                _ = tx.send((key.to_vec(), val.to_vec()));
            }
        });

        UnboundedReceiverStream::new(rx).boxed()
    }

    /// Wipes the datastore.
    async fn wipe(&self) {}
}

#[async_trait]
impl PinStore for RedbDataStore {
    async fn is_pinned(&self, cid: &Cid) -> Result<bool, Error> {
        let cid = cid.to_owned();
        let db = self.get_db();
        let span = tracing::Span::current();
        tokio::task::spawn_blocking(move || {
            let span = tracing::trace_span!(parent: &span, "blocking");
            let _g = span.enter();
            let read_tx = db.begin_read()?;
            let table = read_tx.open_table(PINTABLE)?;

            Ok(get_pinned_mode(Either::Left(&table), &cid)?.is_some())
        })
        .await?
    }

    async fn insert_direct_pin(&self, target: &Cid) -> Result<(), Error> {
        let target = target.to_owned();
        let db = self.get_db();

        let span = tracing::Span::current();

        tokio::task::spawn_blocking(move || {
            let span = tracing::trace_span!(parent: &span, "blocking");
            let _g = span.enter();
            let tx = db.begin_write()?;
            {
                let mut table = tx.open_table(PINTABLE)?;

                let already_pinned = get_pinned_mode(Either::Right(&mut table), &target)?;

                match already_pinned {
                    Some((PinMode::Direct, _)) => return Ok(()),
                    Some((PinMode::Recursive, _)) => {
                        return Err(anyhow::anyhow!("already pinned recursively"))
                    }
                    Some((PinMode::Indirect, key)) => {
                        // TODO: I think the direct should live alongside the indirect?
                        table.remove(key.as_bytes())?;
                    }
                    None => {}
                }

                let direct_key = get_pin_key(&target, &PinMode::Direct);
                table.insert(direct_key.as_bytes(), direct_value())?;
            }

            tx.commit()?;

            Ok::<_, anyhow::Error>(())
        })
        .await?
    }

    async fn insert_recursive_pin(
        &self,
        target: &Cid,
        referenced: References<'_>,
    ) -> Result<(), Error> {
        let set = referenced.try_collect::<BTreeSet<_>>().await?;

        let target = target.to_owned();
        let db = self.get_db().to_owned();

        let span = tracing::Span::current();

        tokio::task::spawn_blocking(move || {
            let span = tracing::trace_span!(parent: &span, "blocking");
            let _g = span.enter();

            let tx = db.begin_write()?;
            {
                let mut table = tx.open_table(PINTABLE)?;

                let already_pinned = get_pinned_mode(Either::Right(&mut table), &target)?;

                match already_pinned {
                    Some((PinMode::Recursive, _)) => return Ok(()),
                    Some((PinMode::Direct, key)) | Some((PinMode::Indirect, key)) => {
                        table.remove(key.as_bytes())?;
                    }
                    None => {}
                }

                let recursive_key = get_pin_key(&target, &PinMode::Recursive);
                table.insert(recursive_key.as_bytes(), recursive_value())?;

                let target_value = indirect_value(&target);

                for cid in set.iter() {
                    let indirect_key = get_pin_key(cid, &PinMode::Indirect);

                    if get_pinned_mode(Either::Right(&mut table), cid)?.is_some() {
                        continue;
                    }

                    table.insert(indirect_key.as_bytes(), target_value.as_bytes())?;
                }
            }

            tx.commit()?;

            Ok::<_, anyhow::Error>(())
        })
        .await??;

        Ok(())
    }

    async fn remove_direct_pin(&self, target: &Cid) -> Result<(), Error> {
        let target = target.to_owned();
        let db = self.get_db().to_owned();

        let span = tracing::Span::current();

        tokio::task::spawn_blocking(move || {
            let span = tracing::trace_span!(parent: &span, "blocking");
            let _g = span.enter();

            let tx = db.begin_write()?;
            {
                let mut table = tx.open_table(PINTABLE)?;

                if is_not_pinned_or_pinned_indirectly(Either::Right(&mut table), &target)? {
                    return Err(anyhow::anyhow!("not pinned or pinned indirectly"));
                }

                let key = get_pin_key(&target, &PinMode::Direct);
                table.remove(key.as_bytes())?;
            }
            tx.commit()?;

            Ok::<_, anyhow::Error>(())
        })
        .await?
    }

    async fn remove_recursive_pin(
        &self,
        target: &Cid,
        referenced: References<'_>,
    ) -> Result<(), Error> {
        let set = referenced.try_collect::<BTreeSet<_>>().await?;

        let target = target.to_owned();
        let db = self.get_db().to_owned();

        let span = tracing::Span::current();

        tokio::task::spawn_blocking(move || {
            let span = tracing::trace_span!(parent: &span, "blocking");
            let _g = span.enter();
            let tx = db.begin_write()?;
            {
                let mut table = tx.open_table(PINTABLE)?;

                if is_not_pinned_or_pinned_indirectly(Either::Right(&mut table), &target)? {
                    return Err(anyhow::anyhow!("not pinned or pinned indirectly"));
                }

                let recursive_key = get_pin_key(&target, &PinMode::Recursive);
                table.remove(recursive_key.as_bytes())?;

                for cid in &set {
                    let already_pinned = get_pinned_mode(Either::Right(&mut table), cid)?;

                    match already_pinned {
                        Some((PinMode::Recursive, _)) | Some((PinMode::Direct, _)) => continue, // this should be unreachable
                        Some((PinMode::Indirect, key)) => {
                            // FIXME: not really sure of this but it might be that recursive removed
                            // the others...?
                            table.remove(key.as_bytes())?;
                        }
                        None => {}
                    }
                }
            }

            tx.commit()?;
            Ok::<_, anyhow::Error>(())
        })
        .await?
    }

    async fn list(
        &self,
        requirement: Option<PinMode>,
    ) -> futures::stream::BoxStream<'static, Result<(Cid, PinMode), Error>> {
        use tokio_stream::wrappers::UnboundedReceiverStream;

        let db = self.get_db().to_owned();

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        let span = tracing::Span::current();

        let _jh = tokio::task::spawn_blocking(move || {
            let span = tracing::trace_span!(parent: &span, "blocking");
            let _g = span.enter();

            let read_tx = match db.begin_read() {
                Ok(r) => r,
                Err(e) => {
                    let _ = tx.send(Err(anyhow::Error::from(e)));
                    return;
                }
            };
            let table = match read_tx.open_table(PINTABLE) {
                Ok(r) => r,
                Err(e) => {
                    let _ = tx.send(Err(anyhow::Error::from(e)));
                    return;
                }
            };

            let iter = match table.iter() {
                Ok(r) => r,
                Err(e) => {
                    let _ = tx.send(Err(anyhow::Error::from(e)));
                    return;
                }
            };

            let requirement = PinModeRequirement::from(requirement);

            let adapted =
                iter.map(|res| res.map_err(Error::from))
                    .filter_map(move |res| match res {
                        Ok((k, _v)) => {
                            let val = k.value();
                            if !val.starts_with(b"pin.") || val.len() < 7 {
                                return Some(Err(anyhow::anyhow!(
                                    "invalid pin: {:?}",
                                    &*String::from_utf8_lossy(val)
                                )));
                            }

                            let mode = match val[4] {
                                b'd' => PinMode::Direct,
                                b'r' => PinMode::Recursive,
                                b'i' => PinMode::Indirect,
                                x => {
                                    return Some(Err(anyhow::anyhow!(
                                        "invalid pinmode: {}",
                                        x as char
                                    )))
                                }
                            };

                            if !requirement.matches(&mode) {
                                None
                            } else {
                                let cid = std::str::from_utf8(&val[6..]).map_err(Error::from);
                                let cid = cid.and_then(|x| Cid::from_str(x).map_err(Error::from));
                                let cid = cid.map_err(|e| {
                                    e.context(format!(
                                        "failed to read pin: {:?}",
                                        &*String::from_utf8_lossy(val)
                                    ))
                                });
                                Some(cid.map(move |cid| (cid, mode)))
                            }
                        }
                        Err(e) => Some(Err(e)),
                    });

            for res in adapted {
                if tx.send(res).is_err() {
                    break;
                }
            }
        });

        UnboundedReceiverStream::new(rx).boxed()
    }

    async fn query(
        &self,
        ids: Vec<Cid>,
        requirement: Option<PinMode>,
    ) -> Result<Vec<(Cid, PinKind<Cid>)>, Error> {
        let requirement = PinModeRequirement::from(requirement);

        let db = self.get_db().to_owned();

        tokio::task::spawn_blocking(move || {
            let mut modes = Vec::with_capacity(ids.len());

            let read_tx = db.begin_read()?;

            let table = read_tx.open_table(PINTABLE)?;

            for id in ids.iter() {
                let mode_and_key = get_pinned_mode(Either::Left(&table), id)?;

                let matched = match mode_and_key {
                    Some((pin_mode, key)) if requirement.matches(&pin_mode) => match pin_mode {
                        PinMode::Direct => Some(PinKind::Direct),
                        PinMode::Recursive => Some(PinKind::Recursive(0)),
                        PinMode::Indirect => table
                            .get(key.as_bytes())?
                            .map(|root| {
                                cid_from_indirect_value(root.value())
                                    .map(PinKind::IndirectFrom)
                                    .map_err(|e| {
                                        e.context(format!(
                                            "failed to read indirect pin source: {:?}",
                                            String::from_utf8_lossy(root.value()).as_ref(),
                                        ))
                                    })
                            })
                            .transpose()?,
                    },
                    Some(_) | None => None,
                };

                modes.push(matched);
            }

            Ok(ids
                .into_iter()
                .zip(modes.into_iter())
                .filter_map(|(cid, mode)| mode.map(move |mode| (cid, mode)))
                .collect::<Vec<_>>())
        })
        .await?
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
    str::from_utf8(bytes)
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
    format!("pin.{}.{}", pin_mode_literal(pin_mode), cid)
}

#[allow(clippy::type_complexity)]
/// Returns a tuple of the parsed mode and the key used
fn get_pinned_mode(
    table: either::Either<
        &redb::ReadOnlyTable<'_, &[u8], &[u8]>,
        &mut redb::Table<'_, '_, &[u8], &[u8]>,
    >,
    block: &Cid,
) -> Result<Option<(PinMode, String)>, anyhow::Error> {
    for mode in &[PinMode::Direct, PinMode::Recursive, PinMode::Indirect] {
        let key = get_pin_key(block, mode);

        let exist = match table {
            Either::Left(read) => read.get(key.as_bytes())?.is_some(),
            Either::Right(ref write) => write.get(key.as_bytes())?.is_some(),
        };

        if exist {
            return Ok(Some((*mode, key)));
        }
    }

    Ok(None)
}

#[allow(clippy::type_complexity)]
fn is_not_pinned_or_pinned_indirectly(
    table: either::Either<
        &redb::ReadOnlyTable<'_, &[u8], &[u8]>,
        &mut redb::Table<'_, '_, &[u8], &[u8]>,
    >,
    block: &Cid,
) -> Result<bool, anyhow::Error> {
    match get_pinned_mode(table, block)? {
        Some((PinMode::Indirect, _)) | None => Ok(true),
        _ => Ok(false),
    }
}

#[cfg(test)]
crate::pinstore_interface_tests!(
    common_tests,
    crate::repo::datastore::redb::RedbDataStore::new
);

#[cfg(test)]
mod test {
    use crate::repo::{datastore::redb::RedbDataStore, DataStore};

    #[tokio::test]
    async fn test_kv_datastore() {
        let tmp = std::env::temp_dir();
        let store = RedbDataStore::new(tmp.clone());
        let key = [1, 2, 3, 4];
        let value = [5, 6, 7, 8];

        store.init().await.unwrap();
        store.open().await.unwrap();

        let contains = store.contains(&key);
        assert!(!contains.await.unwrap());
        let get = store.get(&key);
        assert_eq!(get.await.unwrap(), None);
        store.remove(&key).await.unwrap();

        let put = store.put(&key, &value);
        put.await.unwrap();
        let contains = store.contains(&key);
        assert!(contains.await.unwrap());
        let get = store.get(&key);
        assert_eq!(get.await.unwrap(), Some(value.to_vec()));

        store.remove(&key).await.unwrap();
        let contains = store.contains(&key);
        assert!(!contains.await.unwrap());
        let get = store.get(&key);
        assert_eq!(get.await.unwrap(), None);
        drop(store);
    }
}
