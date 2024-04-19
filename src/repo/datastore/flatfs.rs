//! Persistent filesystem backed pin store. See [`FsDataStore`] for more information.
use crate::error::Error;
use crate::repo::paths::{filestem_to_pin_cid, pin_path};
use crate::repo::{DataStore, PinKind, PinMode, PinModeRequirement, PinStore, References};
use async_trait::async_trait;
use core::convert::TryFrom;
use futures::stream::{BoxStream, TryStreamExt};
use futures::StreamExt;
use libipld::Cid;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs;
use tokio::sync::{RwLock, Semaphore};
use tokio_stream::{empty, wrappers::ReadDirStream};
use tokio_util::either::Either;

/// FsDataStore which uses the filesystem as a lockable key-value store. Maintains a similar to
/// [`FsBlockStore`] sharded two level storage. Direct have empty files, recursive pins record all of
/// their indirect descendants. Pin files are separated by their file extensions.
///
/// When modifying, single lock is used.
///
/// For the [`crate::repo::PinStore`] implementation see `fs/pinstore.rs`.
#[derive(Debug)]
pub struct FsDataStore {
    /// The base directory under which we have a sharded directory structure, and the individual
    /// blocks are stored under the shard. See unixfs/examples/cat.rs for read example.
    path: PathBuf,

    /// Start with simple, conservative solution, allows concurrent queries but single writer.
    /// It is assumed the reads do not require permit as non-empty writes are done through
    /// tempfiles and the consistency regarding reads is not a concern right now. For garbage
    /// collection implementation, it might be needed to hold this permit for the duration of
    /// garbage collection, or something similar.
    lock: Arc<Semaphore>,

    ds_guard: Arc<RwLock<()>>,
}

impl FsDataStore {
    pub fn new(root: PathBuf) -> Self {
        FsDataStore {
            path: root,
            ds_guard: Arc::default(),
            lock: Arc::new(Semaphore::new(1)),
        }
    }

    // Instead of having the file be the key itself, we would split the key into segements with all but the last representing as a directory
    // with the final item being a file.
    fn key(&self, key: &[u8]) -> Option<(String, String)> {
        let key = String::from_utf8_lossy(key);
        let mut key_segments = key.split('/').collect::<Vec<_>>();

        let key_val = key_segments
            .pop()
            .map(PathBuf::from)
            .map(|path| path.with_extension("data"))
            .map(|path| path.to_string_lossy().to_string())?;

        let key_path_raw = key_segments.join("/");

        let key_path = match key_path_raw.starts_with('/') {
            true => key_path_raw[1..].to_string(),
            false => key_path_raw,
        };

        Some((key_path, key_val))
    }

    async fn write(&self, key: &[u8], val: &[u8]) -> std::io::Result<()> {
        let data_path = self.path.join("data");
        if !data_path.is_dir() {
            tokio::fs::create_dir_all(&data_path).await?;
        }

        let (path, key) = self
            .key(key)
            .ok_or::<std::io::Error>(std::io::ErrorKind::NotFound.into())?;

        let path = data_path.join(path);

        if !path.is_dir() {
            tokio::fs::create_dir_all(&path).await?;
        }

        let path = path.join(key);

        if path.is_dir() {
            // The only reason why this would be a directory is if the key didnt exist, is invalid, or the item was
            // actually a directory in which case we return an error here.
            return Err(std::io::ErrorKind::Other.into());
        }

        tokio::fs::write(path, val).await
    }

    fn _contains(&self, key: &[u8]) -> bool {
        let data_path = self.path.join("data");
        let Some((path, key)) = self.key(key) else {
            return false;
        };
        let path = data_path.join(path);
        let path = path.join(key);
        path.is_file()
    }

    async fn delete(&self, key: &[u8]) -> std::io::Result<()> {
        let data_path = self.path.join("data");
        let (path, key) = self
            .key(key)
            .ok_or::<std::io::Error>(std::io::ErrorKind::NotFound.into())?;
        let path = data_path.join(path);
        let path = path.join(key);
        tokio::fs::remove_file(path).await
    }

    async fn read(&self, key: &[u8]) -> std::io::Result<Option<Vec<u8>>> {
        let data_path = self.path.join("data");
        let (path, key) = self
            .key(key)
            .ok_or::<std::io::Error>(std::io::ErrorKind::NotFound.into())?;
        let path = data_path.join(path);
        let path = path.join(key);
        if path.is_dir() {
            return Ok(None);
        }
        tokio::fs::read(path).await.map(Some)
    }
}

fn build_kv<R: AsRef<Path>, P: AsRef<Path>>(
    data_path: R,
    path: P,
) -> BoxStream<'static, (Vec<u8>, Vec<u8>)> {
    let data_path = data_path.as_ref().to_path_buf();
    let path = path.as_ref().to_path_buf();
    async_stream::stream! {
        if path.is_file() {
            return;
        }
        let Ok(dir) = tokio::fs::read_dir(path).await else {
            return;
        };

        let st =
            ReadDirStream::new(dir).filter_map(|result| futures::future::ready(result.ok()));

        for await entry in st {
            let path = entry.path();
            if path.is_dir() {
                for await item in build_kv(&data_path, &path) {
                    yield item;
                }
            } else {
                let root_str = data_path.to_string_lossy().to_string();
                let path_str = path.to_string_lossy().to_string();
                let raw_key = &path_str[root_str.len()..];
                if raw_key.is_empty() {
                    continue;
                }

                let key = raw_key[..5].as_bytes().to_vec();

                if let Ok(bytes) = tokio::fs::read(path).await {
                    yield (key, bytes)
                }
            }
        }
    }
    .boxed()
}

/// The column operations are all unimplemented pending at least downscoping of the
/// DataStore trait itself.
#[async_trait]
impl DataStore for FsDataStore {
    async fn init(&self) -> Result<(), Error> {
        // Although `pins` directory is created when inserting a data, is it not created when there are any attempts at listing the pins (thus causing to fail)
        tokio::fs::create_dir_all(&self.path.join("pins")).await?;
        tokio::fs::create_dir_all(&self.path.join("data")).await?;
        Ok(())
    }

    async fn open(&self) -> Result<(), Error> {
        Ok(())
    }

    async fn contains(&self, key: &[u8]) -> Result<bool, Error> {
        let _g = self.ds_guard.read().await;
        Ok(self._contains(key))
    }

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        let _g = self.ds_guard.read().await;
        self.read(key).await.map_err(Error::from)
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        let _g = self.ds_guard.write().await;
        self.write(key, value).await.map_err(Error::from)
    }

    async fn remove(&self, key: &[u8]) -> Result<(), Error> {
        let _g = self.ds_guard.write().await;
        self.delete(key).await.map_err(Error::from)
    }

    async fn iter(&self) -> BoxStream<'static, (Vec<u8>, Vec<u8>)> {
        let data_path = self.path.join("data");
        build_kv(&data_path, &data_path)
    }
}

// PinStore is a trait from ipfs::repo implemented on FsDataStore defined at ipfs::repo::fs or
// parent module.
#[async_trait]
impl PinStore for FsDataStore {
    async fn is_pinned(&self, cid: &Cid) -> Result<bool, Error> {
        let path = pin_path(self.path.join("pins"), cid);

        if read_direct_or_recursive(path).await?.is_some() {
            return Ok(true);
        }

        let st = self.list_pinfiles().await.try_filter_map(|(cid, mode)| {
            futures::future::ready(if mode == PinMode::Recursive {
                Ok(Some(cid))
            } else {
                Ok(None)
            })
        });

        futures::pin_mut!(st);

        while let Some(recursive) = TryStreamExt::try_next(&mut st).await? {
            // TODO: it might be much better to just deserialize the vec one by one and comparing while
            // going
            let (_, references) =
                read_recursively_pinned(self.path.join("pins"), recursive).await?;

            // if we always wrote down the cids in some order we might be able to binary search?
            if references.into_iter().any(move |x| x == *cid) {
                return Ok(true);
            }
        }

        Ok(false)
    }

    async fn insert_direct_pin(&self, target: &Cid) -> Result<(), Error> {
        let permit = Semaphore::acquire_owned(Arc::clone(&self.lock)).await?;

        let mut path = pin_path(self.path.join("pins"), target);

        let span = tracing::Span::current();

        tokio::task::spawn_blocking(move || {
            // move the permit to the blocking thread to ensure we keep it as long as needed
            let _permit = permit;
            let _entered = span.enter();

            std::fs::create_dir_all(path.parent().expect("shard parent has to exist"))?;
            path.set_extension("recursive");
            if path.is_file() {
                return Err(anyhow::anyhow!("already pinned recursively"));
            }

            path.set_extension("direct");
            let f = std::fs::File::create(path)?;
            f.sync_all()?;
            Ok(())
        })
        .await??;

        Ok(())
    }

    async fn insert_recursive_pin(
        &self,
        target: &Cid,
        referenced: References<'_>,
    ) -> Result<(), Error> {
        let set = referenced
            .try_collect::<std::collections::BTreeSet<_>>()
            .await?;

        let permit = Semaphore::acquire_owned(Arc::clone(&self.lock)).await?;

        let mut path = pin_path(self.path.join("pins"), target);

        let span = tracing::Span::current();

        tokio::task::spawn_blocking(move || {
            let _permit = permit; // again move to the threadpool thread
            let _entered = span.enter();

            std::fs::create_dir_all(path.parent().expect("shard parent has to exist"))?;
            let count = set.len();
            let cids = set.into_iter().map(|cid| cid.to_string());

            path.set_extension("recursive_temp");

            let file = std::fs::File::create(&path)?;

            match sync_write_recursive_pin(file, count, cids) {
                Ok(_) => {
                    let final_path = path.with_extension("recursive");
                    std::fs::rename(&path, final_path)?
                }
                Err(e) => {
                    let removed = std::fs::remove_file(&path);

                    match removed {
                        Ok(_) => debug!("cleaned up ok after botched recursive pin write"),
                        Err(e) => warn!("failed to cleanup temporary file: {}", e),
                    }

                    return Err(e);
                }
            }

            // if we got this far, we have now written and renamed the recursive_temp into place.
            // now we just need to remove the direct pin, if it exists

            path.set_extension("direct");

            match std::fs::remove_file(&path) {
                Ok(_) => { /* good */ }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => { /* good as well */ }
                Err(e) => {
                    warn!(
                        "failed to remove direct pin when adding recursive {:?}: {}",
                        path, e
                    );
                }
            }

            Ok::<_, Error>(())
        })
        .await??;

        Ok(())
    }

    async fn remove_direct_pin(&self, target: &Cid) -> Result<(), Error> {
        let permit = Semaphore::acquire_owned(Arc::clone(&self.lock)).await?;

        let mut path = pin_path(self.path.join("pins"), target);

        let span = tracing::Span::current();

        tokio::task::spawn_blocking(move || {
            let _permit = permit; // move in to threadpool thread
            let _entered = span.enter();

            path.set_extension("recursive");

            if path.is_file() {
                return Err(anyhow::anyhow!("is pinned recursively"));
            }

            path.set_extension("direct");

            match std::fs::remove_file(&path) {
                Ok(_) => {
                    trace!("direct pin removed");
                    Ok(())
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    Err(anyhow::anyhow!("not pinned or pinned indirectly"))
                }
                Err(e) => Err(e.into()),
            }
        })
        .await??;

        Ok(())
    }

    async fn remove_recursive_pin(&self, target: &Cid, _: References<'_>) -> Result<(), Error> {
        let permit = Semaphore::acquire_owned(Arc::clone(&self.lock)).await?;

        let mut path = pin_path(self.path.join("pins"), target);

        let span = tracing::Span::current();

        tokio::task::spawn_blocking(move || {
            let _permit = permit; // move into threadpool thread
            let _entered = span.enter();

            path.set_extension("direct");

            let mut any = false;

            match std::fs::remove_file(&path) {
                Ok(_) => {
                    trace!("direct pin removed");
                    any |= true;
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    // nevermind, we are just trying to remove the direct as it should go, if it
                    // was left by mistake
                }
                // Error::new instead of e.into() to help out the type inference
                Err(e) => return Err(Error::new(e)),
            }

            path.set_extension("recursive");

            match std::fs::remove_file(&path) {
                Ok(_) => {
                    trace!("recursive pin removed");
                    any |= true;
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    // we may have removed only the direct pin, but if we cleaned out a direct pin
                    // this would have been a success
                }
                Err(e) => return Err(e.into()),
            }

            if !any {
                Err(anyhow::anyhow!("not pinned or pinned indirectly"))
            } else {
                Ok(())
            }
        })
        .await??;

        Ok(())
    }

    async fn list(
        &self,
        requirement: Option<PinMode>,
    ) -> futures::stream::BoxStream<'static, Result<(Cid, PinMode), Error>> {
        // no locking, dirty reads are probably good enough until gc
        let cids = self.list_pinfiles().await;

        let path = self.path.join("pins");

        let requirement = PinModeRequirement::from(requirement);

        // depending on what was queried we must iterate through the results in the order of
        // recursive, direct and indirect.
        //
        // if only one kind is required, we must return only those, which may or may not be
        // easier than doing all of the work. this implementation follows:
        //
        // https://github.com/ipfs/go-ipfs/blob/2ae5c52f4f0f074864ea252e90e72e8d5999caba/core/coreapi/pin.go#L222
        let st = async_stream::try_stream! {

            // keep track of all returned not to give out duplicate cids
            let mut returned: HashSet<Cid> = HashSet::default();

            // the set of recursive will be interesting after all others
            let mut recursive: HashSet<Cid> = HashSet::default();
            let mut direct: HashSet<Cid> = HashSet::default();

            let collect_recursive_for_indirect = requirement.is_indirect_or_any();

            futures::pin_mut!(cids);

            while let Some((cid, mode)) = TryStreamExt::try_next(&mut cids).await? {

                let matches = requirement.matches(&mode);

                if mode == PinMode::Recursive {
                    if collect_recursive_for_indirect {
                        recursive.insert(cid);
                    }
                    if matches && returned.insert(cid) {
                        // the recursive pins can always be returned right away since they have
                        // the highest priority in this listing or output
                        yield (cid, mode);
                    }
                } else if mode == PinMode::Direct && matches {
                    direct.insert(cid);
                }
            }

            trace!(unique = returned.len(), "completed listing recursive");

            // now that the recursive are done, next up in priority order are direct. the set
            // of directly pinned and recursively pinned should be disjoint, but probably there
            // are times when 100% accurate results are not possible... Nor needed.
            for cid in direct {
                if returned.insert(cid) {
                    yield (cid, PinMode::Direct)
                }
            }

            trace!(unique = returned.len(), "completed listing direct");

            if !collect_recursive_for_indirect {
                // we didn't collect the recursive to list the indirect so, done.
                return;
            }

            // the threadpool passing adds probably some messaging latency, maybe run small
            // amount in parallel?
            let mut recursive = futures::stream::iter(recursive.into_iter().map(Ok))
                .map_ok(move |cid| read_recursively_pinned(path.clone(), cid))
                .try_buffer_unordered(4);

            while let Some((_, next_batch)) = TryStreamExt::try_next(&mut recursive).await? {
                for indirect in next_batch {
                    if returned.insert(indirect) {
                        yield (indirect, PinMode::Indirect);
                    }
                }

                trace!(unique = returned.len(), "completed batch of indirect");
            }
        };

        Box::pin(st)
    }

    async fn query(
        &self,
        ids: Vec<Cid>,
        requirement: Option<PinMode>,
    ) -> Result<Vec<(Cid, PinKind<Cid>)>, Error> {
        // response vec gets written to whenever we find out what the pin is
        let mut response = Vec::with_capacity(ids.len());
        for _ in 0..ids.len() {
            response.push(None);
        }

        let mut remaining = HashMap::new();

        let (check_direct, searched_suffix, gather_indirect) = match requirement {
            Some(PinMode::Direct) => (true, Some(PinMode::Direct), false),
            Some(PinMode::Recursive) => (true, Some(PinMode::Recursive), false),
            Some(PinMode::Indirect) => (false, None, true),
            None => (true, None, true),
        };

        let searched_suffix = PinModeRequirement::from(searched_suffix);

        let (mut response, mut remaining) = if check_direct {
            // find the recursive and direct ones by just seeing if the files exist
            let base = self.path.join("pins");
            tokio::task::spawn_blocking(move || {
                for (i, cid) in ids.into_iter().enumerate() {
                    let mut path = pin_path(base.clone(), &cid);

                    if let Some(mode) = sync_read_direct_or_recursive(&mut path) {
                        if searched_suffix.matches(&mode) {
                            response[i] = Some((
                                cid,
                                match mode {
                                    PinMode::Direct => PinKind::Direct,
                                    // FIXME: eech that recursive count is now out of place
                                    PinMode::Recursive => PinKind::Recursive(0),
                                    // FIXME: this is also quite unfortunate, should make an enum
                                    // of two?
                                    _ => unreachable!(),
                                },
                            ));
                            continue;
                        }
                    }

                    if !gather_indirect {
                        // if we are only trying to find recursive or direct, we clearly have not
                        // found what we were looking for
                        return Err(anyhow::anyhow!("{} is not pinned", cid));
                    }

                    // use entry api to discard duplicate cids in input
                    remaining.entry(cid).or_insert(i);
                }

                Ok((response, remaining))
            })
            .await??
        } else {
            for (i, cid) in ids.into_iter().enumerate() {
                remaining.entry(cid).or_insert(i);
            }
            (response, remaining)
        };

        // now remaining must have all of the cids => first_index mappings which were not found to
        // be recursive or direct.

        if !remaining.is_empty() {
            assert!(gather_indirect);

            trace!(
                remaining = remaining.len(),
                "query trying to find remaining indirect pins"
            );

            let recursives = self
                .list_pinfiles()
                .await
                .try_filter_map(|(cid, mode)| {
                    futures::future::ready(if mode == PinMode::Recursive {
                        Ok(Some(cid))
                    } else {
                        Ok(None)
                    })
                })
                .map_ok(|cid| read_recursively_pinned(self.path.join("pins"), cid))
                .try_buffer_unordered(4);

            futures::pin_mut!(recursives);

            'out: while let Some((referring, references)) =
                TryStreamExt::try_next(&mut recursives).await?
            {
                // FIXME: maybe binary search?
                for cid in references {
                    if let Some(index) = remaining.remove(&cid) {
                        response[index] = Some((cid, PinKind::IndirectFrom(referring)));

                        if remaining.is_empty() {
                            break 'out;
                        }
                    }
                }
            }
        }

        if let Some((cid, _)) = remaining.into_iter().next() {
            // the error can be for any of these
            return Err(anyhow::anyhow!("{} is not pinned", cid));
        }

        // the input can of course contain duplicate cids so handle them by just giving responses
        // for the first of the duplicates
        Ok(response.into_iter().flatten().collect())
    }
}

impl FsDataStore {
    async fn list_pinfiles(
        &self,
    ) -> impl futures::stream::Stream<Item = Result<(Cid, PinMode), Error>> + 'static {
        let stream = match tokio::fs::read_dir(self.path.join("pins")).await {
            Ok(st) => Either::Left(ReadDirStream::new(st)),
            // make this into a stream which will only yield the initial error
            Err(e) => Either::Right(futures::stream::once(futures::future::ready(Err(e)))),
        };

        stream
            .and_then(|d| async move {
                // map over the shard directories
                Ok(if d.file_type().await?.is_dir() {
                    Either::Left(ReadDirStream::new(fs::read_dir(d.path()).await?))
                } else {
                    Either::Right(empty())
                })
            })
            // flatten each
            .try_flatten()
            .map_err(Error::new)
            // convert the paths ending in ".data" into cid
            .try_filter_map(|d| {
                let name = d.file_name();
                let path: &std::path::Path = name.as_ref();

                let mode = if path.extension() == Some("recursive".as_ref()) {
                    Some(PinMode::Recursive)
                } else if path.extension() == Some("direct".as_ref()) {
                    Some(PinMode::Direct)
                } else {
                    None
                };

                let maybe_tuple = mode.and_then(move |mode| {
                    filestem_to_pin_cid(path.file_stem()).map(move |cid| (cid, mode))
                });

                futures::future::ready(Ok(maybe_tuple))
            })
    }
}

/// Reads our serialized format for recusive pins, which is JSON array of stringified Cids.
///
/// On file not found error returns an empty Vec as if nothing had happened. This is because we
/// do "atomic writes" and file removals are expected to be atomic, but reads don't synchronize on
/// writes, so while iterating it's possible that recursive pin is removed.
async fn read_recursively_pinned(path: PathBuf, cid: Cid) -> Result<(Cid, Vec<Cid>), Error> {
    // our fancy format is a Vec<Cid> as json
    let mut path = pin_path(path, &cid);
    path.set_extension("recursive");
    let contents = match tokio::fs::read(path).await {
        Ok(vec) => vec,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // per method comment, return empty Vec; the pins may have seemed to be present earlier
            // but no longer are.
            return Ok((cid, Vec::new()));
        }
        Err(e) => return Err(e.into()),
    };

    let cids: Vec<&str> = serde_json::from_slice(&contents)?;

    // returning a stream which is updated 8kB at time or such might be better, but this should
    // scale quite up as well.
    let found = cids
        .into_iter()
        .map(Cid::try_from)
        .collect::<Result<Vec<Cid>, _>>()?;

    trace!(cid = %cid, count = found.len(), "read indirect pins");
    Ok((cid, found))
}

async fn read_direct_or_recursive(mut block_path: PathBuf) -> Result<Option<PinMode>, Error> {
    tokio::task::spawn_blocking(move || Ok(sync_read_direct_or_recursive(&mut block_path))).await?
}

fn sync_read_direct_or_recursive(block_path: &mut PathBuf) -> Option<PinMode> {
    // important to first check the recursive then only the direct; the latter might be a left over
    for (ext, mode) in &[
        ("recursive", PinMode::Recursive),
        ("direct", PinMode::Direct),
    ] {
        block_path.set_extension(ext);
        // Path::is_file calls fstat and coerces errors to false; this might be enough, as
        // we are holding the lock
        if block_path.is_file() {
            return Some(*mode);
        }
    }
    None
}

fn sync_write_recursive_pin(
    file: std::fs::File,
    count: usize,
    cids: impl Iterator<Item = String>,
) -> Result<(), Error> {
    use serde::{ser::SerializeSeq, Serializer};
    use std::io::{BufWriter, Write};
    let writer = BufWriter::new(file);

    let mut serializer = serde_json::ser::Serializer::new(writer);

    let mut seq = serializer.serialize_seq(Some(count))?;
    for cid in cids {
        seq.serialize_element(&cid)?;
    }
    seq.end()?;

    let mut writer = serializer.into_inner();
    writer.flush()?;

    let file = writer.into_inner()?;
    file.sync_all()?;
    Ok(())
}

#[cfg(test)]
crate::pinstore_interface_tests!(
    common_tests,
    crate::repo::datastore::flatfs::FsDataStore::new
);

#[cfg(test)]
mod test {
    use crate::repo::{datastore::flatfs::FsDataStore, DataStore};

    #[tokio::test]
    async fn test_kv_datastore() -> anyhow::Result<()> {
        let tmp = std::env::temp_dir();
        let store = FsDataStore::new(tmp.clone());
        let key = [1, 2, 3, 4];
        let value = [5, 6, 7, 8];

        store.init().await?;
        store.open().await?;

        let contains = store.contains(&key).await.unwrap();
        assert!(!contains);
        let get = store.get(&key).await.unwrap_or_default();
        assert_eq!(get, None);
        assert!(store.remove(&key).await.is_err());

        store.put(&key, &value).await.unwrap();
        let contains = store.contains(&key).await.unwrap();
        assert!(contains);
        let get = store.get(&key).await.unwrap();
        assert_eq!(get, Some(value.to_vec()));

        store.remove(&key).await.unwrap();
        let contains = store.contains(&key).await.unwrap();
        assert!(!contains);
        let get = store.get(&key).await.unwrap_or_default();
        assert_eq!(get, None);
        drop(store);
        Ok(())
    }
}
