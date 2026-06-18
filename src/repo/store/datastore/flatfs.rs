//! Persistent filesystem backed pin store. See [`FsDataStore`] for more information.
use crate::error::Error;
use crate::repo::paths::{filestem_to_pin_cid, pin_path};
use crate::repo::{DataStore, PinKind, PinMode, PinModeRequirement, PinStore, References};
use core::convert::TryFrom;
use futures::StreamExt;
use futures::stream::{BoxStream, TryStreamExt};
use ipld_core::cid::Cid;
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
#[derive(Debug, Clone)]
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

    pin_index: Arc<parking_lot::RwLock<HashMap<Cid, HashSet<Cid>>>>,
}

impl FsDataStore {
    pub fn new(root: PathBuf) -> Self {
        FsDataStore {
            path: root,
            ds_guard: Arc::default(),
            lock: Arc::new(Semaphore::new(1)),
            pin_index: Arc::default(),
        }
    }

    // Instead of having the file be the key itself, we would split the key into segements with all but the last representing as a directory
    // with the final item being a file.
    fn key(&self, key: &[u8]) -> Option<(String, String)> {
        let key = String::from_utf8_lossy(key);
        let mut key_segments = key.split('/').collect::<Vec<_>>();

        for (i, segment) in key_segments.iter().enumerate() {
            if *segment == "."
                || *segment == ".."
                || segment.contains('\0')
                || segment.contains('\\')
            {
                return None;
            }
            if segment.is_empty() && i != 0 {
                return None;
            }
        }

        let last = key_segments.pop().filter(|segment| !segment.is_empty())?;
        let key_val = format!("{last}.data");

        let key_path_raw = key_segments.join("/");
        let key_path = match key_path_raw.strip_prefix('/') {
            Some(rest) => rest.to_string(),
            None => key_path_raw,
        };

        Some((key_path, key_val))
    }

    async fn write(&self, key: &[u8], val: &[u8]) -> std::io::Result<()> {
        let data_path = self.path.join("data");
        let (rel, key) = self
            .key(key)
            .ok_or::<std::io::Error>(std::io::ErrorKind::NotFound.into())?;

        let dir = data_path.join(rel);
        let final_path = dir.join(key);
        let val = val.to_vec();

        tokio::task::spawn_blocking(move || {
            std::fs::create_dir_all(&dir)?;
            if final_path.is_dir() {
                return Err(std::io::Error::from(std::io::ErrorKind::Other));
            }
            write_atomic(&dir, &final_path, &val)
        })
        .await
        .map_err(std::io::Error::other)?
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
        let Some((path, key)) = self.key(key) else {
            return Ok(());
        };
        let path = data_path.join(path).join(key);
        match tokio::fs::remove_file(path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
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
    let st = async_stream::stream! {
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
                if path.extension().and_then(|e| e.to_str()) != Some("data") {
                    continue;
                }

                let root_str = data_path.to_string_lossy().to_string();
                let path_str = path.to_string_lossy().to_string();
                let raw_key = &path_str[root_str.len()..];
                if raw_key.is_empty() {
                    continue;
                }

                let Some(key) = raw_key.strip_suffix(".data") else {
                    continue;
                };

                if let Ok(bytes) = tokio::fs::read(path).await {
                    let key = key.as_bytes().to_vec();
                    yield (key, bytes)
                }
            }
        }
    };

    st.boxed()
}

fn write_atomic(dir: &Path, final_path: &Path, val: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let mut temp_name = final_path.as_os_str().to_owned();
    temp_name.push(format!(
        ".{}.{}.tmp",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    let temp_path = PathBuf::from(temp_name);

    let res = (|| {
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&temp_path)?;
        f.write_all(val)?;
        f.flush()?;
        f.sync_all()?;
        std::fs::rename(&temp_path, final_path)
    })();

    match res {
        Ok(()) => {
            // best-effort: make the rename durable across power loss (no-op where unsupported).
            if let Ok(d) = std::fs::File::open(dir) {
                let _ = d.sync_all();
            }
            Ok(())
        }
        Err(e) => {
            let _ = std::fs::remove_file(&temp_path);
            Err(e)
        }
    }
}

/// The column operations are all unimplemented pending at least downscoping of the
/// DataStore trait itself.
impl DataStore for FsDataStore {
    async fn init(&self) -> Result<(), Error> {
        // Although `pins` directory is created when inserting a data, is it not created when there are any attempts at listing the pins (thus causing to fail)
        tokio::fs::create_dir_all(&self.path.join("pins")).await?;
        tokio::fs::create_dir_all(&self.path.join("data")).await?;

        // disk is the source of truth; rebuild the indirect-pin reverse index from it once.
        let mut index: HashMap<Cid, HashSet<Cid>> = HashMap::new();
        let recursives = self.list_pinfiles().await.try_filter_map(|(cid, mode)| {
            futures::future::ready(Ok((mode == PinMode::Recursive).then_some(cid)))
        });
        futures::pin_mut!(recursives);
        while let Some(root) = TryStreamExt::try_next(&mut recursives).await? {
            let (_, refs) = read_recursively_pinned(self.path.join("pins"), root).await?;
            for r in refs {
                index.entry(r).or_default().insert(root);
            }
        }
        *self.pin_index.write() = index;
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

impl PinStore for FsDataStore {
    async fn is_pinned(&self, cid: &Cid) -> Result<bool, Error> {
        let path = pin_path(self.path.join("pins"), cid);

        if read_direct_or_recursive(path).await?.is_some() {
            return Ok(true);
        }

        // indirect: a reverse-index lookup rather than scanning every recursive pin file
        Ok(self
            .pin_index
            .read()
            .get(cid)
            .is_some_and(|roots| !roots.is_empty()))
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
        let index = self.pin_index.clone();
        let target = *target;

        tokio::task::spawn_blocking(move || {
            let _permit = permit; // again move to the threadpool thread
            let _entered = span.enter();

            std::fs::create_dir_all(path.parent().expect("shard parent has to exist"))?;

            // previous refs, if re-pinning, so the reverse index can drop stale entries
            let old_refs = sync_read_recursive_refs(&path.with_extension("recursive"));

            let count = set.len();
            let new_refs: Vec<Cid> = set.iter().copied().collect();
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

            let mut idx = index.write();
            for r in old_refs {
                if let Some(roots) = idx.get_mut(&r) {
                    roots.remove(&target);
                    if roots.is_empty() {
                        idx.remove(&r);
                    }
                }
            }
            for r in new_refs {
                idx.entry(r).or_default().insert(target);
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
        let index = self.pin_index.clone();
        let target = *target;

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

            // read the refs before removing so the reverse index can be reconciled
            let refs = sync_read_recursive_refs(&path.with_extension("recursive"));
            path.set_extension("recursive");

            let mut removed_recursive = false;
            match std::fs::remove_file(&path) {
                Ok(_) => {
                    trace!("recursive pin removed");
                    any |= true;
                    removed_recursive = true;
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    // we may have removed only the direct pin, but if we cleaned out a direct pin
                    // this would have been a success
                }
                Err(e) => return Err(e.into()),
            }

            if removed_recursive {
                let mut idx = index.write();
                for r in refs {
                    if let Some(roots) = idx.get_mut(&r) {
                        roots.remove(&target);
                        if roots.is_empty() {
                            idx.remove(&r);
                        }
                    }
                }
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
    ) -> BoxStream<'static, Result<(Cid, PinMode), Error>> {
        // no locking, dirty reads are probably good enough until gc
        let cids = self.list_pinfiles().await;

        let pin_index = self.pin_index.clone();

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

            let mut direct: HashSet<Cid> = HashSet::default();

            let collect_recursive_for_indirect = requirement.is_indirect_or_any();

            futures::pin_mut!(cids);

            while let Some((cid, mode)) = TryStreamExt::try_next(&mut cids).await? {

                let matches = requirement.matches(&mode);

                if mode == PinMode::Recursive {
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

            let indirect: Vec<Cid> = pin_index.read().keys().copied().collect();
            for cid in indirect {
                if returned.insert(cid) {
                    yield (cid, PinMode::Indirect);
                }
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

                    if let Some(mode) = sync_read_direct_or_recursive(&mut path)
                        && searched_suffix.matches(&mode)
                    {
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

            // reverse-index lookup; any cid still unresolved after this is genuinely not pinned
            let idx = self.pin_index.read();
            remaining.retain(
                |cid, i| match idx.get(cid).and_then(|roots| roots.iter().next()) {
                    Some(referring) => {
                        response[*i] = Some((*cid, PinKind::IndirectFrom(*referring)));
                        false
                    }
                    None => true,
                },
            );
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

/// Reads a `<cid>.recursive` pin file's referenced cids, returning an empty Vec if the file is
/// missing or unparseable (same tolerance as `read_recursively_pinned`).
fn sync_read_recursive_refs(recursive_path: &Path) -> Vec<Cid> {
    let Ok(contents) = std::fs::read(recursive_path) else {
        return Vec::new();
    };
    let Ok(cids) = serde_json::from_slice::<Vec<&str>>(&contents) else {
        return Vec::new();
    };
    cids.into_iter()
        .filter_map(|s| Cid::try_from(s).ok())
        .collect()
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
    use serde::{Serializer, ser::SerializeSeq};
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
    use crate::repo::{DataStore, datastore::flatfs::FsDataStore};

    #[tokio::test]
    async fn test_kv_datastore() -> anyhow::Result<()> {
        let tmp = std::env::temp_dir();
        let store = FsDataStore::new(tmp.clone());
        let key = [1, 2, 3, 4];
        let value = [5, 6, 7, 8];

        store.init().await?;

        let contains = store.contains(&key).await?;
        assert!(!contains);
        let get = store.get(&key).await.unwrap_or_default();
        assert_eq!(get, None);
        store.remove(&key).await?;

        store.put(&key, &value).await?;
        let contains = store.contains(&key).await?;
        assert!(contains);
        let get = store.get(&key).await?;
        assert_eq!(get, Some(value.to_vec()));

        store.remove(&key).await?;
        let contains = store.contains(&key).await?;
        assert!(!contains);
        let get = store.get(&key).await.unwrap_or_default();
        assert_eq!(get, None);
        drop(store);
        Ok(())
    }

    #[tokio::test]
    async fn kv_dotted_keys_and_traversal() -> anyhow::Result<()> {
        use futures::StreamExt;
        let tmp = std::env::temp_dir().join(format!("rust_ipfs_kv_dots_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        let store = FsDataStore::new(tmp.clone());
        store.init().await?;

        store.put(b"/ns/a.b", &[1]).await?;
        store.put(b"/ns/a.c", &[2]).await?;
        assert_eq!(store.get(b"/ns/a.b").await?, Some(vec![1]));
        assert_eq!(
            store.get(b"/ns/a.c").await?,
            Some(vec![2]),
            "keys differing only after a dot must not collide"
        );

        assert!(
            store.put(b"/ns/../escape", &[3]).await.is_err(),
            "a traversal key must be rejected"
        );

        let mut found = std::collections::HashMap::new();
        let mut stream = store.iter().await;
        while let Some((k, v)) = stream.next().await {
            found.insert(k, v);
        }
        assert_eq!(
            found.get(b"/ns/a.b".as_slice()),
            Some(&vec![1]),
            "iter must round-trip the exact key bytes"
        );

        drop(store);
        let _ = std::fs::remove_dir_all(&tmp);
        Ok(())
    }

    fn count_temp_files(dir: &std::path::Path) -> usize {
        let mut n = 0;
        if let Ok(rd) = std::fs::read_dir(dir) {
            for e in rd.flatten() {
                let p = e.path();
                if p.is_dir() {
                    n += count_temp_files(&p);
                } else if p.extension().and_then(|x| x.to_str()) == Some("tmp") {
                    n += 1;
                }
            }
        }
        n
    }

    #[tokio::test]
    async fn kv_put_is_atomic_and_leaves_no_temp() -> anyhow::Result<()> {
        let dir = std::env::temp_dir().join("rust_ipfs_ds_atomic");
        let _ = std::fs::remove_dir_all(&dir);
        let store = FsDataStore::new(dir.clone());
        store.init().await?;

        let key = b"ipns/seq".to_vec();
        store.put(&key, b"v1").await?;
        assert_eq!(store.get(&key).await?, Some(b"v1".to_vec()));

        // overwriting renames a fresh temp over the live value (atomic, no torn write).
        store.put(&key, b"v2-longer").await?;
        assert_eq!(store.get(&key).await?, Some(b"v2-longer".to_vec()));

        assert_eq!(
            count_temp_files(&dir.join("data")),
            0,
            "write temps must be cleaned up"
        );

        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }

    #[tokio::test]
    async fn iter_skips_write_temps() -> anyhow::Result<()> {
        use futures::StreamExt;

        let dir = std::env::temp_dir().join("rust_ipfs_ds_iter_temp");
        let _ = std::fs::remove_dir_all(&dir);
        let store = FsDataStore::new(dir.clone());
        store.init().await?;

        store.put(b"k", b"real").await?;

        // simulate a crash-leftover write temp sitting next to the real value.
        let data_dir = dir.join("data");
        assert!(data_dir.join("k.data").is_file());
        std::fs::write(data_dir.join("k.data.999.0.tmp"), b"garbage")?;

        let items: Vec<(Vec<u8>, Vec<u8>)> = store.iter().await.collect().await;
        assert_eq!(items.len(), 1, "iter must skip the .tmp file: {items:?}");
        assert_eq!(items[0].1, b"real".to_vec());

        let _ = std::fs::remove_dir_all(&dir);
        Ok(())
    }

    #[tokio::test]
    async fn pin_index_rebuilds_on_init() {
        use crate::repo::PinStore;
        use futures::StreamExt;
        use ipld_core::cid::Cid;
        use std::convert::TryFrom;

        let dir = std::env::temp_dir().join("rust_ipfs_pin_index_rebuild");
        let _ = std::fs::remove_dir_all(&dir);

        let root = Cid::try_from("QmX5S2xLu32K6WxWnyLeChQFbDHy79ULV9feJYH2Hy9bgp").unwrap();
        let empty = Cid::try_from("QmbFMke1KXqnYyBBWxB74N4c5SBnJMVAiMNRcGu6x1AwQH").unwrap();

        {
            let store = FsDataStore::new(dir.clone());
            store.init().await.unwrap();
            store
                .insert_recursive_pin(&root, futures::stream::iter(vec![Ok(empty)]).boxed())
                .await
                .unwrap();
            assert!(store.is_pinned(&empty).await.unwrap());
        }

        // a fresh store on the same path must reconstruct the index from disk in init()
        let store = FsDataStore::new(dir.clone());
        store.init().await.unwrap();
        assert!(
            store.is_pinned(&empty).await.unwrap(),
            "indirect pin lost across restart"
        );
        assert!(store.is_pinned(&root).await.unwrap());

        // unpinning the root drops the indirect entry from the index
        store
            .remove_recursive_pin(&root, futures::stream::empty().boxed())
            .await
            .unwrap();
        assert!(
            !store.is_pinned(&empty).await.unwrap(),
            "indirect survived unpin"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }
}
