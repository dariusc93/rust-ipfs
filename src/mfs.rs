use crate::repo::{DataStore, DefaultStorage, Repo};
use crate::{Block, Error};
use anyhow::anyhow;
use ipld_core::cid::{Cid, Version};
use multihash_codetable::Code;
use rust_unixfs::dir::builder::{BufferingTreeBuilder, TreeOptions};
use rust_unixfs::dir::{describe, NodeDescription};
use rust_unixfs::file::adder::FileAdder;
use rust_unixfs::file::visit::IdleFileVisit;
use std::collections::BTreeMap;

/// Datastore key holding the current MFS root Cid.
const ROOT_KEY: &[u8] = b"/mfs/root";

const VERSION: Version = Version::V1;
const HASHER: Code = Code::Sha2_256;

/// A directory's immediate children: name -> (target Cid, cumulative dag size used as the link Tsize).
type DirMap = BTreeMap<String, DirEntry>;

#[derive(Debug, Clone, Copy)]
struct DirEntry {
    cid: Cid,
    tsize: u64,
}

/// The kind of an MFS entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MfsKind {
    File { size: u64 },
    Directory,
    Symlink,
}

/// A single entry in an MFS directory listing.
#[derive(Debug, Clone)]
pub struct MfsEntry {
    pub name: String,
    pub cid: Cid,
    pub kind: MfsKind,
}

/// The result of `stat`.
#[derive(Debug, Clone)]
pub struct MfsStat {
    pub cid: Cid,
    pub kind: MfsKind,
    /// Logical size: file size for files, cumulative dag size (link Tsize) for directories.
    pub size: u64,
}

/// Handle to the node's mutable filesystem. Obtain it via [`crate::Ipfs::mfs`].
#[derive(Clone)]
pub struct Mfs {
    repo: Repo<DefaultStorage>,
}

impl Mfs {
    pub(crate) fn new(repo: Repo<DefaultStorage>) -> Self {
        Self { repo }
    }

    fn repo(&self) -> &Repo<DefaultStorage> {
        &self.repo
    }

    /// Returns the current MFS root Cid, or `None` if nothing has been written yet.
    pub async fn root(&self) -> Result<Option<Cid>, Error> {
        let mut guard = self.repo().inner.mfs_root.lock().await;
        self.cached_root(&mut guard).await
    }

    /// Creates a directory at `path`. With `parents`, missing intermediate directories are created;
    /// otherwise a missing parent is an error.
    pub async fn mkdir(&self, path: &str, parents: bool) -> Result<(), Error> {
        let (cid, tsize, blocks) = encode_dir(&DirMap::new())?;
        self.set_entry(path, DirEntry { cid, tsize }, blocks, parents, false)
            .await
    }

    /// Writes `data` as the file at `path`, creating it or overwriting an existing file. With
    /// `parents`, missing intermediate directories are created. Errors if `path` is a directory.
    pub async fn write(&self, path: &str, data: &[u8], parents: bool) -> Result<(), Error> {
        let comps = split_path(path)?;
        if comps.is_empty() {
            return Err(anyhow!("cannot write to the root directory"));
        }
        if let Ok((cid, tsize)) = self.resolve(&comps).await
            && matches!(self.classify(&cid, tsize).await?, MfsKind::Directory)
        {
            return Err(anyhow!("'{path}' is a directory"));
        }

        let (cid, tsize, blocks) = encode_file(data)?;
        self.set_entry(path, DirEntry { cid, tsize }, blocks, parents, true)
            .await
    }

    /// Reads the whole content of the file at `path`.
    pub async fn read(&self, path: &str) -> Result<Vec<u8>, Error> {
        let comps = split_path(path)?;
        if comps.is_empty() {
            return Err(anyhow!("cannot read the root directory"));
        }
        let root = self
            .snapshot_root()
            .await?
            .ok_or_else(|| anyhow!("MFS is empty"))?;
        let _gc = self.repo().gc_guard().await;
        let (cid, _) = self.resolve_from(root, &comps).await?;
        self.read_file(&cid).await
    }

    /// Removes the entry at `path`. A non-empty directory requires `recursive`.
    pub async fn rm(&self, path: &str, recursive: bool) -> Result<(), Error> {
        let comps = split_path(path)?;
        let Some((name, dirs)) = comps.split_last() else {
            return Err(anyhow!("cannot remove the root directory"));
        };

        let mut guard = self.repo().inner.mfs_root.lock().await;
        let (mut frames, names) = self.load_chain(&mut guard, dirs, false).await?;

        let entry = frames
            .last()
            .expect("root frame")
            .get(name)
            .copied()
            .ok_or_else(|| anyhow!("path not found: {path}"))?;

        if !recursive
            && let Ok(map) = self.load_dir(&entry.cid).await
            && !map.is_empty()
        {
            return Err(anyhow!("'{path}' is a non-empty directory; pass recursive"));
        }

        frames.last_mut().expect("root frame").remove(name);
        self.reencode_and_commit(&mut guard, frames, names, Vec::new())
            .await
    }

    /// Copies the MFS entry at `from` to `to` (both MFS paths). Copying from `/ipfs` is a later phase.
    pub async fn cp(&self, from: &str, to: &str, parents: bool) -> Result<(), Error> {
        let from_comps = split_path(from)?;
        let (cid, tsize) = self.resolve(&from_comps).await?;
        self.set_entry(to, DirEntry { cid, tsize }, Vec::new(), parents, false)
            .await
    }

    /// Moves the MFS entry at `from` to `to`. Non-atomic: it links the destination then unlinks the
    /// source as two commits, so a failure between them leaves the entry at both paths (never lost).
    pub async fn mv(&self, from: &str, to: &str, parents: bool) -> Result<(), Error> {
        let from_comps = split_path(from)?;
        if from_comps.is_empty() {
            return Err(anyhow!("cannot move the root directory"));
        }
        let (cid, tsize) = self.resolve(&from_comps).await?;
        // link at the destination first (safe to fail), then unlink the source
        self.set_entry(to, DirEntry { cid, tsize }, Vec::new(), parents, true)
            .await?;
        self.rm(from, true).await
    }

    /// Lists the immediate entries of the directory at `path` (`/` for the root).
    pub async fn ls(&self, path: &str) -> Result<Vec<MfsEntry>, Error> {
        let comps = split_path(path)?;
        let Some(root) = self.snapshot_root().await? else {
            if comps.is_empty() {
                return Ok(Vec::new());
            }
            return Err(anyhow!("MFS is empty"));
        };

        let _gc = self.repo().gc_guard().await;
        let (cid, _) = self.resolve_from(root, &comps).await?;
        let map = self.load_dir(&cid).await?;

        let mut out = Vec::with_capacity(map.len());
        for (name, entry) in map {
            let kind = self.classify(&entry.cid, entry.tsize).await?;
            out.push(MfsEntry {
                name,
                cid: entry.cid,
                kind,
            });
        }
        Ok(out)
    }

    /// Returns type and size information for the entry at `path`.
    pub async fn stat(&self, path: &str) -> Result<MfsStat, Error> {
        let comps = split_path(path)?;
        let Some(root) = self.snapshot_root().await? else {
            if comps.is_empty() {
                let (cid, tsize, _) = encode_dir(&DirMap::new())?;
                return Ok(MfsStat {
                    cid,
                    kind: MfsKind::Directory,
                    size: tsize,
                });
            }
            return Err(anyhow!("MFS is empty"));
        };

        let _gc = self.repo().gc_guard().await;
        let (cid, tsize) = self.resolve_from(root, &comps).await?;
        let kind = self.classify(&cid, tsize).await?;
        let size = match kind {
            MfsKind::File { size } => size,
            MfsKind::Directory | MfsKind::Symlink => tsize,
        };
        Ok(MfsStat { cid, kind, size })
    }

    // --- internals -------------------------------------------------------------------------------

    /// Loads the current root Cid, reading the datastore on first access. The cache distinguishes
    /// "not loaded yet" (`bool == false`) from "loaded, no root" (`None`).
    async fn cached_root(&self, guard: &mut (bool, Option<Cid>)) -> Result<Option<Cid>, Error> {
        if !guard.0 {
            guard.1 = match self.repo().data_store().get(ROOT_KEY).await? {
                Some(bytes) => Some(Cid::try_from(bytes.as_slice())?),
                None => None,
            };
            guard.0 = true;
        }
        Ok(guard.1)
    }

    /// A point-in-time snapshot of the root Cid for read operations.
    async fn snapshot_root(&self) -> Result<Option<Cid>, Error> {
        let mut guard = self.repo().inner.mfs_root.lock().await;
        self.cached_root(&mut guard).await
    }

    /// Inserts or replaces the entry at `path` with `entry`, storing `entry_blocks`. With `parents`,
    /// missing intermediate directories are created. When `overwrite` is false an existing entry is
    /// an error.
    async fn set_entry(
        &self,
        path: &str,
        entry: DirEntry,
        entry_blocks: Vec<Block>,
        parents: bool,
        overwrite: bool,
    ) -> Result<(), Error> {
        let comps = split_path(path)?;
        let Some((name, dirs)) = comps.split_last() else {
            return Err(anyhow!("cannot replace the root directory"));
        };

        let mut guard = self.repo().inner.mfs_root.lock().await;
        let (mut frames, names) = self.load_chain(&mut guard, dirs, parents).await?;

        let parent = frames.last_mut().expect("root frame");
        if !overwrite && parent.contains_key(name) {
            return Err(anyhow!("'{path}' already exists"));
        }
        parent.insert(name.clone(), entry);

        self.reencode_and_commit(&mut guard, frames, names, entry_blocks)
            .await
    }

    /// Loads the chain of directory maps from the root along `dirs`. `frames[0]` is the root and
    /// `names[i]` is the component linking `frames[i]` to `frames[i+1]`. With `create`, missing
    /// intermediate directories become empty maps.
    async fn load_chain(
        &self,
        guard: &mut (bool, Option<Cid>),
        dirs: &[String],
        create: bool,
    ) -> Result<(Vec<DirMap>, Vec<String>), Error> {
        let root_map = match self.cached_root(guard).await? {
            Some(cid) => self.load_dir(&cid).await?,
            None => DirMap::new(),
        };

        let mut frames = vec![root_map];
        let mut names = Vec::with_capacity(dirs.len());

        for comp in dirs {
            let next = match frames.last().expect("non-empty").get(comp).copied() {
                Some(entry) => self.load_dir(&entry.cid).await?,
                None if create => DirMap::new(),
                None => return Err(anyhow!("directory '{comp}' does not exist")),
            };
            names.push(comp.clone());
            frames.push(next);
        }

        Ok((frames, names))
    }

    /// Re-encodes every frame bottom-up (each parent's link updated to the child's new Cid), stores
    /// all produced blocks, and commits the new root.
    async fn reencode_and_commit(
        &self,
        guard: &mut (bool, Option<Cid>),
        mut frames: Vec<DirMap>,
        names: Vec<String>,
        mut blocks: Vec<Block>,
    ) -> Result<(), Error> {
        for i in (0..frames.len()).rev() {
            let (cid, tsize, mut blks) = encode_dir(&frames[i])?;
            blocks.append(&mut blks);
            if i == 0 {
                return self.commit_root(guard, cid, blocks).await;
            }
            frames[i - 1].insert(names[i - 1].clone(), DirEntry { cid, tsize });
        }
        unreachable!("frames always contains the root at index 0")
    }

    /// Persists `new_root`: stores its blocks, recursively pins the new tree, unpins the old root,
    /// writes the root Cid to the datastore, and updates the cache.
    async fn commit_root(
        &self,
        guard: &mut (bool, Option<Cid>),
        new_root: Cid,
        blocks: Vec<Block>,
    ) -> Result<(), Error> {
        let old = guard.1;

        self.repo().put_blocks(blocks).await?;
        self.repo().pin(new_root).recursive().await?;
        self.repo()
            .data_store()
            .put(ROOT_KEY, &new_root.to_bytes())
            .await?;

        guard.1 = Some(new_root);
        guard.0 = true;

        if let Some(old) = old
            && old != new_root
        {
            // best-effort: the new tree is already pinned, so a failed unpin only delays GC
            let _ = self.repo().remove_pin(old).recursive().await;
        }
        Ok(())
    }

    /// Resolves a path (component list, empty == root) to its `(cid, link tsize)`, holding off GC
    /// for the duration of the navigation.
    async fn resolve(&self, comps: &[String]) -> Result<(Cid, u64), Error> {
        let root = self
            .snapshot_root()
            .await?
            .ok_or_else(|| anyhow!("MFS is empty"))?;
        let _gc = self.repo().gc_guard().await;
        self.resolve_from(root, comps).await
    }

    /// Navigates from `root` along `comps`. The caller must hold a GC guard, since this reads
    /// immutable blocks with `get_block_now`.
    async fn resolve_from(&self, root: Cid, comps: &[String]) -> Result<(Cid, u64), Error> {
        let mut cid = root;
        let mut tsize = 0;
        for comp in comps {
            let map = self.load_dir(&cid).await?;
            let entry = map
                .get(comp)
                .ok_or_else(|| anyhow!("path not found: {comp}"))?;
            cid = entry.cid;
            tsize = entry.tsize;
        }
        Ok((cid, tsize))
    }

    /// Loads a flat directory's children. Errors on a HAMT shard (phase 1) or a non-directory.
    async fn load_dir(&self, cid: &Cid) -> Result<DirMap, Error> {
        let block = self.get_block(cid).await?;
        match describe(block.data()) {
            NodeDescription::Directory { links } => Ok(links
                .into_iter()
                .map(|l| {
                    (
                        l.name,
                        DirEntry {
                            cid: l.target,
                            tsize: l.tsize,
                        },
                    )
                })
                .collect()),
            NodeDescription::HamtShard => Err(anyhow!(
                "{cid} is a HAMT-sharded directory; not supported in MFS phase 1"
            )),
            _ => Err(anyhow!("{cid} is not a directory")),
        }
    }

    /// Classifies an entry by its block; `link_tsize` is the cumulative size recorded by the parent.
    async fn classify(&self, cid: &Cid, _link_tsize: u64) -> Result<MfsKind, Error> {
        let block = self.get_block(cid).await?;
        Ok(match describe(block.data()) {
            NodeDescription::Directory { .. } | NodeDescription::HamtShard => MfsKind::Directory,
            NodeDescription::File { size } => MfsKind::File { size },
            NodeDescription::Symlink => MfsKind::Symlink,
            NodeDescription::Other => MfsKind::File {
                size: block.data().len() as u64,
            },
        })
    }

    /// Reads a UnixFS file DAG (or raw leaf) rooted at `cid` into a byte vector, fetching blocks
    /// locally (the MFS tree is pinned and present).
    async fn read_file(&self, cid: &Cid) -> Result<Vec<u8>, Error> {
        let block = self.get_block(cid).await?;

        // a single raw leaf is the content itself
        if matches!(describe(block.data()), NodeDescription::Other) {
            return Ok(block.data().to_vec());
        }

        let mut out = Vec::new();
        let mut cache = None;
        let (content, _, _, mut step) = IdleFileVisit::default().start(block.data())?;
        out.extend_from_slice(content);

        while let Some(visit) = step {
            let next = *visit.pending_links().0;
            let block = self.get_block(&next).await?;
            let (content, next_step) = visit.continue_walk(block.data(), &mut cache)?;
            out.extend_from_slice(content);
            step = next_step;
        }

        Ok(out)
    }

    async fn get_block(&self, cid: &Cid) -> Result<Block, Error> {
        self.repo()
            .get_block_now(cid)
            .await?
            .ok_or_else(|| anyhow!("missing block {cid}"))
    }
}

/// Splits an MFS path into its components, rejecting `.`/`..` and treating `/` as the root (empty).
fn split_path(path: &str) -> Result<Vec<String>, Error> {
    let mut comps = Vec::new();
    for segment in path.split('/') {
        match segment {
            "" => continue,
            "." | ".." => return Err(anyhow!("'.' and '..' are not supported in MFS paths")),
            other => comps.push(other.to_string()),
        }
    }
    Ok(comps)
}

/// Encodes a single flat directory node from its children, returning its Cid, cumulative size, and
/// every block produced (the directory node, last). HAMT sharding is disabled in phase 1.
fn encode_dir(entries: &DirMap) -> Result<(Cid, u64, Vec<Block>), Error> {
    let mut opts = TreeOptions::default();
    opts.wrap_with_directory();
    opts.cid_version(VERSION);
    opts.hasher(HASHER);
    opts.shard_threshold(None);

    let mut builder = BufferingTreeBuilder::new(opts);
    for (name, entry) in entries {
        builder.put_link(name, entry.cid, entry.tsize)?;
    }

    let mut blocks = Vec::new();
    let mut root = None;
    for node in builder.build() {
        let node = node?;
        root = Some((node.cid, node.total_size));
        blocks.push(Block::new(node.cid, node.block.into_vec())?);
    }

    let (cid, tsize) = root.ok_or_else(|| anyhow!("directory produced no node"))?;
    Ok((cid, tsize, blocks))
}

/// Encodes `data` as a UnixFS file DAG, returning its root Cid, cumulative dag size (the link
/// Tsize), and every produced block (root last).
fn encode_file(data: &[u8]) -> Result<(Cid, u64, Vec<Block>), Error> {
    let mut adder = FileAdder::builder()
        .with_cid_version(VERSION)
        .with_hasher(HASHER)
        .build();

    let mut blocks = Vec::new();
    let mut tsize = 0u64;
    let mut root = None;

    let mut push = |cid: Cid, block: Vec<u8>| -> Result<(), Error> {
        tsize += block.len() as u64;
        root = Some(cid);
        blocks.push(Block::new(cid, block)?);
        Ok(())
    };

    let mut offset = 0;
    while offset < data.len() {
        let (ready, consumed) = adder.push(&data[offset..]);
        for (cid, block) in ready {
            push(cid, block)?;
        }
        offset += consumed;
    }
    for (cid, block) in adder.finish() {
        push(cid, block)?;
    }

    let cid = root.ok_or_else(|| anyhow!("file produced no blocks"))?;
    Ok((cid, tsize, blocks))
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn mfs() -> Mfs {
        let repo = Repo::new_memory();
        repo.init().await.unwrap();
        Mfs::new(repo)
    }

    #[tokio::test]
    async fn mkdir_ls_stat() {
        let mfs = mfs().await;

        mfs.mkdir("/a/b/c", true).await.unwrap();

        let top = mfs.ls("/").await.unwrap();
        assert_eq!(top.len(), 1);
        assert_eq!(top[0].name, "a");
        assert_eq!(top[0].kind, MfsKind::Directory);

        let inner = mfs.ls("/a/b").await.unwrap();
        assert_eq!(inner.len(), 1);
        assert_eq!(inner[0].name, "c");

        let st = mfs.stat("/a/b/c").await.unwrap();
        assert_eq!(st.kind, MfsKind::Directory);
    }

    #[tokio::test]
    async fn empty_root_lists_empty() {
        let mfs = mfs().await;
        assert!(mfs.ls("/").await.unwrap().is_empty());
        assert_eq!(mfs.stat("/").await.unwrap().kind, MfsKind::Directory);
        assert!(mfs.root().await.unwrap().is_none(), "reads must not create a root");
    }

    #[tokio::test]
    async fn write_read_roundtrip() {
        let mfs = mfs().await;

        mfs.write("/docs/hello.txt", b"hello mfs", true).await.unwrap();
        assert_eq!(mfs.read("/docs/hello.txt").await.unwrap(), b"hello mfs");

        let st = mfs.stat("/docs/hello.txt").await.unwrap();
        assert_eq!(st.kind, MfsKind::File { size: 9 });

        // overwrite
        mfs.write("/docs/hello.txt", b"changed", false).await.unwrap();
        assert_eq!(mfs.read("/docs/hello.txt").await.unwrap(), b"changed");

        // a directory listing reflects the file
        let entries = mfs.ls("/docs").await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "hello.txt");
        assert!(matches!(entries[0].kind, MfsKind::File { .. }));
    }

    #[tokio::test]
    async fn write_read_multiblock() {
        let mfs = mfs().await;
        // larger than the default chunk size so the file becomes a multi-block DAG
        let data: Vec<u8> = (0..1_000_000u32).map(|i| i as u8).collect();

        mfs.write("/big.bin", &data, false).await.unwrap();
        assert_eq!(mfs.read("/big.bin").await.unwrap(), data);

        match mfs.stat("/big.bin").await.unwrap().kind {
            MfsKind::File { size } => assert_eq!(size, data.len() as u64),
            other => panic!("expected file, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn rm_cp_mv() {
        let mfs = mfs().await;
        mfs.write("/a/file", b"data", true).await.unwrap();

        // cp duplicates
        mfs.cp("/a/file", "/a/copy", false).await.unwrap();
        assert_eq!(mfs.read("/a/copy").await.unwrap(), b"data");
        assert_eq!(mfs.read("/a/file").await.unwrap(), b"data");

        // mv relocates (source gone, dest present)
        mfs.mkdir("/b", false).await.unwrap();
        mfs.mv("/a/copy", "/b/moved", false).await.unwrap();
        assert_eq!(mfs.read("/b/moved").await.unwrap(), b"data");
        assert!(mfs.read("/a/copy").await.is_err());

        // rm removes
        mfs.rm("/a/file", false).await.unwrap();
        assert!(mfs.read("/a/file").await.is_err());

        // non-empty dir requires recursive
        assert!(mfs.rm("/b", false).await.is_err());
        mfs.rm("/b", true).await.unwrap();
        assert!(mfs.ls("/b").await.is_err());
    }

    #[tokio::test]
    async fn mkdir_without_parents_errors_on_missing_parent() {
        let mfs = mfs().await;
        assert!(mfs.mkdir("/x/y", false).await.is_err());
        mfs.mkdir("/x", false).await.unwrap();
        mfs.mkdir("/x/y", false).await.unwrap();
        assert!(mfs.mkdir("/x/y", false).await.is_err(), "duplicate mkdir must fail");
    }

    #[tokio::test]
    async fn mfs_tree_survives_gc() {
        let repo = Repo::new_memory();
        repo.init().await.unwrap();
        let mfs = Mfs::new(repo.clone());

        mfs.write("/a/keep.txt", b"survive gc", true).await.unwrap();

        // GC removes everything not pinned; the recursively-pinned MFS root must keep the tree alive
        repo.cleanup().await.unwrap();

        assert_eq!(mfs.read("/a/keep.txt").await.unwrap(), b"survive gc");
        assert_eq!(mfs.ls("/a").await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn root_persists_across_handles() {
        let repo = Repo::new_memory();
        repo.init().await.unwrap();

        let root_cid = {
            let mfs = Mfs::new(repo.clone());
            mfs.mkdir("/keep", true).await.unwrap();
            mfs.root().await.unwrap().unwrap()
        };

        // a fresh handle on the same repo reads the persisted root and sees the directory
        let reopened = Mfs::new(repo);
        assert_eq!(reopened.root().await.unwrap(), Some(root_cid));
        let entries = reopened.ls("/").await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "keep");
    }
}
