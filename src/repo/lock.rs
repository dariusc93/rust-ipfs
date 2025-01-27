//! Persistent fs backed repo.
//!
//! Consists of [`FsDataStore`] and [`FsBlockStore`].

use super::{Lock, LockError};

#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug, Clone)]
pub struct FsLock {
    inner: std::sync::Arc<parking_lot::Mutex<FsLockInner>>,
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug)]
struct FsLockInner {
    file: Option<std::fs::File>,
    path: std::path::PathBuf,
    state: State,
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug)]
enum State {
    Unlocked,
    Exclusive,
}

#[cfg(not(target_arch = "wasm32"))]
impl FsLock {
    pub fn new(path: std::path::PathBuf) -> Self {
        let inner = FsLockInner {
            file: None,
            path,
            state: State::Unlocked,
        };
        Self {
            inner: std::sync::Arc::new(parking_lot::Mutex::new(inner)),
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl Lock for FsLock {
    fn try_exclusive(&self) -> Result<(), LockError> {
        let mut inner = self.inner.lock();

        use fs2::FileExt;
        use std::fs::OpenOptions;

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&inner.path)?;

        file.try_lock_exclusive()?;

        inner.state = State::Exclusive;
        inner.file = Some(file);

        Ok(())
    }
}

#[derive(Debug, Default, Clone)]
pub struct MemLock;

impl Lock for MemLock {
    fn try_exclusive(&self) -> Result<(), LockError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{FsLock, Lock};

    #[test]
    fn creates_an_exclusive_repo_lock() {
        let temp_dir = std::env::temp_dir();
        let lockfile_path = temp_dir.join("repo_lock");

        let lock = FsLock::new(lockfile_path.clone());
        let result = lock.try_exclusive();
        assert!(result.is_ok());

        let failing_lock = FsLock::new(lockfile_path.clone());
        let result = failing_lock.try_exclusive();
        assert!(result.is_err());

        // Clean-up.
        std::fs::remove_file(lockfile_path).unwrap();
    }
}
