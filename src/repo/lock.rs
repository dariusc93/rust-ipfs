//! Persistent fs backed repo.
//!
//! Consists of [`FsDataStore`] and [`FsBlockStore`].

use super::{Lock, LockError};

#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug)]
pub struct FsLock {
    file: parking_lot::Mutex<Option<std::fs::File>>,
    path: std::path::PathBuf,
    state: parking_lot::Mutex<State>,
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
        Self {
            file: parking_lot::Mutex::new(None),
            path,
            state: parking_lot::Mutex::new(State::Unlocked),
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl Lock for FsLock {
    fn try_exclusive(&self) -> Result<(), LockError> {
        use fs2::FileExt;
        use std::fs::OpenOptions;

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&self.path)?;

        file.try_lock_exclusive()?;

        *self.state.lock() = State::Exclusive;
        *self.file.lock() = Some(file);

        Ok(())
    }
}

#[derive(Debug, Default)]
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
