//! Persistent fs backed repo.
//!
//! Consists of [`FsDataStore`] and [`FsBlockStore`].

use parking_lot::Mutex;
use std::fs::File;
use std::path::PathBuf;

use super::{Lock, LockError};

#[derive(Debug)]
pub struct FsLock {
    file: Mutex<Option<File>>,
    path: PathBuf,
    state: Mutex<State>,
}

#[derive(Debug)]
enum State {
    Unlocked,
    Exclusive,
}

impl FsLock {
    pub fn new(path: PathBuf) -> Self {
        Self {
            file: Mutex::new(None),
            path,
            state: Mutex::new(State::Unlocked),
        }
    }
}

impl Lock for FsLock {
    fn try_exclusive(&self) -> Result<(), LockError> {
        use fs2::FileExt;
        use std::fs::OpenOptions;

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
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
