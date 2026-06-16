#[cfg(not(target_arch = "wasm32"))]
use connexa::keystore::store::fs::FilesystemKeystore as PersistenceKeystore;
use connexa::keystore::{EncryptedEntry, KeyMetadata, Keystore};

#[cfg(target_arch = "wasm32")]
use connexa::keystore::store::indexeddb::IndexedDbKeystore as PersistenceKeystore;

use connexa::keystore::store::memory::MemoryKeystore;
use either::Either;

use connexa::keystore::Error;

type KeystoreResult<T> = Result<T, Error>;

pub struct DefaultKeystore {
    keystore: Either<MemoryKeystore, PersistenceKeystore>,
}

impl Default for DefaultKeystore {
    fn default() -> Self {
        Self {
            keystore: Either::Left(MemoryKeystore::default()),
        }
    }
}

impl DefaultKeystore {
    #[cfg(not(target_arch = "wasm32"))]
    pub fn set_path(path: impl AsRef<std::path::Path>) -> Self {
        Self {
            keystore: Either::Right(PersistenceKeystore::new(path)),
        }
    }

    #[cfg(target_arch = "wasm32")]
    pub fn set_identifier(namespace: impl Into<Option<String>>) -> Self {
        let namespace = namespace.into();
        let namespace = namespace.unwrap_or_else(|| "rust-ipfs".into());
        Self {
            keystore: Either::Right(PersistenceKeystore::new(namespace)),
        }
    }
}

impl Keystore for DefaultKeystore {
    async fn put(&self, entry: EncryptedEntry) -> KeystoreResult<()> {
        match &self.keystore {
            Either::Left(keystore) => keystore.put(entry).await,
            Either::Right(keystore) => keystore.put(entry).await,
        }
    }

    async fn put_many(&self, entries: Vec<EncryptedEntry>) -> KeystoreResult<()> {
        match &self.keystore {
            Either::Left(keystore) => keystore.put_many(entries).await,
            Either::Right(keystore) => keystore.put_many(entries).await,
        }
    }

    async fn get(&self, label: &str) -> KeystoreResult<Option<EncryptedEntry>> {
        match &self.keystore {
            Either::Left(keystore) => keystore.get(label).await,
            Either::Right(keystore) => keystore.get(label).await,
        }
    }

    async fn list(&self) -> KeystoreResult<Vec<KeyMetadata>> {
        match &self.keystore {
            Either::Left(keystore) => keystore.list().await,
            Either::Right(keystore) => keystore.list().await,
        }
    }

    async fn remove(&self, label: &str) -> KeystoreResult<bool> {
        match &self.keystore {
            Either::Left(keystore) => keystore.remove(label).await,
            Either::Right(keystore) => keystore.remove(label).await,
        }
    }
}
