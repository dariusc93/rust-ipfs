use std::{
    collections::{btree_map::Entry, BTreeMap},
    sync::Arc,
};

use anyhow::Error;
use futures::{stream::BoxStream, StreamExt};
use libp2p::identity::{Keypair, PublicKey};
use tokio::sync::Mutex;
use zeroize::Zeroize;

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum KeyType {
    Ed25519,
    Ecdsa,
    Secp256k1,
}

#[derive(Clone)]
pub struct Key {
    key: Vec<u8>,
}

impl Drop for Key {
    fn drop(&mut self) {
        self.zeroize()
    }
}

impl Zeroize for Key {
    fn zeroize(&mut self) {
        self.key.zeroize()
    }
}

impl AsRef<[u8]> for Key {
    fn as_ref(&self) -> &[u8] {
        &self.key
    }
}

impl From<Vec<u8>> for Key {
    fn from(key: Vec<u8>) -> Self {
        Self { key }
    }
}

impl From<&[u8]> for Key {
    fn from(key: &[u8]) -> Self {
        key.to_vec().into()
    }
}

#[derive(Clone)]
pub struct Keystore {
    storage: Arc<dyn KeyStorage>,
}

impl std::fmt::Debug for Keystore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Keystore").finish()
    }
}

impl Keystore {
    /// Create a new keystore
    pub fn new(storage: Arc<dyn KeyStorage>) -> Self {
        Self { storage }
    }

    /// Create an in-memory keystore
    pub fn in_memory() -> Self {
        Self::new(Arc::new(MemoryKeyStorage::default()))
    }

    /// Import a [`Keypair`] into the keystore
    /// If `name` is not supplied, the [`crate::PeerId`] will be the used as the name by default
    pub async fn import_key(
        &self,
        keypair: &Keypair,
        name: Option<&str>,
    ) -> Result<PublicKey, Error> {
        let public_key = keypair.public();

        let peer_id = public_key.to_peer_id().to_string();

        let peer_id_str = peer_id.as_str();

        let name = name.unwrap_or_else(|| peer_id_str);

        let bytes = Key::from(keypair.to_protobuf_encoding()?);

        self.storage.set(name, bytes.as_ref()).await?;

        Ok(public_key)
    }

    /// Generate a Ed25519 Keypair
    /// If `name` is not supplied, the [`crate::PeerId`] will be the used as the name by default
    pub async fn generate_ed25519(&self, name: Option<&str>) -> Result<PublicKey, Error> {
        self.generate_key(name, KeyType::Ed25519).await
    }

    /// Generate a Ecdsa Keypair
    /// If `name` is not supplied, the [`crate::PeerId`] will be the used as the name by default
    pub async fn generate_ecdsa(&self, name: Option<&str>) -> Result<PublicKey, Error> {
        self.generate_key(name, KeyType::Ecdsa).await
    }

    /// Generate a Secp256k1 Keypair
    /// If `name` is not supplied, the [`crate::PeerId`] will be the used as the name by default
    pub async fn generate_secp256k1(&self, name: Option<&str>) -> Result<PublicKey, Error> {
        self.generate_key(name, KeyType::Secp256k1).await
    }

    /// Generate a [`Keypair`] based on the [`KeyType`] supplied
    /// If `name` is not supplied, the [`crate::PeerId`] will be the used as the name by default
    pub async fn generate_key(
        &self,
        name: Option<&str>,
        key_type: KeyType,
    ) -> Result<PublicKey, Error> {
        let keypair = match key_type {
            KeyType::Ed25519 => Keypair::generate_ed25519(),
            KeyType::Ecdsa => Keypair::generate_ecdsa(),
            KeyType::Secp256k1 => Keypair::generate_secp256k1(),
        };
        let public_key = keypair.public();

        let peer_id = public_key.to_peer_id().to_string();

        let peer_id_str = peer_id.as_str();

        // We could probably wrap this in `Zeroizing` instead until `KeyStore` is refactored to accept `Key`
        let bytes = Key::from(keypair.to_protobuf_encoding()?);

        let name = name.unwrap_or_else(|| peer_id_str);

        self.storage.set(name, bytes.as_ref()).await?;

        Ok(public_key)
    }

    /// Get a [`Keypair`] from the [`Keystore`]
    pub async fn get_keypair(&self, name: &str) -> Result<Keypair, Error> {
        let key = self.storage.get(name).await?;
        let keypair = Keypair::from_protobuf_encoding(key.as_ref())?;
        Ok(keypair)
    }

    /// Rename a key stored in [`Keystore`]
    pub async fn rename(&self, name: &str, new_name: &str) -> Result<(), Error> {
        self.storage.rename(name, new_name).await
    }

    /// Check to determine if a the [`Keystore`] contains a key
    pub async fn contains(&self, name: &str) -> Result<bool, Error> {
        self.storage.contains(name).await
    }
}

#[async_trait::async_trait]
pub trait KeyStorage: Sync + Send + 'static {
    async fn set(&self, name: &str, key: &[u8]) -> Result<(), Error>;
    async fn get(&self, name: &str) -> Result<Key, Error>;
    async fn contains(&self, name: &str) -> Result<bool, Error>;
    async fn remove(&self, name: &str) -> Result<(), Error>;
    async fn rename(&self, name: &str, new_name: &str) -> Result<(), Error>;
    async fn list(&self) -> Result<BoxStream<'static, Key>, Error>;
    async fn len(&self) -> Result<usize, Error> {
        let amount = self.list().await?.count().await;
        Ok(amount)
    }
}

#[derive(Default)]
pub struct MemoryKeyStorage {
    inner: Mutex<BTreeMap<String, Key>>,
}

#[async_trait::async_trait]
impl KeyStorage for MemoryKeyStorage {
    async fn set(&self, name: &str, key: &[u8]) -> Result<(), Error> {
        let mut inner = self.inner.lock().await;
        match inner.entry(name.into()) {
            Entry::Occupied(mut entry) => {
                let key_entry = entry.get_mut();
                *key_entry = key.into();
            }
            Entry::Vacant(entry) => {
                entry.insert(key.into());
            }
        };

        Ok(())
    }
    async fn get(&self, name: &str) -> Result<Key, Error> {
        let inner = self.inner.lock().await;
        inner
            .get(name)
            .cloned()
            .ok_or(anyhow::anyhow!("Key doesnt exist"))
    }
    async fn remove(&self, name: &str) -> Result<(), Error> {
        let mut inner = self.inner.lock().await;
        inner
            .remove(name)
            .map(|_| ())
            .ok_or(anyhow::anyhow!("Key doesnt exist"))
    }

    async fn contains(&self, name: &str) -> Result<bool, Error> {
        let inner = self.inner.lock().await;
        Ok(inner.contains_key(name))
    }

    async fn rename(&self, name: &str, new_name: &str) -> Result<(), Error> {
        let mut inner = self.inner.lock().await;
        if inner.contains_key(new_name) {
            anyhow::bail!("{new_name} exist");
        }

        let key = inner
            .remove(name)
            .ok_or(anyhow::anyhow!("Key doesnt exist"))?;
        inner.insert(new_name.into(), key);
        Ok(())
    }

    async fn list(&self) -> Result<BoxStream<'static, Key>, Error> {
        let inner = self.inner.lock().await.clone();
        let stream = async_stream::stream! {
            for (_, key) in inner {
                yield key;
            }
        };

        Ok(stream.boxed())
    }
}

#[cfg(test)]
mod test {
    use crate::keystore::Keystore;

    #[tokio::test]
    async fn keystore_with_peerid() -> anyhow::Result<()> {
        let keystore = Keystore::in_memory();
        let pkey = keystore.generate_ed25519(None).await?;

        let peer_id = pkey.to_peer_id().to_string();

        let key = keystore.get_keypair(&peer_id).await?;

        assert_eq!(key.public(), pkey);
        Ok(())
    }

    #[tokio::test]
    async fn keystore_with_name() -> anyhow::Result<()> {
        let keystore = Keystore::in_memory();
        let pkey = keystore.generate_ed25519(Some("primary")).await?;

        let key = keystore.get_keypair("primary").await?;

        assert_eq!(key.public(), pkey);
        Ok(())
    }

    #[tokio::test]
    async fn keystore_key_exist() -> anyhow::Result<()> {
        let keystore = Keystore::in_memory();
        keystore.generate_ed25519(Some("primary")).await?;
        let result = keystore.generate_ed25519(Some("primary")).await;
        assert!(result.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn keystore_multiple_keys() -> anyhow::Result<()> {
        let keystore = Keystore::in_memory();
        let pkey1 = keystore.generate_ed25519(None).await?;
        let pkey2 = keystore.generate_ed25519(None).await?;

        let peer_id1 = pkey1.to_peer_id().to_string();
        let peer_id2 = pkey2.to_peer_id().to_string();

        let key1 = keystore.get_keypair(&peer_id1).await?;
        let key2 = keystore.get_keypair(&peer_id2).await?;

        assert_ne!(key1.public(), key2.public());
        Ok(())
    }

    #[tokio::test]
    async fn keystore_rename() -> anyhow::Result<()> {
        let keystore = Keystore::in_memory();
        let pkey = keystore.generate_ed25519(None).await?;

        let peer_id = pkey.to_peer_id().to_string();

        keystore.rename(&peer_id, "primary").await?;

        let result = keystore.get_keypair(&peer_id).await;
        assert!(result.is_err());

        let key = keystore.get_keypair("primary").await?;

        assert_eq!(key.public(), pkey);

        Ok(())
    }
}
