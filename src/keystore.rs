use std::{
    collections::{btree_map::Entry, BTreeMap},
    sync::Arc,
};

use anyhow::Error;
use libp2p::identity::{Keypair, PublicKey};
use tokio::sync::Mutex;
use zeroize::Zeroize;

pub enum KeyType {
    Ed25519,
    Ecdsa,
    Secp256k1,
    Rsa,
}

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
    pub fn new(storage: Arc<dyn KeyStorage>) -> Self {
        Self { storage }
    }

    pub fn in_memory() -> Self {
        Self::new(Arc::new(MemoryKeyStorage::default()))
    }

    pub async fn import_key(
        &self,
        keypair: &Keypair,
        name: Option<&str>,
    ) -> Result<PublicKey, Error> {
        let public_key = keypair.public();

        let peer_id = public_key.to_peer_id().to_string();

        let peer_id_str = peer_id.as_str();

        let name = match name {
            Some(name) => name,
            None => peer_id_str,
        };

        let bytes = Key::from(keypair.to_protobuf_encoding()?);

        self.storage.set(name, bytes.as_ref()).await?;

        Ok(public_key)
    }

    pub async fn generate_ed25519(&self, name: Option<&str>) -> Result<PublicKey, Error> {
        self.generate_key(name, KeyType::Ed25519).await
    }

    pub async fn generate_ecdsa(&self, name: Option<&str>) -> Result<PublicKey, Error> {
        self.generate_key(name, KeyType::Ecdsa).await
    }

    pub async fn generate_secp256k1(&self, name: Option<&str>) -> Result<PublicKey, Error> {
        self.generate_key(name, KeyType::Secp256k1).await
    }

    pub async fn generate_key(
        &self,
        name: Option<&str>,
        key_type: KeyType,
    ) -> Result<PublicKey, Error> {
        let keypair = match key_type {
            KeyType::Ed25519 => Keypair::generate_ed25519(),
            _ => anyhow::bail!("unimplemented"),
        };
        let public_key = keypair.public();

        let peer_id = public_key.to_peer_id().to_string();

        let peer_id_str = peer_id.as_str();

        let bytes = Key::from(keypair.to_protobuf_encoding()?);

        let name = match name {
            Some(name) => name,
            None => peer_id_str,
        };

        self.storage.set(name, bytes.as_ref()).await?;

        Ok(public_key)
    }

    pub async fn get_keypair(&self, name: &str) -> Result<Keypair, Error> {
        let key = self.storage.get(name).await?;
        let keypair = Keypair::from_protobuf_encoding(key.as_ref())?;
        Ok(keypair)
    }

    pub async fn rename(&self, name: &str, new_name: &str) -> Result<(), Error> {
        self.storage.rename(name, new_name).await
    }

    pub async fn contains(&self, name: &str) -> Result<bool, Error> {
        self.storage.contains(name).await
    }

    pub async fn get_key_type(&self, _: &str) -> Result<KeyType, Error> {
        anyhow::bail!("unimplemented")
    }
}

#[async_trait::async_trait]
pub trait KeyStorage: Sync + Send + 'static {
    async fn set(&self, name: &str, key: &[u8]) -> Result<(), Error>;
    async fn get(&self, name: &str) -> Result<Key, Error>;
    async fn contains(&self, name: &str) -> Result<bool, Error>;
    async fn remove(&self, name: &str) -> Result<(), Error>;
    async fn rename(&self, name: &str, new_name: &str) -> Result<(), Error>;
}

#[derive(Default)]
pub struct MemoryKeyStorage {
    inner: Mutex<BTreeMap<String, Vec<u8>>>,
}

#[async_trait::async_trait]
impl KeyStorage for MemoryKeyStorage {
    async fn set(&self, name: &str, key: &[u8]) -> Result<(), Error> {
        let mut inner = self.inner.lock().await;
        match inner.entry(name.into()) {
            Entry::Occupied(mut entry) => {
                if !entry.get().is_empty() {
                    anyhow::bail!("Key exist");
                }

                *entry.get_mut() = key.to_vec();
            }
            Entry::Vacant(entry) => {
                entry.insert(key.to_vec());
            }
        };

        Ok(())
    }
    async fn get(&self, name: &str) -> Result<Key, Error> {
        let inner = self.inner.lock().await;
        inner
            .get(name)
            .cloned()
            .map(Key::from)
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
        let key = inner
            .remove(name)
            .ok_or(anyhow::anyhow!("Key doesnt exist"))?;
        inner.insert(new_name.into(), key);
        Ok(())
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
