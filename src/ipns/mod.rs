//! IPNS functionality around [`Ipfs`].

use crate::error::Error;
use crate::p2p::DnsResolver;
use crate::path::{IpfsPath, PathRoot};
use crate::Ipfs;

mod dnslink;

/// IPNS facade around [`Ipns`].
#[derive(Clone, Debug)]
pub struct Ipns {
    ipfs: Ipfs,
    resolver: Option<DnsResolver>,
}

#[derive(Clone, Copy, Debug, Default)]
pub enum IpnsOption {
    Local,
    #[default]
    DHT,
}

impl Ipns {
    pub fn new(ipfs: Ipfs) -> Self {
        Ipns {
            ipfs,
            resolver: None,
        }
    }

    /// Set dns resolver
    pub fn set_resolver(&mut self, resolver: DnsResolver) {
        self.resolver = Some(resolver);
    }

    /// Resolves a ipns path to an ipld path.
    // TODO: Implement ipns pubsub
    // TODO: Maybe implement a check to the dht store itself too?
    pub async fn resolve(&self, path: &IpfsPath) -> Result<IpfsPath, Error> {
        let path = path.to_owned();
        match path.root() {
            PathRoot::Ipld(_) => Ok(path),
            PathRoot::Ipns(peer) => {
                use std::str::FromStr;
                use std::time::Duration;

                use futures::StreamExt;
                use libipld::Cid;
                use libp2p::PeerId;

                let mut path_iter = path.iter();

                let hash: libipld::multihash::Multihash =
                    libipld::multihash::Multihash::from_bytes(&peer.to_bytes())?;

                let cid = Cid::new_v1(0x72, hash);

                let mb = format!(
                    "/ipns/{}",
                    cid.to_string_of_base(libipld::multibase::Base::Base36Lower)?
                );

                //TODO: Determine if we want to encode the cid of the multihash in base32 or if we can just use the peer id instead
                // let mb = format!("/ipns/{}", peer);

                let repo = self.ipfs.repo();
                let datastore = repo.data_store();

                if let Ok(Some(data)) = datastore.get(mb.as_bytes()).await {
                    if let Ok(path) = rust_ipns::Record::decode(data).and_then(|record| {
                        //Although stored locally, we should verify the record anyway
                        record.verify(*peer)?;
                        let data = record.data()?;
                        let path = String::from_utf8_lossy(data.value());
                        IpfsPath::from_str(&path)
                            .and_then(|mut internal_path| {
                                internal_path.path.push_split(path_iter.by_ref()).map_err(
                                    |_| crate::path::IpfsPathError::InvalidPath(path.to_string()),
                                )?;
                                Ok(internal_path)
                            })
                            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
                    }) {
                        return Ok(path);
                    }
                }

                let stream = self.ipfs.dht_get(mb).await?;

                //TODO: Implement configurable timeout
                let mut records = tokio::time::timeout(
                    Duration::from_secs(60 * 2),
                    stream
                        .filter_map(|record| async move {
                            let key = &record.key.as_ref()[6..];
                            let record = rust_ipns::Record::decode(&record.value).ok()?;
                            let peer_id = PeerId::from_bytes(key).ok()?;
                            record.verify(peer_id).ok()?;
                            Some(record)
                        })
                        .collect::<Vec<_>>(),
                )
                .await
                .unwrap_or_default();

                if records.is_empty() {
                    anyhow::bail!("No records found")
                }

                records.sort_by_key(|record| record.sequence());

                let record = records.last().ok_or(anyhow::anyhow!("No records found"))?;

                let data = record.data()?;

                let path = String::from_utf8_lossy(data.value()).to_string();

                IpfsPath::from_str(&path)
                    .map_err(anyhow::Error::from)
                    .and_then(|mut internal_path| {
                        internal_path
                            .path
                            .push_split(path_iter)
                            .map_err(|_| crate::path::IpfsPathError::InvalidPath(path))?;
                        Ok(internal_path)
                    })
            }
            PathRoot::Dns(domain) => {
                let path_iter = path.iter();
                Ok(dnslink::resolve(self.resolver.unwrap_or_default(), domain, path_iter).await?)
            }
        }
    }

    pub async fn publish(
        &self,
        key: Option<&str>,
        path: &IpfsPath,
        option: Option<IpnsOption>,
    ) -> Result<IpfsPath, Error> {
        use libipld::Cid;
        use libp2p::kad::Quorum;
        use std::str::FromStr;

        let keypair = match key {
            Some(key) => self.ipfs.keystore().get_keypair(key).await?,
            None => self.ipfs.keypair()?.clone(),
        };

        let peer_id = keypair.public().to_peer_id();

        let hash: libipld::multihash::Multihash =
            libipld::multihash::Multihash::from_bytes(&peer_id.to_bytes())?;

        let cid = Cid::new_v1(0x72, hash);

        let mb = format!(
            "/ipns/{}",
            cid.to_string_of_base(libipld::multibase::Base::Base36Lower)?
        );

        let repo = self.ipfs.repo();

        let datastore = repo.data_store();

        let record_data = datastore.get(mb.as_bytes()).await?;

        let mut seq = 0;

        if let Some(record) = record_data.as_ref() {
            let record = rust_ipns::Record::decode(record)?;
            //Although stored locally, we should verify the record anyway
            record.verify(peer_id)?;

            let data = record.data()?;

            let ipfs_path = IpfsPath::from_str(&String::from_utf8_lossy(data.value()))?;

            if ipfs_path.eq(path) {
                return IpfsPath::from_str(&mb);
            }

            // inc req of the record
            seq = record.sequence() + 1;
        }

        let path_bytes = path.to_string();

        let record = rust_ipns::Record::new(
            &keypair,
            path_bytes.as_bytes(),
            chrono::Duration::hours(48),
            seq,
            60000,
        )?;

        let bytes = record.encode()?;

        datastore.put(mb.as_bytes(), &bytes).await?;

        match option.unwrap_or_default() {
            IpnsOption::DHT => self.ipfs.dht_put(&mb, bytes, Quorum::One).await?,
            IpnsOption::Local => {}
        };

        IpfsPath::from_str(&mb)
    }
}
