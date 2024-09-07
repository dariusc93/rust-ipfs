//! IPNS functionality around [`Ipfs`].

use futures_timeout::TimeoutExt;
use std::borrow::Borrow;

use crate::p2p::DnsResolver;
use crate::path::{IpfsPath, PathRoot};
use crate::Ipfs;

mod dnslink;

/// IPNS facade around [`Ipns`].
#[derive(Clone, Debug)]
pub struct Ipns {
    ipfs: Ipfs,
    resolver: DnsResolver,
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
            resolver: DnsResolver::default(),
        }
    }

    /// Set dns resolver
    pub fn set_resolver(&mut self, resolver: DnsResolver) {
        self.resolver = resolver;
    }

    /// Resolves a ipns path to an ipld path.
    // TODO: Implement ipns pubsub
    // TODO: Maybe implement a check to the dht store itself too?
    pub async fn resolve<B: Borrow<IpfsPath>>(&self, path: B) -> Result<IpfsPath, IpnsError> {
        let path = path.borrow();
        match path.root() {
            PathRoot::Ipld(_) => Ok(path.clone()),
            PathRoot::Ipns(peer) => {
                use std::str::FromStr;
                use std::time::Duration;

                use futures::StreamExt;
                use ipld_core::cid::Cid;
                use libp2p::PeerId;
                use multihash::Multihash;

                let mut path_iter = path.iter();

                let hash = Multihash::from_bytes(&peer.to_bytes()).map_err(anyhow::Error::from)?;

                let cid = Cid::new_v1(0x72, hash);

                let mb = format!(
                    "/ipns/{}",
                    cid.to_string_of_base(multibase::Base::Base36Lower)
                        .map_err(anyhow::Error::from)?
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
                let mut records = stream
                    .filter_map(|record| async move {
                        let key = &record.key.as_ref()[6..];
                        let record = rust_ipns::Record::decode(&record.value).ok()?;
                        let peer_id = PeerId::from_bytes(key).ok()?;
                        record.verify(peer_id).ok()?;
                        Some(record)
                    })
                    .collect::<Vec<_>>()
                    .timeout(Duration::from_secs(60 * 2))
                    .await
                    .unwrap_or_default();

                if records.is_empty() {
                    return Err(anyhow::anyhow!("No records found").into());
                }

                records.sort_by_key(|record| record.sequence());

                let record = records.last().ok_or(anyhow::anyhow!("No records found"))?;

                let data = record.data()?;

                let path = String::from_utf8_lossy(data.value()).to_string();

                IpfsPath::from_str(&path)
                    .map_err(IpnsError::from)
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
                dnslink::resolve(self.resolver, domain, path_iter)
                    .await
                    .map_err(IpnsError::from)
            }
        }
    }

    pub async fn publish<B: Borrow<IpfsPath>>(
        &self,
        key: Option<&str>,
        path: B,
        option: IpnsOption,
    ) -> Result<IpfsPath, IpnsError> {
        use ipld_core::cid::Cid;
        use libp2p::kad::Quorum;
        use multihash::Multihash;
        use std::str::FromStr;

        let path = path.borrow();

        let keypair = match key {
            Some(key) => self.ipfs.keystore().get_keypair(key).await?,
            None => self.ipfs.keypair().clone(),
        };

        let peer_id = keypair.public().to_peer_id();

        let hash = Multihash::from_bytes(&peer_id.to_bytes()).map_err(anyhow::Error::from)?;

        let cid = Cid::new_v1(0x72, hash);

        let mb = format!(
            "/ipns/{}",
            cid.to_string_of_base(multibase::Base::Base36Lower)
                .map_err(anyhow::Error::from)?
        );

        let repo = self.ipfs.repo();

        let datastore = repo.data_store();

        let record_data = datastore.get(mb.as_bytes()).await.unwrap_or_default();

        let mut seq = 0;

        if let Some(record) = record_data.as_ref() {
            let record = rust_ipns::Record::decode(record)?;
            //Although stored locally, we should verify the record anyway
            record.verify(peer_id)?;

            let data = record.data()?;

            let ipfs_path = IpfsPath::from_str(&String::from_utf8_lossy(data.value()))?;

            if ipfs_path.eq(path) {
                return IpfsPath::from_str(&mb).map_err(IpnsError::from);
            }

            // inc req of the record
            seq = record.sequence() + 1;
        }

        let path_bytes = path.to_string();

        let record = rust_ipns::Record::new(
            &keypair,
            path_bytes.as_bytes(),
            chrono::Duration::try_hours(48).expect("shouldnt panic"),
            seq,
            60000,
        )?;

        let bytes = record.encode()?;

        datastore.put(mb.as_bytes(), &bytes).await?;

        match option {
            IpnsOption::DHT => self.ipfs.dht_put(&mb, bytes, Quorum::One).await?,
            IpnsOption::Local => {}
        };

        IpfsPath::from_str(&mb).map_err(IpnsError::from)
    }
}

#[non_exhaustive]
#[derive(thiserror::Error, Debug)]
pub enum IpnsError {
    #[error(transparent)]
    IpfsPath(#[from] crate::path::IpfsPathError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Any(#[from] anyhow::Error),
}
