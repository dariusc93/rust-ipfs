//! IPNS functionality around [`Ipfs`].

use crate::error::Error;
use crate::p2p::DnsResolver;
use crate::path::{IpfsPath, PathRoot};
use crate::Ipfs;

mod dnslink;

/// IPNS facade around [`Ipns`].
#[derive(Clone, Debug)]
#[cfg_attr(not(feature = "experimental"), allow(dead_code))]
pub struct Ipns {
    ipfs: Ipfs,
}

impl Ipns {
    pub fn new(ipfs: Ipfs) -> Self {
        Ipns { ipfs }
    }

    /// Resolves a ipns path to an ipld path.
    // TODO: Implement a local store check and eventually pubsub
    pub async fn resolve(&self, resolver: DnsResolver, path: &IpfsPath) -> Result<IpfsPath, Error> {
        let path = path.to_owned();
        match path.root() {
            PathRoot::Ipld(_) => Ok(path),
            #[cfg(feature = "experimental")]
            PathRoot::Ipns(peer) => {
                use std::str::FromStr;
                use std::time::Duration;

                use futures::StreamExt;
                use libp2p::PeerId;
                // let hash: libipld::multihash::Multihash =
                //     libipld::multihash::Multihash::from_bytes(&peer.to_bytes())?;

                // let cid = Cid::new_v1(0x72, hash);

                // let mb = format!("/ipns/{}", cid.to_string_of_base(libipld::multibase::Base::Base36Lower)?);

                //TODO: Determine if we want to encode the cid of the multihash in base32 or if we can just use the peer id instead
                let mb = format!("/ipns/{}", peer);

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

                IpfsPath::from_str(&path).map_err(anyhow::Error::from)
            }
            #[cfg(not(feature = "experimental"))]
            PathRoot::Ipns(_) => Err(anyhow::anyhow!("unimplemented")),
            PathRoot::Dns(domain) => Ok(dnslink::resolve(resolver, domain).await?),
        }
    }
}
