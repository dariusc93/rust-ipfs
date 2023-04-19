//! IPNS functionality around [`Ipfs`].

use crate::error::Error;
use crate::p2p::DnsResolver;
use crate::path::{IpfsPath, PathRoot};
use crate::Ipfs;

mod dnslink;
// pub mod ipns_pb {
//     include!(concat!(env!("OUT_DIR"), "/ipns_pb.rs"));
// }

/// IPNS facade around [`Ipns`].
#[derive(Clone, Debug)]
pub struct Ipns {
    // FIXME(unused): scaffolding while ipns functionality as a whole suggests we should have dht
    // queries etc. here (currently unimplemented).
    _ipfs: Ipfs,
}

impl Ipns {
    pub fn new(_ipfs: Ipfs) -> Self {
        Ipns { _ipfs }
    }

    /// Resolves a ipns path to an ipld path.
    pub async fn resolve(&self, resolver: DnsResolver, path: &IpfsPath) -> Result<IpfsPath, Error> {
        let path = path.to_owned();
        match path.root() {
            PathRoot::Ipld(_) => Ok(path),
            PathRoot::Ipns(_) => Err(anyhow::anyhow!("unimplemented")),
            PathRoot::Dns(domain) => Ok(dnslink::resolve(resolver, domain).await?),
        }
    }
}
