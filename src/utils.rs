use libipld::{Cid, multihash::{Code, MultihashDigest}, IpldCodec};

pub fn ns_to_cid(string: &str) -> Cid {
    let hash = Code::Sha2_256.digest(string.as_bytes());
    Cid::new_v1(IpldCodec::Raw.into(), hash)
}

pub fn cid_to_ns(cid: Cid) -> String {
    format!("/provider/{cid}")
}