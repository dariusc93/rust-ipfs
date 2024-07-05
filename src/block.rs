use bytes::Bytes;
use core::hash::{Hash, Hasher};
use ipld_core::cid::Cid;
use ipld_core::codec::Codec;
use ipld_core::ipld::Ipld;
use ipld_core::serde::to_ipld;
use multihash_codetable::Code;
use multihash_derive::MultihashDigest;
use std::fmt::{Debug, Formatter};

/// Container housing the cid and bytes of data
#[derive(Clone)]
pub struct Block {
    cid: Cid,
    data: Bytes,
}
impl Block {
    pub fn new<D: Into<Bytes>>(cid: Cid, data: D) -> std::io::Result<Self> {
        let block = Self::new_unchecked(cid, data);
        block.verify()?;
        Ok(block)
    }

    pub fn new_unchecked<D: Into<Bytes>>(cid: Cid, data: D) -> Self {
        let data = data.into();
        Self { cid, data }
    }

    pub fn cid(&self) -> &Cid {
        &self.cid
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }

    pub fn into_inner(self) -> (Cid, Bytes) {
        (self.cid, self.data)
    }

    pub fn verify(&self) -> std::io::Result<()> {
        let hash = Code::try_from(self.cid.hash().code())
            .map_err(std::io::Error::other)?
            .digest(&self.data);

        if hash.digest() != self.cid.hash().digest() {
            return Err(std::io::ErrorKind::InvalidData.into());
        }

        Ok(())
    }

    pub fn to_ipld(&self) -> std::io::Result<Ipld> {
        let codec = BlockCodec::try_from(self.cid.codec())?;
        let ipld = match codec {
            BlockCodec::Raw => to_ipld(&self.data).map_err(std::io::Error::other)?,
            BlockCodec::DagCbor => {
                serde_ipld_dagcbor::codec::DagCborCodec::decode_from_slice(&self.data)
                    .map_err(std::io::Error::other)?
            }
            BlockCodec::DagJson => {
                serde_ipld_dagjson::codec::DagJsonCodec::decode_from_slice(&self.data)
                    .map_err(std::io::Error::other)?
            }
            BlockCodec::DagPb => ipld_dagpb::to_ipld(&self.data).map_err(std::io::Error::other)?,
        };
        Ok(ipld)
    }

    pub fn references<E: Extend<Cid>>(&self, set: &mut E) -> std::io::Result<()> {
        let ipld = self.to_ipld()?;
        ipld.references(set);
        Ok(())
    }
}

impl Debug for Block {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Block")
            .field("cid", &self.cid)
            .field("data", &format!("{} bytes", self.data.len()))
            .finish()
    }
}

impl Hash for Block {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Hash::hash(&self.cid, state)
    }
}

impl PartialEq for Block {
    fn eq(&self, other: &Self) -> bool {
        self.cid.eq(&other.cid)
    }
}

impl Eq for Block {}

impl AsRef<[u8]> for Block {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

/// Backwards compatible
pub type IpldCodec = BlockCodec;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlockCodec {
    /// Raw codec.
    Raw,
    /// Cbor codec.
    DagCbor,
    /// Json codec.
    DagJson,
    /// Protobuf codec.
    DagPb,
}

impl From<BlockCodec> for u64 {
    fn from(mc: BlockCodec) -> Self {
        match mc {
            BlockCodec::Raw => 0x55,
            BlockCodec::DagCbor => 0x71,
            BlockCodec::DagJson => 0x0129,
            BlockCodec::DagPb => 0x70,
        }
    }
}

impl TryFrom<u64> for BlockCodec {
    type Error = std::io::Error;

    fn try_from(codec: u64) -> Result<Self, Self::Error> {
        let codec = match codec {
            0x55 => Self::Raw,
            0x71 => Self::DagCbor,
            0x0129 => Self::DagJson,
            0x70 => Self::DagPb,
            _ => return Err(std::io::ErrorKind::Unsupported.into()),
        };
        Ok(codec)
    }
}
