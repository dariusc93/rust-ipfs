use chrono::DateTime;
use chrono::Utc;
use cid::Cid;
use libipld::cbor::DagCborCodec;
use libipld::DagCbor;
use libipld::Ipld;
use libp2p::identity::PublicKey;
use quick_protobuf::{BytesReader, MessageRead};
use serde::{Deserialize, Serialize};

mod generate;

#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, DagCbor,
)]
#[repr(i32)]
pub enum ValidityType {
    EOL = 0,
}

impl From<ValidityType> for i32 {
    fn from(ty: ValidityType) -> Self {
        match ty {
            ValidityType::EOL => 0,
        }
    }
}

impl From<generate::ipns_pb::mod_IpnsEntry::ValidityType> for ValidityType {
    fn from(v_ty: generate::ipns_pb::mod_IpnsEntry::ValidityType) -> Self {
        match v_ty {
            generate::ipns_pb::mod_IpnsEntry::ValidityType::EOL => ValidityType::EOL,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(i32)]
pub enum KeyType {
    RSA = 0,
    Ed25519 = 1,
    Secp256k1 = 2,
    ECDSA = 3,
}

impl From<libp2p::identity::KeyType> for KeyType {
    fn from(ty: libp2p::identity::KeyType) -> Self {
        match ty {
            libp2p::identity::KeyType::Ed25519 => KeyType::Ed25519,
            libp2p::identity::KeyType::RSA => KeyType::RSA,
            libp2p::identity::KeyType::Secp256k1 => KeyType::Secp256k1,
            libp2p::identity::KeyType::Ecdsa => KeyType::ECDSA,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Record {
    data: Vec<u8>,

    value: Vec<u8>,
    validity_type: ValidityType,
    validity: Vec<u8>,
    sequence: u64,
    ttl: u64,

    public_key: Vec<u8>,

    signature_v1: Vec<u8>,
    signature_v2: Vec<u8>,
}

impl From<generate::ipns_pb::IpnsEntry<'_>> for Record {
    fn from(entry: generate::ipns_pb::IpnsEntry<'_>) -> Self {
        Record {
            data: entry.data.into(),
            value: entry.value.into(),
            validity_type: entry.validityType.into(),
            validity: entry.validity.into(),
            sequence: entry.sequence,
            ttl: entry.ttl,
            public_key: entry.pubKey.into(),
            signature_v1: entry.signatureV1.into(),
            signature_v2: entry.signatureV2.into(),
        }
    }
}

#[derive(Debug, Clone, DagCbor)]
pub struct Document {
    #[ipld(rename = "Valid")]
    value: Vec<u8>,

    #[ipld(rename = "ValidityType")]
    validity_type: ValidityType,

    #[ipld(rename = "Validity")]
    validity: Vec<u8>,

    #[ipld(rename = "Sequence")]
    sequence: u64,

    #[ipld(rename = "TTL")]
    ttl: u64,
}

impl Record {
    pub fn decode(data: impl AsRef<[u8]>) -> std::io::Result<Self> {
        let data = data.as_ref();

        if data.len() > 10 * 1024 {
            return Err(std::io::Error::from(std::io::ErrorKind::InvalidData));
        }

        let mut reader = BytesReader::from_bytes(data);
        let entry = generate::ipns_pb::IpnsEntry::from_reader(&mut reader, data)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        let record = entry.into();
        Ok(record)
    }
}

impl Record {
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    pub fn validity_type(&self) -> ValidityType {
        self.validity_type
    }

    pub fn validity(&self) -> DateTime<Utc> {
        // chrono::DateTime::try_from(self.validity.clone()).unwrap()
        unimplemented!()
    }

    pub fn document(&self) -> std::io::Result<Document> {
        use libipld::prelude::Codec;

        //Note: Because of the crate giving errors without exact reason, why have to convert it into an ipld document
        //      and map it to the struct manually for the time being.
        //TODO: Investigate cbor crate and why it would not deserialize the data directly
        let document: Ipld = DagCborCodec
            .decode(&self.data)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        let ipld_value = match document.get("Value") {
            Ok(Ipld::Bytes(bytes)) => bytes.clone(),
            _ => return Err(std::io::Error::from(std::io::ErrorKind::InvalidData)),
        };

        let ipld_validity_type = match document.get("ValidityType") {
            Ok(Ipld::Integer(0)) => ValidityType::EOL,
            _ => return Err(std::io::Error::from(std::io::ErrorKind::InvalidData)),
        };

        let ipld_validity = match document.get("Validity") {
            Ok(Ipld::Bytes(bytes)) => bytes.clone(),
            _ => return Err(std::io::Error::from(std::io::ErrorKind::InvalidData)),
        };

        let ipld_ttl = match document.get("TTL") {
            Ok(Ipld::Integer(int)) => std::cmp::min(*int, i64::MAX as i128) as u64,
            _ => return Err(std::io::Error::from(std::io::ErrorKind::InvalidData)),
        };

        let ipld_sequence = match document.get("Sequence") {
            Ok(Ipld::Integer(int)) => std::cmp::min(*int, i64::MAX as i128) as u64,
            _ => return Err(std::io::Error::from(std::io::ErrorKind::InvalidData)),
        };

        let document = Document {
            value: ipld_value,
            validity_type: ipld_validity_type,
            validity: ipld_validity,
            sequence: ipld_sequence,
            ttl: ipld_ttl,
        };

        if document.value != self.value
            || document.validity != self.validity
            || document.validity_type != self.validity_type
            || document.sequence != self.sequence
            || document.ttl != self.ttl
        {
            return Err(std::io::Error::from(std::io::ErrorKind::InvalidData));
        }

        Ok(document)
    }

    pub fn value(&self) -> std::io::Result<Cid> {
        let cid_str = String::from_utf8_lossy(&self.value);
        Cid::try_from(cid_str.as_ref())
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    }

    pub fn address(&self) -> Option<Cid> {
        None
    }

    pub fn verify(&self, key: Cid) -> std::io::Result<()> {
        if self.signature_v2.is_empty() && self.signature_v1.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Empty signature field",
            ));
        }

        if self.data.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Empty data field",
            ));
        }

        let public_key = match self.public_key.is_empty() {
            true => key.hash().digest(),
            //TODO: Validate internal public key against the multhash publickey
            false => self.public_key.as_ref(),
        };

        let pk = PublicKey::try_decode_protobuf(public_key)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        

        //TODO: Implement support for RSA
        if matches!(pk.key_type().into(), KeyType::RSA) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "RSA Keys are not supported at this time",
            ));
        }

        let mut signature_v2 = vec![
            0x69, 0x70, 0x6e, 0x73, 0x2d, 0x73, 0x69, 0x67, 0x6e, 0x61, 0x74, 0x75, 0x72, 0x65,
            0x3a,
        ];

        signature_v2.extend(self.data.iter());

        self.document()?;

        if !pk.verify(&signature_v2, &self.signature_v2) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Signature is invalid",
            ));
        }

        Ok(())
    }
}
