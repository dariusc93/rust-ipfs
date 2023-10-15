use std::ops::Add;

use chrono::DateTime;
use chrono::Duration;
use chrono::FixedOffset;
use chrono::SecondsFormat;
use chrono::Utc;
use cid::Cid;
use libipld::cbor::DagCborCodec;
use libipld::ipld;
use libipld::prelude::Codec;
use libipld::DagCbor;
use libipld::Ipld;
use libp2p::identity::Keypair;
use libp2p::identity::PublicKey;
use libp2p::PeerId;
use quick_protobuf::MessageWrite;
use quick_protobuf::Writer;
use quick_protobuf::{BytesReader, MessageRead};
use serde::{Deserialize, Serialize};

mod generate;

#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    DagCbor,
    derive_more::Display,
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

#[cfg(feature = "libp2p")]
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

impl From<&Record> for generate::ipns_pb::IpnsEntry<'_> {
    fn from(record: &Record) -> Self {
        generate::ipns_pb::IpnsEntry {
            validity: record.validity.clone().into(),
            validityType: generate::ipns_pb::mod_IpnsEntry::ValidityType::EOL,
            value: record.value.clone().into(),
            signatureV1: record.signature_v1.clone().into(),
            signatureV2: record.signature_v2.clone().into(),
            sequence: record.sequence,
            pubKey: record.public_key.clone().into(),
            ttl: record.ttl,
            data: record.data.clone().into(),
        }
    }
}

#[derive(Debug, Clone, DagCbor)]
pub struct Data {
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

impl Data {
    pub fn value(&self) -> &[u8] {
        &self.value
    }

    pub fn validity_type(&self) -> ValidityType {
        self.validity_type
    }

    pub fn validity(&self) -> &[u8] {
        &self.validity
    }

    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    pub fn ttl(&self) -> u64 {
        self.ttl
    }
}

impl Record {
    #[cfg(feature = "libp2p")]
    pub fn new(
        keypair: &Keypair,
        value: impl AsRef<[u8]>,
        duration: Duration,
        seq: u64,
        ttl: u64,
    ) -> std::io::Result<Self> {
        let value = value.as_ref().to_vec();

        let validity = Utc::now()
            .add(duration)
            .to_rfc3339_opts(SecondsFormat::Nanos, false)
            .into_bytes();

        let validity_type = ValidityType::EOL;

        let signature_v1_construct = {
            let mut data = Vec::with_capacity(value.len() + validity.len() + 3);

            data.extend(value.iter());
            data.extend(validity.iter());
            data.extend(validity_type.to_string().as_bytes());

            data
        };

        let signature_v1 = keypair
            .sign(&signature_v1_construct)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        let document = ipld!({
            "Value": value.clone(),
            "ValidityType": i32::from(validity_type),
            "Validity": validity.clone(),
            "Sequence": seq,
            "TTL": ttl
        });

        let data = DagCborCodec
            .encode(&document)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        let mut signature_v2_construct = vec![
            0x69, 0x70, 0x6e, 0x73, 0x2d, 0x73, 0x69, 0x67, 0x6e, 0x61, 0x74, 0x75, 0x72, 0x65,
            0x3a,
        ];

        signature_v2_construct.extend(data.iter());

        let signature_v2 = keypair
            .sign(&signature_v2_construct)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        let public_key = match keypair.key_type().into() {
            KeyType::RSA => keypair
                .to_protobuf_encoding()
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?,
            _ => vec![],
        };

        Ok(Record {
            data,
            value,
            validity_type,
            validity,
            sequence: seq,
            ttl,
            public_key,
            signature_v1,
            signature_v2,
        })
    }

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

    pub fn encode(&self) -> std::io::Result<Vec<u8>> {
        let entry: generate::ipns_pb::IpnsEntry = self.into();

        let mut buf = Vec::with_capacity(entry.get_size());
        let mut writer = Writer::new(&mut buf);

        entry
            .write_message(&mut writer)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        Ok(buf)
    }
}

impl Record {
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    pub fn validity_type(&self) -> ValidityType {
        self.validity_type
    }

    pub fn validity(&self) -> std::io::Result<DateTime<FixedOffset>> {
        let time = String::from_utf8_lossy(&self.validity);
        chrono::DateTime::parse_from_rfc3339(&time)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    }

    pub fn ttl(&self) -> u64 {
        self.ttl
    }

    pub fn signature_v1(&self) -> bool {
        !self.signature_v1.is_empty()
    }

    pub fn signature_v2(&self) -> bool {
        !self.signature_v2.is_empty()
    }

    pub fn data(&self) -> std::io::Result<Data> {
        //Note: Because of the cbor4ii crate giving errors without exact reason, we will convert it into an ipld document instead
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

        let data = Data {
            value: ipld_value,
            validity_type: ipld_validity_type,
            validity: ipld_validity,
            sequence: ipld_sequence,
            ttl: ipld_ttl,
        };

        if data.value != self.value
            || data.validity != self.validity
            || data.validity_type != self.validity_type
            || data.sequence != self.sequence
            || data.ttl != self.ttl
        {
            return Err(std::io::Error::from(std::io::ErrorKind::InvalidData));
        }

        Ok(data)
    }

    pub fn value(&self) -> std::io::Result<Cid> {
        let cid_str = String::from_utf8_lossy(&self.value);
        Cid::try_from(cid_str.as_ref())
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    }

    #[cfg(feature = "libp2p")]
    pub fn verify(&self, peer_id: PeerId) -> std::io::Result<()> {
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

        let key = peer_id.to_bytes();

        let mh = libipld::Multihash::from_bytes(&key).expect("msg");
        let cid = libipld::Cid::new_v1(0x72, mh);

        let public_key = match self.public_key.is_empty() {
            true => cid.hash().digest(),
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

        self.data()?;

        if !pk.verify(&signature_v2, &self.signature_v2) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Signature is invalid",
            ));
        }

        Ok(())
    }
}
