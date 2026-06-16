use bytes::Bytes;
use chrono::DateTime;
use chrono::Duration;
use chrono::FixedOffset;
use chrono::SecondsFormat;
use chrono::Utc;
use cid::Cid;
use libp2p_identity::Keypair;
use libp2p_identity::PeerId;
use libp2p_identity::PublicKey;
use quick_protobuf::MessageWrite;
use quick_protobuf::Writer;
use quick_protobuf::{BytesReader, MessageRead};
use serde::{Deserialize, Serialize, Serializer};
use std::ops::Add;

mod generate;

const SIGNATURE_V2_BASE: &[u8] = &[
    0x69, 0x70, 0x6e, 0x73, 0x2d, 0x73, 0x69, 0x67, 0x6e, 0x61, 0x74, 0x75, 0x72, 0x65, 0x3a,
];

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(i32)]
pub enum ValidityType {
    EOL = 0,
}

impl std::fmt::Display for ValidityType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "EOL")
    }
}

impl Serialize for ValidityType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_i32(*self as i32)
    }
}

impl<'de> Deserialize<'de> for ValidityType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let i = i32::deserialize(deserializer)?;
        ValidityType::try_from(i).map_err(serde::de::Error::custom)
    }
}

impl TryFrom<i32> for ValidityType {
    type Error = std::io::Error;
    fn try_from(i: i32) -> Result<Self, Self::Error> {
        match i {
            0 => Ok(ValidityType::EOL),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid validity type",
            )),
        }
    }
}

impl From<ValidityType> for i32 {
    fn from(ty: ValidityType) -> Self {
        ty as i32
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
impl From<libp2p_identity::KeyType> for KeyType {
    fn from(ty: libp2p_identity::KeyType) -> Self {
        match ty {
            libp2p_identity::KeyType::Ed25519 => KeyType::Ed25519,
            libp2p_identity::KeyType::RSA => KeyType::RSA,
            libp2p_identity::KeyType::Secp256k1 => KeyType::Secp256k1,
            libp2p_identity::KeyType::Ecdsa => KeyType::ECDSA,
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

impl<'a> From<&'a Record> for generate::ipns_pb::IpnsEntry<'a> {
    fn from(record: &'a Record) -> Self {
        generate::ipns_pb::IpnsEntry {
            validity: (&record.validity).into(),
            validityType: generate::ipns_pb::mod_IpnsEntry::ValidityType::EOL,
            value: (&record.value).into(),
            signatureV1: (&record.signature_v1).into(),
            signatureV2: (&record.signature_v2).into(),
            sequence: record.sequence,
            pubKey: (&record.public_key).into(),
            ttl: record.ttl,
            data: (&record.data).into(),
        }
    }
}

// Fields of the Bytes type are used here instead of Vec<u8> to ensure that
// these fields are (de)serialized into "byte string" CBOR values instead of simple arrays.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Data {
    #[serde(rename = "Value")]
    pub value: Bytes,

    #[serde(rename = "ValidityType")]
    pub validity_type: ValidityType,

    #[serde(rename = "Validity")]
    pub validity: Bytes,

    #[serde(rename = "Sequence")]
    pub sequence: u64,

    #[serde(rename = "TTL")]
    pub ttl: u64,
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
            .to_rfc3339_opts(SecondsFormat::Nanos, true)
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

        let document = Data {
            value: Bytes::from(value.clone()),
            validity_type,
            validity: Bytes::from(validity.clone()),
            sequence: seq,
            ttl,
        };

        let data = serde_ipld_dagcbor::to_vec(&document)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        let signature_v2_construct = SIGNATURE_V2_BASE
            .iter()
            .chain(data.iter())
            .copied()
            .collect::<Vec<_>>();

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
        let data: Data = serde_ipld_dagcbor::from_slice(&self.data)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

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
    pub fn verify_signature(&self, peer_id: PeerId) -> std::io::Result<()> {
        use multihash::Multihash;

        if self.signature_v2.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "missing signatureV2",
            ));
        }

        if self.data.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Empty data field",
            ));
        }

        let public_key = if self.public_key.is_empty() {
            let mh = Multihash::<64>::from_bytes(&peer_id.to_bytes())
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            // small keys are inlined in the name via an identity (code 0) multihash; anything
            // else (e.g. an RSA name) carries no inlined key, so the record must embed one.
            if mh.code() != 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "record omits pubKey but the IPNS name does not inline one",
                ));
            }
            PublicKey::try_decode_protobuf(mh.digest())
        } else {
            PublicKey::try_decode_protobuf(&self.public_key)
        }
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        if PeerId::from_public_key(&public_key) != peer_id {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "public key does not match the IPNS name",
            ));
        }

        self.data()?;

        let signature_v2 = SIGNATURE_V2_BASE
            .iter()
            .chain(self.data.iter())
            .copied()
            .collect::<Vec<_>>();

        if !public_key.verify(&signature_v2, &self.signature_v2) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Signature is invalid",
            ));
        }

        Ok(())
    }

    /// Fully validates the record against `peer_id`: name binding, V2 signature, and that the EOL
    /// validity has not elapsed.
    #[cfg(feature = "libp2p")]
    pub fn verify(&self, peer_id: PeerId) -> std::io::Result<()> {
        self.verify_signature(peer_id)?;

        if self.validity()?.with_timezone(&Utc) < Utc::now() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "record has expired",
            ));
        }

        Ok(())
    }
}

#[cfg(all(test, feature = "libp2p"))]
mod tests {
    use super::*;

    fn record_for(kp: &Keypair, hours: i64) -> Record {
        Record::new(kp, b"/ipfs/bafkqaaa", Duration::hours(hours), 0, 0).unwrap()
    }

    #[test]
    fn valid_record_roundtrips_and_verifies() {
        let kp = Keypair::generate_ed25519();
        let peer = PeerId::from_public_key(&kp.public());
        let rec = record_for(&kp, 24);
        rec.verify(peer).unwrap();

        let decoded = Record::decode(rec.encode().unwrap()).unwrap();
        decoded.verify(peer).unwrap();
    }

    #[test]
    fn verify_rejects_expired_record_but_signature_still_checks() {
        let kp = Keypair::generate_ed25519();
        let peer = PeerId::from_public_key(&kp.public());
        let rec = record_for(&kp, -1);
        rec.verify_signature(peer).unwrap();
        assert!(rec.verify(peer).is_err());
    }

    #[test]
    fn embedded_pubkey_must_match_the_name() {
        let attacker = Keypair::generate_ed25519();
        let victim = Keypair::generate_ed25519();
        let attacker_peer = PeerId::from_public_key(&attacker.public());
        let victim_peer = PeerId::from_public_key(&victim.public());

        // a genuine attacker record, with the attacker's pubKey spliced into the protobuf
        // (field 7, tag 0x3a) to mimic a record that carries an embedded key.
        let mut bytes = record_for(&attacker, 24).encode().unwrap();
        let pk = attacker.public().encode_protobuf();
        bytes.push(0x3a);
        bytes.push(pk.len() as u8); // an ed25519 protobuf key is < 128 bytes
        bytes.extend_from_slice(&pk);

        let tampered = Record::decode(&bytes).unwrap();
        tampered.verify_signature(attacker_peer).unwrap();
        // the embedded key does not hash to the victim's name, so it must not validate for it.
        assert!(tampered.verify_signature(victim_peer).is_err());
    }
}
