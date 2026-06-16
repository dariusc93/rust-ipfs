use bytes::Bytes;
use chrono::DateTime;
use chrono::FixedOffset;
use chrono::SecondsFormat;
use chrono::Utc;
use libp2p_identity::PeerId;
use libp2p_identity::PublicKey;
use libp2p_identity::{DecodingError, Keypair, SigningError};
use quick_protobuf::MessageWrite;
use quick_protobuf::Writer;
use quick_protobuf::{BytesReader, MessageRead};
use serde::{Deserialize, Serialize, Serializer};

mod generate;

const SIGNATURE_V2_BASE: &[u8] = &[
    0x69, 0x70, 0x6e, 0x73, 0x2d, 0x73, 0x69, 0x67, 0x6e, 0x61, 0x74, 0x75, 0x72, 0x65, 0x3a,
];

/// libp2p inlines a public key into the PeerID (via an `identity` multihash) when its protobuf
/// encoding is at most this many bytes; larger keys are referenced by a sha2-256 hash instead.
const MAX_INLINE_KEY_LENGTH: usize = 42;

/// Errors produced when creating, decoding, or validating an IPNS [`Record`].
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// The record exceeds the 10 KiB IPNS size limit.
    RecordTooLarge,
    /// The record is missing its V2 signature.
    MissingSignature,
    /// The record is missing its data field.
    EmptyData,
    /// The signing key does not correspond to the IPNS name.
    NameMismatch,
    /// The IPNS name does not inline a public key and the record omits one.
    MissingPublicKey,
    /// The V2 signature failed verification.
    InvalidSignature,
    /// The record's EOL validity has elapsed.
    Expired,
    /// The dag-cbor data does not match the record's protobuf fields.
    DataMismatch,
    /// Unrecognized validity type.
    InvalidValidityType,
    /// Malformed protobuf.
    Protobuf(quick_protobuf::Error),
    /// Malformed dag-cbor data.
    Cbor(Box<dyn std::error::Error + Send + Sync + 'static>),
    /// Malformed EOL validity timestamp.
    InvalidValidity(chrono::ParseError),
    /// A key or signing operation failed.
    SigningError(SigningError),
    /// Invalid public key.
    InvalidPublicKey(DecodingError),
    /// Malformed multihash or peer id.
    Multihash(multihash::Error),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::RecordTooLarge => write!(f, "record exceeds the 10 KiB limit"),
            Error::MissingSignature => write!(f, "record is missing a V2 signature"),
            Error::EmptyData => write!(f, "record is missing its data field"),
            Error::NameMismatch => write!(f, "public key does not match the IPNS name"),
            Error::MissingPublicKey => {
                write!(
                    f,
                    "record omits pubKey but the IPNS name does not inline one"
                )
            }
            Error::InvalidSignature => write!(f, "signature is invalid"),
            Error::Expired => write!(f, "record has expired"),
            Error::DataMismatch => write!(f, "dag-cbor data does not match the protobuf fields"),
            Error::InvalidValidityType => write!(f, "invalid validity type"),
            Error::Protobuf(e) => write!(f, "protobuf error: {e}"),
            Error::Cbor(e) => write!(f, "dag-cbor error: {e}"),
            Error::InvalidValidity(e) => write!(f, "invalid validity timestamp: {e}"),
            Error::SigningError(e) => write!(f, "signing error: {e}"),
            Error::InvalidPublicKey(e) => write!(f, "invalid public key: {e}"),
            Error::Multihash(e) => write!(f, "invalid multihash: {e}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Protobuf(e) => Some(e),
            Error::SigningError(e) => Some(e),
            Error::InvalidPublicKey(e) => Some(e),
            Error::Multihash(e) => Some(e),
            Error::Cbor(e) => Some(&**e),
            Error::InvalidValidity(e) => Some(e),
            _ => None,
        }
    }
}

impl From<quick_protobuf::Error> for Error {
    fn from(e: quick_protobuf::Error) -> Self {
        Error::Protobuf(e)
    }
}

impl From<chrono::ParseError> for Error {
    fn from(e: chrono::ParseError) -> Self {
        Error::InvalidValidity(e)
    }
}

impl From<Error> for std::io::Error {
    fn from(e: Error) -> Self {
        std::io::Error::new(std::io::ErrorKind::InvalidData, e)
    }
}

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
    type Error = Error;
    fn try_from(i: i32) -> Result<Self, Self::Error> {
        match i {
            0 => Ok(ValidityType::EOL),
            _ => Err(Error::InvalidValidityType),
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
    /// Creates and signs an IPNS record pointing at `value`, valid until the absolute `eol`
    /// (End-Of-Life) timestamp. `ttl` is a caching hint for resolvers.
    pub fn new(
        keypair: &Keypair,
        value: impl AsRef<[u8]>,
        eol: DateTime<Utc>,
        seq: u64,
        ttl: std::time::Duration,
    ) -> Result<Self, Error> {
        let value = value.as_ref().to_vec();

        let ttl = u64::try_from(ttl.as_nanos()).unwrap_or(u64::MAX);

        let validity = eol.to_rfc3339_opts(SecondsFormat::Nanos, true).into_bytes();

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
            .map_err(Error::SigningError)?;

        let document = Data {
            value: Bytes::from(value.clone()),
            validity_type,
            validity: Bytes::from(validity.clone()),
            sequence: seq,
            ttl,
        };

        let data = serde_ipld_dagcbor::to_vec(&document).map_err(|e| Error::Cbor(Box::new(e)))?;

        let signature_v2_construct = SIGNATURE_V2_BASE
            .iter()
            .chain(data.iter())
            .copied()
            .collect::<Vec<_>>();

        let signature_v2 = keypair
            .sign(&signature_v2_construct)
            .map_err(Error::SigningError)?;

        let encoded_public_key = keypair.public().encode_protobuf();
        let public_key = if encoded_public_key.len() > MAX_INLINE_KEY_LENGTH {
            encoded_public_key
        } else {
            Vec::new()
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

    pub fn decode(data: impl AsRef<[u8]>) -> Result<Self, Error> {
        let data = data.as_ref();

        if data.len() > 10 * 1024 {
            return Err(Error::RecordTooLarge);
        }

        let mut reader = BytesReader::from_bytes(data);
        let entry = generate::ipns_pb::IpnsEntry::from_reader(&mut reader, data)?;
        let record = entry.into();
        Ok(record)
    }

    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        let entry: generate::ipns_pb::IpnsEntry = self.into();

        let mut buf = Vec::with_capacity(entry.get_size());
        let mut writer = Writer::new(&mut buf);

        entry.write_message(&mut writer)?;

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

    pub fn validity(&self) -> Result<DateTime<FixedOffset>, Error> {
        let time = String::from_utf8_lossy(&self.validity);
        Ok(chrono::DateTime::parse_from_rfc3339(&time)?)
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

    pub fn data(&self) -> Result<Data, Error> {
        let data: Data =
            serde_ipld_dagcbor::from_slice(&self.data).map_err(|e| Error::Cbor(Box::new(e)))?;

        if data.value != self.value
            || data.validity != self.validity
            || data.validity_type != self.validity_type
            || data.sequence != self.sequence
            || data.ttl != self.ttl
        {
            return Err(Error::DataMismatch);
        }

        Ok(data)
    }

    /// The raw IPNS value.
    pub fn value(&self) -> &[u8] {
        &self.value
    }

    pub fn verify_signature(&self, peer_id: PeerId) -> Result<(), Error> {
        use multihash::Multihash;

        if self.signature_v2.is_empty() {
            return Err(Error::MissingSignature);
        }

        if self.data.is_empty() {
            return Err(Error::EmptyData);
        }

        let public_key = if self.public_key.is_empty() {
            let mh = Multihash::<64>::from_bytes(&peer_id.to_bytes()).map_err(Error::Multihash)?;
            // small keys are inlined in the name via an identity (code 0) multihash; anything
            // else (e.g. an RSA name) carries no inlined key, so the record must embed one.
            if mh.code() != 0 {
                return Err(Error::MissingPublicKey);
            }
            PublicKey::try_decode_protobuf(mh.digest())
        } else {
            PublicKey::try_decode_protobuf(&self.public_key)
        }
        .map_err(Error::InvalidPublicKey)?;

        if PeerId::from_public_key(&public_key) != peer_id {
            return Err(Error::NameMismatch);
        }

        self.data()?;

        let signature_v2 = SIGNATURE_V2_BASE
            .iter()
            .chain(self.data.iter())
            .copied()
            .collect::<Vec<_>>();

        if !public_key.verify(&signature_v2, &self.signature_v2) {
            return Err(Error::InvalidSignature);
        }

        Ok(())
    }

    /// Fully validates the record against `peer_id`: name binding, V2 signature, and that the EOL
    /// validity has not elapsed.
    pub fn verify(&self, peer_id: PeerId) -> Result<(), Error> {
        self.verify_signature(peer_id)?;

        if self.validity()?.with_timezone(&Utc) < Utc::now() {
            return Err(Error::Expired);
        }

        Ok(())
    }

    /// Orders this record against `other` by IPNS precedence: higher `sequence` wins, then the
    /// later EOL `validity`.
    /// Records should already be validated and refer to the same name.
    pub fn compare(&self, other: &Record) -> Result<std::cmp::Ordering, Error> {
        use std::cmp::Ordering;
        match self.sequence.cmp(&other.sequence) {
            Ordering::Equal => Ok(self.validity()?.cmp(&other.validity()?)),
            ord => Ok(ord),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    fn record_for(kp: &Keypair, hours: i64) -> Record {
        Record::new(
            kp,
            b"/ipfs/bafkqaaa",
            Utc::now() + Duration::hours(hours),
            0,
            std::time::Duration::ZERO,
        )
        .unwrap()
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

    #[test]
    fn create_and_verify_across_key_types() {
        // Ed25519/Secp256k1 inline into the name; ECDSA does not, so its record must embed the
        // public key. All must create and verify (and survive a wire round-trip).
        for kp in [
            Keypair::generate_ed25519(),
            Keypair::generate_secp256k1(),
            Keypair::generate_ecdsa(),
        ] {
            let peer = PeerId::from_public_key(&kp.public());
            let rec = Record::new(
                &kp,
                b"/ipfs/bafkqaaa",
                Utc::now() + Duration::hours(24),
                0,
                std::time::Duration::ZERO,
            )
            .unwrap();
            rec.verify(peer).unwrap();
            let decoded = Record::decode(rec.encode().unwrap()).unwrap();
            decoded.verify(peer).unwrap();
        }
    }

    #[test]
    fn compare_prefers_higher_sequence_then_later_validity() {
        use std::cmp::Ordering;
        let kp = Keypair::generate_ed25519();

        let seq0 = Record::new(
            &kp,
            b"/ipfs/bafkqaaa",
            Utc::now() + Duration::hours(24),
            0,
            std::time::Duration::ZERO,
        )
        .unwrap();
        let seq1 = Record::new(
            &kp,
            b"/ipfs/bafkqaaa",
            Utc::now() + Duration::hours(1),
            1,
            std::time::Duration::ZERO,
        )
        .unwrap();
        // higher sequence wins even with an earlier EOL
        assert_eq!(seq1.compare(&seq0).unwrap(), Ordering::Greater);
        assert_eq!(seq0.compare(&seq1).unwrap(), Ordering::Less);

        let near = Record::new(
            &kp,
            b"/ipfs/bafkqaaa",
            Utc::now() + Duration::hours(1),
            5,
            std::time::Duration::ZERO,
        )
        .unwrap();
        let far = Record::new(
            &kp,
            b"/ipfs/bafkqaaa",
            Utc::now() + Duration::hours(48),
            5,
            std::time::Duration::ZERO,
        )
        .unwrap();
        // equal sequence: the later EOL wins
        assert_eq!(far.compare(&near).unwrap(), Ordering::Greater);
    }
}
