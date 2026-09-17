use bytes::Bytes;
use chrono::DateTime;
use chrono::FixedOffset;
use chrono::SecondsFormat;
use chrono::Utc;
use ipld_core::ipld::Ipld;
use libp2p_identity::PeerId;
use libp2p_identity::PublicKey;
use libp2p_identity::{DecodingError, Keypair, SigningError};
use quick_protobuf::MessageWrite;
use quick_protobuf::Writer;
use quick_protobuf::{BytesReader, MessageRead};
use serde::{Deserialize, Serialize, Serializer};
use std::collections::BTreeMap;

mod generate;

const SIGNATURE_V2_BASE: &[u8] = &[
    0x69, 0x70, 0x6e, 0x73, 0x2d, 0x73, 0x69, 0x67, 0x6e, 0x61, 0x74, 0x75, 0x72, 0x65, 0x3a,
];
const MAX_RECORD_SIZE: usize = 10 * 1024;

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
    /// A metadata key collides with a reserved IPNS field name.
    ReservedMetadataKey(String),
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
            Error::ReservedMetadataKey(k) => write!(f, "metadata key `{k}` is reserved"),
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

#[derive(Clone, Debug)]
pub struct Record {
    // Keep the exact bytes for SignatureV2 verification and parse them once for semantic access.
    data: Vec<u8>,
    signed_data: Option<Data>,

    // Legacy V1 protobuf fields are retained for hybrid validation and wire encoding.
    value: Vec<u8>,
    validity_type: Option<i32>,
    validity: Vec<u8>,
    sequence: Option<u64>,
    ttl: Option<u64>,

    public_key: Vec<u8>,

    signature_v1: Vec<u8>,
    signature_v2: Vec<u8>,
}

impl TryFrom<generate::ipns_pb::IpnsEntry<'_>> for Record {
    type Error = Error;

    fn try_from(entry: generate::ipns_pb::IpnsEntry<'_>) -> Result<Self, Self::Error> {
        let signed_data = if entry.data.is_empty() {
            None
        } else {
            Some(
                serde_ipld_dagcbor::from_slice(&entry.data)
                    .map_err(|e| Error::Cbor(Box::new(e)))?,
            )
        };

        Ok(Record {
            data: entry.data.into(),
            signed_data,
            value: entry.value.into(),
            validity_type: entry.validityType,
            validity: entry.validity.into(),
            sequence: entry.sequence,
            ttl: entry.ttl,
            public_key: entry.pubKey.into(),
            signature_v1: entry.signatureV1.into(),
            signature_v2: entry.signatureV2.into(),
        })
    }
}

impl<'a> From<&'a Record> for generate::ipns_pb::IpnsEntry<'a> {
    fn from(record: &'a Record) -> Self {
        generate::ipns_pb::IpnsEntry {
            validity: (&record.validity).into(),
            validityType: record.validity_type,
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

    /// Additional non-standard metadata keys carried in the dag-cbor map. Empty for typical
    /// records; preserved verbatim on decode/encode and covered by the V2 signature.
    #[serde(flatten)]
    pub metadata: BTreeMap<String, Ipld>,
}

impl Data {
    pub fn value(&self) -> &[u8] {
        &self.value
    }

    /// Non-standard metadata keys carried alongside the reserved IPNS fields.
    pub fn metadata(&self) -> &BTreeMap<String, Ipld> {
        &self.metadata
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
        Self::new_with_metadata(keypair, value, eol, seq, ttl, BTreeMap::new())
    }

    /// Like [`Record::new`] but attaches additional dag-cbor `metadata` keys to the record. Keys
    /// must not collide with the reserved fields (`Value`, `Validity`, `ValidityType`, `Sequence`,
    /// `TTL`).
    pub fn new_with_metadata(
        keypair: &Keypair,
        value: impl AsRef<[u8]>,
        eol: DateTime<Utc>,
        seq: u64,
        ttl: std::time::Duration,
        metadata: BTreeMap<String, Ipld>,
    ) -> Result<Self, Error> {
        for reserved in ["Value", "Validity", "ValidityType", "Sequence", "TTL"] {
            if metadata.contains_key(reserved) {
                return Err(Error::ReservedMetadataKey(reserved.to_string()));
            }
        }

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
            metadata,
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

        let record = Record {
            data,
            signed_data: Some(document),
            value,
            validity_type: Some(validity_type.into()),
            validity,
            sequence: Some(seq),
            ttl: Some(ttl),
            public_key,
            signature_v1,
            signature_v2,
        };

        if record.encoded_len() > MAX_RECORD_SIZE {
            return Err(Error::RecordTooLarge);
        }

        Ok(record)
    }

    pub fn decode(data: impl AsRef<[u8]>) -> Result<Self, Error> {
        let data = data.as_ref();

        if data.len() > MAX_RECORD_SIZE {
            return Err(Error::RecordTooLarge);
        }

        let mut reader = BytesReader::from_bytes(data);
        let entry = generate::ipns_pb::IpnsEntry::from_reader(&mut reader, data)?;
        entry.try_into()
    }

    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        let entry: generate::ipns_pb::IpnsEntry = self.into();

        if entry.get_size() > MAX_RECORD_SIZE {
            return Err(Error::RecordTooLarge);
        }

        let mut buf = Vec::with_capacity(entry.get_size());
        let mut writer = Writer::new(&mut buf);

        entry.write_message(&mut writer)?;

        Ok(buf)
    }
}

impl Record {
    pub fn sequence(&self) -> u64 {
        self.signed_data
            .as_ref()
            .map_or_else(|| self.sequence.unwrap_or_default(), Data::sequence)
    }

    pub fn validity_type(&self) -> ValidityType {
        self.signed_data.as_ref().map_or_else(
            || {
                ValidityType::try_from(self.validity_type.unwrap_or_default())
                    .unwrap_or(ValidityType::EOL)
            },
            Data::validity_type,
        )
    }

    pub fn validity(&self) -> Result<DateTime<FixedOffset>, Error> {
        let validity = self
            .signed_data
            .as_ref()
            .map_or(self.validity.as_slice(), Data::validity);
        let time = String::from_utf8_lossy(validity);
        Ok(chrono::DateTime::parse_from_rfc3339(&time)?)
    }

    pub fn ttl(&self) -> u64 {
        self.signed_data
            .as_ref()
            .map_or_else(|| self.ttl.unwrap_or_default(), Data::ttl)
    }

    /// Whether the record carries a (legacy) V1 signature.
    pub fn has_signature_v1(&self) -> bool {
        !self.signature_v1.is_empty()
    }

    /// Whether the record carries a V2 signature.
    pub fn has_signature_v2(&self) -> bool {
        !self.signature_v2.is_empty()
    }

    pub fn data(&self) -> Result<Data, Error> {
        self.signed_data.clone().ok_or(Error::EmptyData)
    }

    /// The canonical IPNS value from signed V2 data, or the legacy value for a V1-only record.
    pub fn value(&self) -> &[u8] {
        self.signed_data
            .as_ref()
            .map_or(self.value.as_slice(), Data::value)
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

        let signature_v2 = SIGNATURE_V2_BASE
            .iter()
            .chain(self.data.iter())
            .copied()
            .collect::<Vec<_>>();

        if !public_key.verify(&signature_v2, &self.signature_v2) {
            return Err(Error::InvalidSignature);
        }

        self.validate_legacy_fields()?;

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

        match self
            .has_signature_v2()
            .cmp(&other.has_signature_v2())
            .then_with(|| self.sequence().cmp(&other.sequence()))
        {
            Ordering::Equal => Ok(self.validity()?.cmp(&other.validity()?)),
            ord => Ok(ord),
        }
    }

    fn encoded_len(&self) -> usize {
        let entry: generate::ipns_pb::IpnsEntry = self.into();
        entry.get_size()
    }

    fn validate_legacy_fields(&self) -> Result<(), Error> {
        // V2-only records omit fields 1-6. SignatureV1 or Value marks a hybrid record whose
        // five legacy values must all match signed Data, per IPNS Record Verification step 7.
        if self.signature_v1.is_empty() && self.value.is_empty() {
            return Ok(());
        }

        let data = self.signed_data.as_ref().ok_or(Error::EmptyData)?;
        if data.value != self.value
            || data.validity != self.validity
            || i32::from(data.validity_type) != self.validity_type.unwrap_or_default()
            || data.sequence != self.sequence.unwrap_or_default()
            || data.ttl != self.ttl.unwrap_or_default()
        {
            return Err(Error::DataMismatch);
        }

        Ok(())
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

    fn without_v1(mut record: Record) -> Record {
        record.value.clear();
        record.validity_type = None;
        record.validity.clear();
        record.sequence = None;
        record.ttl = None;
        record.signature_v1.clear();
        record
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

    #[test]
    fn compare_prefers_v2_over_v1_only() {
        use std::cmp::Ordering;
        let kp = Keypair::generate_ed25519();

        let with_v2 = record_for(&kp, 1);
        let mut v1_only = record_for(&kp, 48); // later EOL, but no V2
        v1_only.signature_v2.clear();
        assert!(with_v2.has_signature_v2() && !v1_only.has_signature_v2());

        // V2 presence outranks both sequence and the later validity
        assert_eq!(with_v2.compare(&v1_only).unwrap(), Ordering::Greater);
        assert_eq!(v1_only.compare(&with_v2).unwrap(), Ordering::Less);
    }

    #[test]
    fn v2_only_records_use_signed_data() {
        use std::cmp::Ordering;

        let kp = Keypair::generate_ed25519();
        let peer = PeerId::from_public_key(&kp.public());
        let eol = Utc::now() + Duration::hours(24);
        let record = without_v1(
            Record::new(
                &kp,
                b"/ipfs/bafkqablimvwgy3y",
                eol,
                42,
                std::time::Duration::from_secs(90),
            )
            .unwrap(),
        );

        assert!(!record.has_signature_v1());
        assert_eq!(record.value(), b"/ipfs/bafkqablimvwgy3y");
        assert_eq!(record.sequence(), 42);
        assert_eq!(record.validity_type(), ValidityType::EOL);
        assert_eq!(record.validity().unwrap(), eol.fixed_offset());
        assert_eq!(record.ttl(), 90_000_000_000);
        assert_eq!(record.data().unwrap().sequence(), 42);
        record.verify_signature(peer).unwrap();

        let newer = without_v1(
            Record::new(
                &kp,
                b"/ipfs/bafkqablimvwgy3y",
                eol - Duration::hours(1),
                43,
                std::time::Duration::from_secs(90),
            )
            .unwrap(),
        );
        assert_eq!(newer.compare(&record).unwrap(), Ordering::Greater);

        let later = without_v1(
            Record::new(
                &kp,
                b"/ipfs/bafkqablimvwgy3y",
                eol + Duration::hours(1),
                42,
                std::time::Duration::from_secs(90),
            )
            .unwrap(),
        );
        assert_eq!(later.compare(&record).unwrap(), Ordering::Greater);

        let roundtrip = Record::decode(record.encode().unwrap()).unwrap();
        roundtrip.verify_signature(peer).unwrap();
        assert_eq!(roundtrip.sequence(), 42);

        let mut scalar_only = record;
        scalar_only.sequence = Some(0);
        let scalar_only_bytes = scalar_only.encode().unwrap();
        let scalar_only_roundtrip = Record::decode(&scalar_only_bytes).unwrap();
        scalar_only_roundtrip.verify_signature(peer).unwrap();
        assert_eq!(scalar_only_roundtrip.sequence(), 42);
        assert_eq!(scalar_only_roundtrip.encode().unwrap(), scalar_only_bytes);
    }

    #[test]
    fn hybrid_mismatch_is_rejected_after_v2_signature_verification() {
        let kp = Keypair::generate_ed25519();
        let peer = PeerId::from_public_key(&kp.public());
        let mut record = record_for(&kp, 24);

        record.value = b"/ipfs/bafkqaaa-mismatch".to_vec();

        assert_eq!(record.value(), b"/ipfs/bafkqaaa");
        assert_eq!(record.data().unwrap().value(), b"/ipfs/bafkqaaa");
        assert!(matches!(
            record.verify_signature(peer),
            Err(Error::DataMismatch)
        ));

        let mut invalid_signature = record.clone();
        invalid_signature.signature_v2[0] ^= 1;
        assert!(matches!(
            invalid_signature.verify_signature(peer),
            Err(Error::InvalidSignature)
        ));

        let mut invalid_validity_type = record_for(&kp, 24);
        invalid_validity_type.validity_type = Some(1);
        assert!(matches!(
            invalid_validity_type.verify_signature(peer),
            Err(Error::DataMismatch)
        ));

        let mut invalid_validity = record_for(&kp, 24);
        invalid_validity.validity[0] ^= 1;
        assert!(matches!(
            invalid_validity.verify_signature(peer),
            Err(Error::DataMismatch)
        ));

        let mut invalid_sequence = record_for(&kp, 24);
        invalid_sequence.sequence = Some(1);
        assert!(matches!(
            invalid_sequence.verify_signature(peer),
            Err(Error::DataMismatch)
        ));

        let mut invalid_ttl = record_for(&kp, 24);
        invalid_ttl.ttl = Some(1);
        assert!(matches!(
            invalid_ttl.verify_signature(peer),
            Err(Error::DataMismatch)
        ));
    }

    #[test]
    fn malformed_v2_data_is_rejected_during_decode() {
        let kp = Keypair::generate_ed25519();
        let mut record = record_for(&kp, 24);
        record.data = vec![0xff];

        let encoded = record.encode().unwrap();
        assert!(matches!(Record::decode(encoded), Err(Error::Cbor(_))));
    }

    #[test]
    fn record_size_limit_applies_to_creation_encoding_and_decode() {
        let kp = Keypair::generate_ed25519();
        let oversized_value = vec![b'x'; MAX_RECORD_SIZE];
        assert!(matches!(
            Record::new(
                &kp,
                oversized_value,
                Utc::now() + Duration::hours(24),
                0,
                std::time::Duration::ZERO,
            ),
            Err(Error::RecordTooLarge)
        ));

        let mut record = record_for(&kp, 24);
        record.data = vec![0; MAX_RECORD_SIZE];
        assert!(matches!(record.encode(), Err(Error::RecordTooLarge)));
        assert!(matches!(
            Record::decode(vec![0; MAX_RECORD_SIZE + 1]),
            Err(Error::RecordTooLarge)
        ));
        assert!(!matches!(
            Record::decode(vec![0; MAX_RECORD_SIZE]),
            Err(Error::RecordTooLarge)
        ));
    }

    #[test]
    fn metadata_roundtrips_and_is_signed() {
        let kp = Keypair::generate_ed25519();
        let peer = PeerId::from_public_key(&kp.public());
        let mut metadata = BTreeMap::new();
        metadata.insert("Foo".to_string(), Ipld::String("bar".into()));
        metadata.insert("Count".to_string(), Ipld::Integer(7));

        let rec = Record::new_with_metadata(
            &kp,
            b"/ipfs/bafkqaaa",
            Utc::now() + Duration::hours(24),
            0,
            std::time::Duration::ZERO,
            metadata.clone(),
        )
        .unwrap();
        rec.verify(peer).unwrap();

        // survives a wire round-trip, still verifies, and exposes the metadata unchanged
        let decoded = Record::decode(rec.encode().unwrap()).unwrap();
        decoded.verify(peer).unwrap();
        assert_eq!(decoded.data().unwrap().metadata(), &metadata);

        // reserved keys are rejected
        let mut bad = BTreeMap::new();
        bad.insert("TTL".to_string(), Ipld::Integer(1));
        assert!(matches!(
            Record::new_with_metadata(
                &kp,
                b"/x",
                Utc::now() + Duration::hours(1),
                0,
                std::time::Duration::ZERO,
                bad,
            ),
            Err(Error::ReservedMetadataKey(_))
        ));
    }
}
