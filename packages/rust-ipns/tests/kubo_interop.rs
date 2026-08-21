use chrono::{Datelike, Timelike};
use libp2p_identity::ed25519::PublicKey as Ed25519PublicKey;
use libp2p_identity::PeerId;
use libp2p_identity::PublicKey;
use rust_ipns::{Error, Record, ValidityType};
use std::fs;
use std::str::FromStr;

const PUBLIC_KEY_BYTES: [u8; 32] = [
    250, 155, 28, 240, 6, 251, 30, 167, 35, 45, 154, 154, 4, 215, 179, 84, 161, 68, 2, 178, 68, 47,
    140, 220, 75, 67, 224, 135, 51, 233, 181, 84,
];
const PUBLIC_KEY_BYTES_ZERO_SEQ: [u8; 32] = [
    9, 57, 103, 203, 179, 253, 77, 146, 11, 119, 113, 95, 23, 157, 189, 85, 95, 23, 181, 249, 48,
    226, 100, 124, 164, 163, 237, 105, 86, 156, 255, 180,
];

fn decode_hex(hex: &str) -> Vec<u8> {
    let hex = hex.trim();
    assert_eq!(hex.len() % 2, 0);

    hex.as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let digits = std::str::from_utf8(pair).unwrap();
            u8::from_str_radix(digits, 16).unwrap()
        })
        .collect()
}

fn boxo_fixture(label: &str) -> (PeerId, Vec<u8>) {
    let line = include_str!("boxo_0_42_2_records.txt")
        .lines()
        .find(|line| line.starts_with(label))
        .unwrap();
    let mut fields = line.split('|');
    assert_eq!(fields.next(), Some(label));
    let peer_id = PeerId::from_str(fields.next().unwrap()).unwrap();
    let hex = fields.next().unwrap();
    assert!(fields.next().is_none());
    (peer_id, decode_hex(hex))
}

fn assert_boxo_fields(record: &Record, value: &[u8], sequence: u64, ttl: u64) {
    assert_eq!(record.value(), value);
    assert_eq!(record.sequence(), sequence);
    assert_eq!(record.validity_type(), ValidityType::EOL);
    assert_eq!(record.ttl(), ttl);

    let eol = record.validity().unwrap();
    assert_eq!(eol.year(), 2100);
    assert_eq!(eol.month(), 1);
    assert_eq!(eol.day(), 2);
    assert_eq!(eol.hour(), 3);
    assert_eq!(eol.minute(), 4);
    assert_eq!(eol.second(), 5);
    assert_eq!(eol.nanosecond(), 600_000_000);
}

#[test]
fn parse_and_reconstruct_kubo_record() {
    // This IPNS record was created using Kubo (the Go IPFS node implementation)
    let original_record_bytes = fs::read("./tests/test.ipns").unwrap();
    let record = Record::decode(&original_record_bytes).unwrap();
    let public_key: PublicKey = Ed25519PublicKey::try_from_bytes(&PUBLIC_KEY_BYTES)
        .unwrap()
        .into();

    let peer_id = PeerId::from_public_key(&public_key);
    // these fixtures have a long-elapsed EOL, so full verify() rejects them as expired while the
    // signature/name binding still checks out.
    record.verify_signature(peer_id).unwrap();
    assert!(record.verify(peer_id).is_err());

    let new_record_bytes = record.encode().unwrap();
    assert_eq!(original_record_bytes, new_record_bytes);
}

#[test]
fn parse_and_reconstruct_kubo_record_seq_zero() {
    // This IPNS record was created using Kubo (the Go IPFS node implementation).
    // It contains a ZERO sequence value
    let original_record_bytes = fs::read("./tests/test_seq_zero.ipns").unwrap();
    let record = Record::decode(&original_record_bytes).unwrap();
    let public_key: PublicKey = Ed25519PublicKey::try_from_bytes(&PUBLIC_KEY_BYTES_ZERO_SEQ)
        .unwrap()
        .into();

    let peer_id = PeerId::from_public_key(&public_key);
    record.verify_signature(peer_id).unwrap();
    assert!(record.verify(peer_id).is_err());

    let new_record_bytes = record.encode().unwrap();
    assert_eq!(original_record_bytes, new_record_bytes);
}

#[test]
fn official_spec_v2_only_record_validates_and_reads_signed_fields() {
    // Official IPNS Record specification vector 6:
    // https://specs.ipfs.tech/ipns/ipns-record/#test-vectors
    let bytes = decode_hex(include_str!("ipns_v2_only_spec_record.hex"));
    let peer_id = PeerId::from_str("12D3KooWGuR5BdSqp23UeoeesuwYwW3ebQ9rZ8aVwfWEDU8kvCYJ").unwrap();
    let record = Record::decode(&bytes).unwrap();

    assert!(!record.has_signature_v1());
    assert!(record.has_signature_v2());
    assert_eq!(record.value(), b"/ipfs/bafkqadtwgiww63tmpeqhezldn5zgi");
    assert_eq!(record.sequence(), 0);
    assert_eq!(record.ttl(), 1_800_000_000_000);
    assert_eq!(
        record.validity().unwrap().to_rfc3339(),
        "2123-08-14T12:17:03.694052+00:00"
    );
    record.verify(peer_id).unwrap();

    let (wrong_name, _) = boxo_fixture("v2-ed25519");
    assert!(matches!(
        record.verify_signature(wrong_name),
        Err(Error::InvalidSignature)
    ));
}

#[test]
fn boxo_v2_only_records_validate_and_read_signed_fields() {
    for (label, value, sequence, ttl) in [
        (
            "v2-ed25519",
            b"/ipfs/bafkqaaa".as_slice(),
            7,
            90_000_000_000,
        ),
        ("v2-rsa", b"/ipfs/bafkqaaa".as_slice(), 9, 120_000_000_000),
    ] {
        let (peer_id, bytes) = boxo_fixture(label);
        let record = Record::decode(&bytes).unwrap();

        assert!(!record.has_signature_v1());
        assert!(record.has_signature_v2());
        assert_boxo_fields(&record, value, sequence, ttl);
        assert_eq!(record.data().unwrap().value(), value);
        record.verify(peer_id).unwrap();
        assert_eq!(record.encode().unwrap(), bytes);
    }
}

#[test]
fn boxo_v1_v2_record_validates_and_reads_signed_fields() {
    let (peer_id, bytes) = boxo_fixture("v1-v2-ed25519");
    let record = Record::decode(&bytes).unwrap();

    assert!(record.has_signature_v1());
    assert!(record.has_signature_v2());
    assert_boxo_fields(&record, b"/ipfs/bafkqablimvwgy3y", 11, 180_000_000_000);
    record.verify(peer_id).unwrap();
    assert_eq!(record.encode().unwrap(), bytes);
}

#[test]
fn boxo_hybrid_legacy_mismatch_is_rejected() {
    let (peer_id, mut bytes) = boxo_fixture("v1-v2-ed25519");
    let legacy_value = b"/ipfs/bafkqablimvwgy3y";
    let offset = bytes
        .windows(legacy_value.len())
        .position(|window| window == legacy_value)
        .unwrap();
    bytes[offset + legacy_value.len() - 1] ^= 1;

    let record = Record::decode(bytes).unwrap();
    assert_eq!(record.value(), legacy_value);
    assert!(matches!(
        record.verify_signature(peer_id),
        Err(Error::DataMismatch)
    ));
}

#[test]
fn malformed_boxo_v2_data_is_rejected() {
    let (_, mut bytes) = boxo_fixture("v2-ed25519");
    *bytes.last_mut().unwrap() = 0xff;

    assert!(matches!(Record::decode(bytes), Err(Error::Cbor(_))));
}

#[test]
fn boxo_v2_signer_name_matching_and_comparison_use_signed_data() {
    use std::cmp::Ordering;

    let (ed25519_name, ed25519_bytes) = boxo_fixture("v2-ed25519");
    let (rsa_name, rsa_bytes) = boxo_fixture("v2-rsa");
    let (other_ed25519_name, _) = boxo_fixture("v1-v2-ed25519");
    let ed25519 = Record::decode(ed25519_bytes).unwrap();
    let rsa = Record::decode(rsa_bytes).unwrap();

    ed25519.verify_signature(ed25519_name).unwrap();
    rsa.verify_signature(rsa_name).unwrap();
    assert!(matches!(
        ed25519.verify_signature(other_ed25519_name),
        Err(Error::InvalidSignature)
    ));
    assert!(matches!(
        rsa.verify_signature(ed25519_name),
        Err(Error::NameMismatch)
    ));
    assert_eq!(rsa.compare(&ed25519).unwrap(), Ordering::Greater);
}
