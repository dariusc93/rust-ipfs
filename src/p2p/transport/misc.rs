use hkdf::Hkdf;
use libp2p::identity::{self as identity, Keypair};
use p256::ecdsa::signature::Signer;
use rand::SeedableRng;
use rand_chacha::ChaCha20Rng;
use rcgen::{Certificate, CertificateParams, DnType, KeyPair};
use sec1::{der::Encode, pkcs8::EncodePrivateKey};
use sha2::Sha256;
use std::io;
use web_time::{Duration, SystemTime};

/// The year 2000.
const UNIX_2000: i64 = 946645200;

/// The year 3000.
const UNIX_3000: i64 = 32503640400;

/// OID for the organisation name. See <http://oid-info.com/get/2.5.4.10>.
const ORGANISATION_NAME_OID: [u64; 4] = [2, 5, 4, 10];

/// OID for Elliptic Curve Public Key Cryptography. See <http://oid-info.com/get/1.2.840.10045.2.1>.
const EC_OID: [u64; 6] = [1, 2, 840, 10045, 2, 1];

/// OID for 256-bit Elliptic Curve Cryptography (ECC) with the P256 curve. See <http://oid-info.com/get/1.2.840.10045.3.1.7>.
const P256_OID: [u64; 7] = [1, 2, 840, 10045, 3, 1, 7];

/// OID for the ECDSA signature algorithm with using SHA256 as the hash function. See <http://oid-info.com/get/1.2.840.10045.4.3.2>.
const ECDSA_SHA256_OID: [u64; 7] = [1, 2, 840, 10045, 4, 3, 2];

const ENCODE_CONFIG: pem::EncodeConfig = {
    let line_ending = match cfg!(target_family = "windows") {
        true => pem::LineEnding::CRLF,
        false => pem::LineEnding::LF,
    };
    pem::EncodeConfig::new().set_line_ending(line_ending)
};

/// Generates a TLS certificate that derives from libp2p `Keypair` with a salt.
/// Note: If `expire` is true, it will produce a expired pem that can be appended for webrtc transport
///       Additionally, this function does not generate deterministic certs *yet* due to
///       `CertificateParams::self_signed` using ring rng. This may change in the future
pub fn generate_cert(
    keypair: &Keypair,
    salt: &[u8],
    expire: bool,
) -> io::Result<(Certificate, KeyPair, Option<String>)> {
    let internal_keypair = derive_keypair(keypair, salt)?;
    let mut param =
        CertificateParams::new(vec!["localhost".into()]).map_err(std::io::Error::other)?;
    param.distinguished_name.push(
        DnType::CommonName,
        keypair.public().to_peer_id().to_string().as_str(),
    );

    // Note: The certificate, while it is signed,
    let cert = param
        .self_signed(&internal_keypair)
        .map_err(std::io::Error::other)?;

    let expired_pem = expire.then(|| {
        let expired = SystemTime::UNIX_EPOCH
            .checked_add(Duration::from_secs(UNIX_3000 as u64))
            .expect("year 3000 to be representable by SystemTime")
            .to_der()
            .unwrap();

        pem::encode_config(
            &pem::Pem::new("EXPIRES".to_string(), expired),
            ENCODE_CONFIG,
        )
    });

    Ok((cert, internal_keypair, expired_pem))
}

/// Used to generate webrtc certificates.
/// Note: Although simple_x509 does not deal with crypto directly (eg signing certificate)
///       we would still have to be careful of any changes upstream that may cause a change in the certificate

pub(crate) fn generate_wrtc_cert(keypair: &Keypair) -> io::Result<String> {
    let (secret, public_key) = derive_keypair_secret(keypair, b"libp2p-webrtc")?;
    let peer_id = keypair.public().to_peer_id();

    let certificate = simple_x509::X509::builder()
        .issuer_utf8(Vec::from(ORGANISATION_NAME_OID), "rust-ipfs")
        .subject_utf8(Vec::from(ORGANISATION_NAME_OID), &peer_id.to_string())
        .not_before_gen(UNIX_2000)
        .not_after_gen(UNIX_3000)
        .pub_key_ec(
            Vec::from(EC_OID),
            public_key.to_encoded_point(false).as_bytes().to_owned(),
            Vec::from(P256_OID),
        )
        .sign_oid(Vec::from(ECDSA_SHA256_OID))
        .build()
        .sign(
            |cert, _| {
                let signature: p256::ecdsa::DerSignature = secret.sign(cert);
                Some(signature.as_bytes().to_owned())
            },
            &[], // We close over the keypair so no need to pass it.
        )
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("{e:?}")))?;

    let der_bytes = certificate.x509_enc().unwrap();

    let cert_pem = pem::encode_config(
        &pem::Pem::new("CERTIFICATE".to_string(), der_bytes),
        ENCODE_CONFIG,
    );

    let private_pem = secret
        .to_pkcs8_pem(Default::default())
        .map_err(std::io::Error::other)?
        .replace("PRIVATE KEY", "PRIVATE_KEY");

    let expired_pem = {
        let expired = SystemTime::UNIX_EPOCH
            .checked_add(Duration::from_secs(UNIX_3000 as u64))
            .expect("year 3000 to be representable by SystemTime")
            .to_der()
            .unwrap();

        pem::encode_config(
            &pem::Pem::new("EXPIRES".to_string(), expired),
            ENCODE_CONFIG,
        )
    };

    let pem = expired_pem + "\n\n" + &private_pem + "\n\n" + &cert_pem;

    Ok(pem)
}

fn derive_keypair(keypair: &Keypair, salt: &[u8]) -> io::Result<KeyPair> {
    let (secret, _) = derive_keypair_secret(keypair, salt)?;

    let pem = secret
        .to_pkcs8_pem(Default::default())
        .map_err(std::io::Error::other)?;

    KeyPair::from_pem(&pem).map_err(std::io::Error::other)
}

fn derive_keypair_secret(
    keypair: &Keypair,
    salt: &[u8],
) -> io::Result<(p256::ecdsa::SigningKey, p256::ecdsa::VerifyingKey)> {
    let secret = keypair_secret(keypair).ok_or(io::Error::from(io::ErrorKind::Unsupported))?;
    let hkdf_gen = Hkdf::<Sha256>::from_prk(secret.as_ref()).expect("key length to be valid");

    let mut seed = [0u8; 32];
    hkdf_gen
        .expand(salt, &mut seed)
        .expect("key length to be valid");

    let mut rng = ChaCha20Rng::from_seed(seed);

    let secret = p256::ecdsa::SigningKey::random(&mut rng);
    let public = p256::ecdsa::VerifyingKey::from(&secret);

    Ok((secret, public))
}

fn keypair_secret(keypair: &Keypair) -> Option<[u8; 32]> {
    match keypair.key_type() {
        identity::KeyType::Ed25519 => {
            let keypair = keypair.clone().try_into_ed25519().ok()?;
            let secret = keypair.secret();
            Some(secret.as_ref().try_into().expect("secret is 32 bytes"))
        }
        identity::KeyType::RSA => None,
        identity::KeyType::Secp256k1 => {
            let keypair = keypair.clone().try_into_secp256k1().ok()?;
            let secret = keypair.secret();
            Some(secret.to_bytes())
        }
        identity::KeyType::Ecdsa => {
            let keypair = keypair.clone().try_into_ecdsa().ok()?;
            Some(
                keypair
                    .secret()
                    .to_bytes()
                    .try_into()
                    .expect("secret is 32 bytes"),
            )
        }
    }
}

#[cfg(test)]
mod test {
    use libp2p::identity::Keypair;

    use crate::p2p::transport::misc::generate_wrtc_cert;

    const PEM: &str = r#"-----BEGIN EXPIRES-----
GA8yOTk5MTIzMTEzMDAwMFo=
-----END EXPIRES-----


-----BEGIN PRIVATE_KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgXARqgq74dVCrVR6G
VT/iHnwBmx9s217QqvegG1xKNpqhRANCAAQvm08WYqoMCCEF36I5OAhA/XS7SqhR
7n2CahGwC/fEqtvRrwAfZGejF21lzOW/m+A3EbDIzjy+xpUY+zaCE57V
-----END PRIVATE_KEY-----


-----BEGIN CERTIFICATE-----
MIIBPjCB5QIBADAKBggqhkjOPQQDAjAUMRIwEAYDVQQKDAlydXN0LWlwZnMwIhgP
MTk5OTEyMzExMzAwMDBaGA8yOTk5MTIzMTEzMDAwMFowPzE9MDsGA1UECgw0MTJE
M0tvb1dQamNlUXJTd2RXWFB5TExlQUJSWG11cXQ2OVJnM3NCWWJVMU5mdDlIeVE2
WDBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABC+bTxZiqgwIIQXfojk4CED9dLtK
qFHufYJqEbAL98Sq29GvAB9kZ6MXbWXM5b+b4DcRsMjOPL7GlRj7NoITntUwCgYI
KoZIzj0EAwIDSAAwRQIhAP+F5COvtCQbZiyBQpAoiIoQP12KwIsNe1zhumki4bkU
AiAH43Q833G8p1eXxqJr2xRrA1B5vCZ1qgl/44Z++NDMqQ==
-----END CERTIFICATE-----
"#;

    #[test]
    fn generate_cert() {
        let keypair = generate_ed25519();
        let pem = generate_wrtc_cert(&keypair).expect("not to fail");
        assert_eq!(pem, PEM)
    }

    fn generate_ed25519() -> Keypair {
        let mut bytes = [0u8; 32];
        bytes[0] = 1;
    
        Keypair::ed25519_from_bytes(bytes).expect("only errors on wrong length")
    }
    
}
