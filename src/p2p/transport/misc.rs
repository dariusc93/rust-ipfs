use hkdf::Hkdf;
use libp2p::identity::{self as identity, Keypair};
use rand::SeedableRng;
use rand_chacha::ChaCha20Rng;
use rcgen::{Certificate, CertificateParams, DnType, KeyPair};
use sec1::{der::Encode, pkcs8::EncodePrivateKey};
use sha2::Sha256;
use std::io;
use web_time::{Duration, SystemTime};

const UNIX_3000: i64 = 32503640400;

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

fn derive_keypair(keypair: &Keypair, salt: &[u8]) -> io::Result<KeyPair> {
    //Note: We could use `Keypair::derive_secret`, but this seems more sensible?
    let secret = keypair_secret(keypair).ok_or(io::Error::from(io::ErrorKind::Unsupported))?;
    let hkdf_gen = Hkdf::<Sha256>::from_prk(secret.as_ref()).expect("key length to be valid");

    let mut seed = [0u8; 32];
    hkdf_gen
        .expand(salt, &mut seed)
        .expect("key length to be valid");

    let mut rng = ChaCha20Rng::from_seed(seed);

    let secret = p256::ecdsa::SigningKey::random(&mut rng);

    let pem = secret
        .to_pkcs8_pem(Default::default())
        .map_err(std::io::Error::other)?;

    KeyPair::from_pem(&pem).map_err(std::io::Error::other)
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
