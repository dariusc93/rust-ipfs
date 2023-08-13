use chrono::Duration;

use libp2p::identity::Keypair;
use rust_ipns::Record;

fn main() -> std::io::Result<()> {
    let keypair = Keypair::generate_ed25519();

    let record = Record::encode(
        &keypair,
        b"/path/cid".to_vec(),
        Duration::seconds(60),
        0,
        0,
    )?;

    let peer_id = keypair.public().to_peer_id();
    record.verify(peer_id)?;

    Ok(())
}
