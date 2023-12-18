mod message;
mod pb;
mod prefix;
mod protocol;

pub use message::CompatMessage;
pub use protocol::{CompatProtocol, InboundMessage};

fn other<E: std::error::Error + Send + Sync + 'static>(e: E) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::Other, e)
}
