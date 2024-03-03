use bitswap_pb::message::{BlockPresenceType, Wantlist};
use libipld::Cid;

use super::{bitswap_pb, pb::bitswap_pb::mod_Message::mod_Wantlist::WantType, prefix::Prefix};



/// Type of a `bitswap` request
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum RequestType {
    Have,
    Block,
}

impl From<WantType> for RequestType {
    fn from(value: WantType) -> Self {
        match value {
            WantType::Have => RequestType::Have,
            WantType::Block => RequestType::Block,
        }
    }
}

impl From<RequestType> for WantType {
    fn from(value: RequestType) -> Self {
        match value {
            RequestType::Have => WantType::Have,
            RequestType::Block => WantType::Block,
        }
    }
}

/// `Bitswap` request type
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BitswapRequest {
    pub ty: RequestType,
    pub cid: Cid,
    pub send_dont_have: bool,
    pub cancel: bool,
}

impl BitswapRequest {
    pub fn new_have(cid: Cid) -> Self {
        Self {
            ty: RequestType::Have,
            cid,
            send_dont_have: false,
            cancel: false,
        }
    }

    pub fn new_block(cid: Cid) -> Self {
        Self {
            ty: RequestType::Block,
            cid,
            send_dont_have: false,
            cancel: false,
        }
    }

    pub fn send_dont_have(mut self, b: bool) -> Self {
        self.send_dont_have = b;
        self
    }

    pub fn new_cancel(cid: Cid) -> Self {
        Self {
            ty: RequestType::Block,
            cid,
            send_dont_have: false,
            cancel: true,
        }
    }
}

/// `Bitswap` response type
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BitswapResponse {
    Have(bool),
    Block(Vec<u8>),
}

/// `Bitswap` message Enum type that is either a [`BitswapRequest`] or a
/// [`BitswapResponse`]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BitswapMessage {
    Request(BitswapRequest),
    Response(Cid, BitswapResponse),
}

impl BitswapMessage {
    pub fn into_proto(self) -> std::io::Result<bitswap_pb::Message> {
        let mut msg = bitswap_pb::Message::default();
        match self {
            Self::Request(BitswapRequest {
                ty,
                cid,
                send_dont_have,
                cancel,
            }) => {
                let wantlist = Wantlist {
                    entries: vec![bitswap_pb::message::wantlist::Entry {
                        block: cid.to_bytes(),
                        wantType: ty.into(),
                        sendDontHave: send_dont_have,
                        cancel,
                        priority: 1,
                    }],
                    ..Default::default()
                };

                msg.wantlist = Some(wantlist);
            }
            Self::Response(cid, BitswapResponse::Have(have)) => {
                msg.blockPresences
                    .push(bitswap_pb::message::BlockPresence {
                        cid: cid.to_bytes(),
                        type_pb: if have {
                            BlockPresenceType::Have
                        } else {
                            BlockPresenceType::DontHave
                        },
                    });
            }
            Self::Response(cid, BitswapResponse::Block(bytes)) => {
                msg.payload.push(bitswap_pb::message::Block {
                    prefix: Prefix::from(cid).to_bytes(),
                    data: bytes,
                });
            }
        }
        Ok(msg)
    }
}