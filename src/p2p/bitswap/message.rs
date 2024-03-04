use super::{bitswap_pb, pb::bitswap_pb::mod_Message::mod_Wantlist::WantType, prefix::Prefix};
use bitswap_pb::message::{BlockPresenceType, Wantlist};
use libipld::Cid;
use std::io;

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
    pub fn have(cid: Cid) -> Self {
        Self {
            ty: RequestType::Have,
            cid,
            send_dont_have: false,
            cancel: false,
        }
    }

    pub fn block(cid: Cid) -> Self {
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

    pub fn cancel(cid: Cid) -> Self {
        Self {
            ty: RequestType::Block,
            cid,
            send_dont_have: false,
            cancel: true,
        }
    }
}

/// `Bitswap` response type
#[derive(Clone, Debug)]
pub enum BitswapResponse {
    Have(bool),
    Block(Vec<u8>),
}

#[derive(Clone, Debug)]
pub enum BitswapMessage {
    Request(BitswapRequest),
    Response(Cid, BitswapResponse),
}

impl BitswapMessage {
    pub fn from_proto(message: bitswap_pb::Message) -> io::Result<Vec<BitswapMessage>> {
        let mut messages = vec![];
        if let Some(list) = message.wantlist {
            for entry in list.entries {
                let cid = Cid::try_from(entry.block).map_err(io::Error::other)?;
                messages.push(BitswapMessage::Request(BitswapRequest {
                    ty: entry.wantType.into(),
                    cid,
                    send_dont_have: entry.sendDontHave,
                    cancel: entry.cancel,
                }));
            }
        }

        for payload in message.payload {
            let prefix = Prefix::new(&payload.prefix).map_err(io::Error::other)?;
            let cid = prefix.to_cid(&payload.data).map_err(io::Error::other)?;
            messages.push(BitswapMessage::Response(
                cid,
                BitswapResponse::Block(payload.data),
            ));
        }

        for presence in message.blockPresences {
            let cid = Cid::try_from(presence.cid).map_err(io::Error::other)?;
            let have = presence.type_pb == BlockPresenceType::Have;
            messages.push(BitswapMessage::Response(cid, BitswapResponse::Have(have)));
        }

        Ok(messages)
    }

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
                msg.blockPresences.push(bitswap_pb::message::BlockPresence {
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
