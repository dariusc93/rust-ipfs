use super::{bitswap_pb, pb::bitswap_pb::mod_Message::mod_Wantlist::WantType, prefix::Prefix};
use bitswap_pb::message::{BlockPresenceType, Wantlist};
use bytes::Bytes;
use libipld::Cid;
use std::{collections::HashMap, fmt::Debug, hash::Hash, io};

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
#[derive(Clone, Copy, Debug, Eq)]
pub struct BitswapRequest {
    pub ty: RequestType,
    pub cid: Cid,
    pub send_dont_have: bool,
    pub cancel: bool,
    pub priority: i32,
}

impl Hash for BitswapRequest {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        Hash::hash(&self.cid, state)
    }
}

impl PartialEq for BitswapRequest {
    fn eq(&self, other: &Self) -> bool {
        self.cid.eq(&other.cid)
    }
}

impl PartialOrd for BitswapRequest {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.priority.cmp(&other.priority))
    }
}

impl BitswapRequest {
    pub fn have(cid: Cid) -> Self {
        Self {
            ty: RequestType::Have,
            cid,
            send_dont_have: false,
            cancel: false,
            priority: 1,
        }
    }

    pub fn block(cid: Cid) -> Self {
        Self {
            ty: RequestType::Block,
            cid,
            send_dont_have: false,
            cancel: false,
            priority: 1,
        }
    }

    pub fn send_dont_have(mut self, b: bool) -> Self {
        self.send_dont_have = b;
        self
    }

    pub fn set_priority(mut self, p: i32) -> Self {
        self.priority = p;
        self
    }

    pub fn cancel(cid: Cid) -> Self {
        Self {
            ty: RequestType::Block,
            cid,
            send_dont_have: false,
            cancel: true,
            priority: 1,
        }
    }
}

/// `Bitswap` response type
#[derive(Clone)]
pub enum BitswapResponse {
    Have(bool),
    Block(Bytes),
}

impl Debug for BitswapResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BitswapResponse::Have(have) => write!(f, "Have({have})"),
            BitswapResponse::Block(block) => write!(f, "Block({} bytes)", block.len()),
        }
    }
}

#[derive(Default, Clone, Debug)]
pub struct BitswapMessage {
    pub requests: Vec<BitswapRequest>,
    pub responses: HashMap<Cid, BitswapResponse>,
    pub full: bool,
}

impl BitswapMessage {
    pub fn new(full: bool) -> Self {
        Self {
            full,
            ..Default::default()
        }
    }

    pub fn add_request(mut self, request: BitswapRequest) -> Self {
        self.requests.push(request);
        self
    }

    pub fn set_requests(mut self, requests: Vec<BitswapRequest>) -> Self {
        self.requests = requests;
        self
    }

    pub fn add_response(mut self, cid: Cid, response: BitswapResponse) -> Self {
        self.responses.insert(cid, response);
        self
    }

    pub fn set_responses(mut self, responses: Vec<(Cid, BitswapResponse)>) -> Self {
        self.responses = HashMap::from_iter(responses);
        self
    }
}

impl BitswapMessage {
    pub fn from_proto(message: bitswap_pb::Message) -> io::Result<BitswapMessage> {
        let mut bitswap_message = Self::default();

        if let Some(list) = message.wantlist {
            bitswap_message.full = list.full;
            for entry in list.entries {
                let cid = Cid::try_from(entry.block).map_err(io::Error::other)?;
                bitswap_message.requests.push(BitswapRequest {
                    ty: entry.wantType.into(),
                    cid,
                    send_dont_have: entry.sendDontHave,
                    cancel: entry.cancel,
                    priority: entry.priority,
                });
            }
        }

        for payload in message.payload {
            let prefix = Prefix::new(&payload.prefix).map_err(io::Error::other)?;
            let cid = prefix.to_cid(&payload.data).map_err(io::Error::other)?;
            bitswap_message
                .responses
                .insert(cid, BitswapResponse::Block(Bytes::from(payload.data)));
        }

        for presence in message.blockPresences {
            let cid = Cid::try_from(presence.cid).map_err(io::Error::other)?;
            let have = presence.type_pb == BlockPresenceType::Have;
            bitswap_message
                .responses
                .insert(cid, BitswapResponse::Have(have));
        }

        Ok(bitswap_message)
    }

    pub fn is_empty(&self) -> bool {
        self.requests.is_empty() && self.responses.is_empty()
    }

    pub fn into_proto(self) -> std::io::Result<bitswap_pb::Message> {
        // We shouldnt be reaching the point where we are sending an empty message. In such case, we should return an error
        if self.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "message is empty",
            ));
        }

        let mut msg = bitswap_pb::Message::default();
        let BitswapMessage {
            requests,
            responses,
            full,
        } = self;

        let mut wantlist = Wantlist {
            full,
            ..Default::default()
        };

        for BitswapRequest {
            ty,
            cid,
            send_dont_have,
            cancel,
            priority,
        } in requests
        {
            wantlist.entries.push(bitswap_pb::message::wantlist::Entry {
                block: cid.to_bytes(),
                wantType: ty.into(),
                sendDontHave: send_dont_have,
                cancel,
                priority,
            });
        }

        msg.wantlist = Some(wantlist);

        for (cid, response) in responses {
            match response {
                BitswapResponse::Have(have) => {
                    msg.blockPresences.push(bitswap_pb::message::BlockPresence {
                        cid: cid.to_bytes(),
                        type_pb: if have {
                            BlockPresenceType::Have
                        } else {
                            BlockPresenceType::DontHave
                        },
                    });
                }
                BitswapResponse::Block(bytes) => {
                    msg.payload.push(bitswap_pb::message::Block {
                        prefix: Prefix::from(cid).to_bytes(),
                        data: bytes.to_vec(),
                    });
                }
            }
        }

        Ok(msg)
    }
}
