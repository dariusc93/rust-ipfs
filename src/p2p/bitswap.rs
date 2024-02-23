mod ledger;
mod pb;
mod prefix;
mod protocol;

use std::{
    collections::{hash_map::Entry, BTreeSet, HashMap, VecDeque},
    task::{Context, Poll},
    time::Duration,
};

use futures::{future::BoxFuture, stream::FuturesUnordered, FutureExt, StreamExt};
use libipld::Cid;
use libp2p::{
    core::Endpoint,
    swarm::{
        behaviour::ConnectionEstablished, dial_opts::DialOpts, ConnectionClosed, ConnectionDenied,
        ConnectionId, FromSwarm, NetworkBehaviour, NotifyHandler, OneShotHandler, THandler,
        THandlerInEvent, THandlerOutEvent, ToSwarm,
    },
    Multiaddr, PeerId,
};
use quick_protobuf::MessageWrite;
use tokio_stream::StreamMap;

mod bitswap_pb {
    pub use super::pb::bitswap_pb::Message;
    pub mod message {
        use super::super::pb::bitswap_pb::mod_Message as message;
        pub use message::mod_Wantlist as wantlist;
        pub use message::Wantlist;
        pub use message::{Block, BlockPresence, BlockPresenceType};
    }
}

use crate::{repo::Repo, Block};

use self::{
    ledger::Ledger,
    prefix::Prefix,
    protocol::{BitswapProtocol, Message},
};

#[derive(Default, Debug, Clone, Copy)]
pub struct Config {
    pub max_wanted_blocks: Option<u8>,
    pub timeout: Option<Duration>,
}

#[derive(Debug)]
pub enum Event {
    NeedBlock { cid: Cid },
}

type FuturesList =
    FuturesUnordered<BoxFuture<'static, Result<Option<bitswap_pb::Message>, BitswapError>>>;

pub struct Behaviour {
    events: VecDeque<ToSwarm<<Self as NetworkBehaviour>::ToSwarm, THandlerInEvent<Self>>>,
    connection: HashMap<PeerId, HashMap<ConnectionId, Multiaddr>>,
    blacklist_connections: HashMap<PeerId, BTreeSet<ConnectionId>>,
    ledger: Ledger,
    store: Repo,
    config: Config,
    tasks: StreamMap<(PeerId, ConnectionId), FuturesList>,
}

impl Behaviour {
    pub fn new(config: Config, store: Repo) -> Self {
        Self {
            events: Default::default(),
            connection: Default::default(),
            blacklist_connections: Default::default(),
            ledger: Ledger::default(),
            store,
            tasks: StreamMap::new(),
            config,
        }
    }

    pub fn get(&mut self, cids: Vec<Cid>, peers: Vec<PeerId>) {
        if cids.is_empty() {
            return;
        }

        let mut message = bitswap_pb::Message::default();

        let mut wantlist = bitswap_pb::message::Wantlist::default();
        for cid in &cids {
            self.ledger.add_want(*cid, 0);
            wantlist.entries.push(bitswap_pb::message::wantlist::Entry {
                cancel: false,
                sendDontHave: true,
                priority: 0,
                wantType: bitswap_pb::message::wantlist::WantType::Have,
                block: cid.to_bytes(),
            });
        }

        message.wantlist = Some(wantlist);

        let peers = match peers.is_empty() {
            true => self.connection.keys().copied().collect::<Vec<_>>(),
            false => {
                for peer_id in &peers {
                    if self.connection.contains_key(peer_id) {
                        continue;
                    }
                    let opts = DialOpts::peer_id(*peer_id).build();

                    self.events.push_back(ToSwarm::Dial { opts });
                }
                peers
            }
        };

        match peers.is_empty() {
            true => {
                for cid in cids {
                    self.events
                        .push_back(ToSwarm::GenerateEvent(Event::NeedBlock { cid }));
                }
            }
            false => self
                .events
                .extend(peers.into_iter().map(|peer_id| ToSwarm::NotifyHandler {
                    peer_id,
                    handler: NotifyHandler::Any,
                    event: message.clone(),
                })),
        }
    }

    pub fn notify_new_blocks(&mut self, cid: impl IntoIterator<Item = Cid>) {
        let mut message = bitswap_pb::Message::default();
        for cid in cid.into_iter() {
            message
                .blockPresences
                .push(bitswap_pb::message::BlockPresence {
                    cid: cid.to_bytes(),
                    type_pb: bitswap_pb::message::BlockPresenceType::Have,
                })
        }

        self.events.extend(
            self.connection
                .keys()
                .map(|peer_id| ToSwarm::NotifyHandler {
                    peer_id: *peer_id,
                    handler: NotifyHandler::Any,
                    event: message.clone(),
                }),
        )
    }

    pub fn on_connection_established(
        &mut self,
        ConnectionEstablished {
            connection_id,
            peer_id,
            endpoint,
            ..
        }: ConnectionEstablished,
    ) {
        let addresses = endpoint.get_remote_address().clone();
        self.connection
            .entry(peer_id)
            .or_default()
            .insert(connection_id, addresses);

        let futs = FuturesUnordered::new();
        futs.push(futures::future::pending().boxed());

        self.tasks.insert((peer_id, connection_id), futs);
    }

    pub fn on_connection_close(
        &mut self,
        ConnectionClosed {
            connection_id,
            peer_id,
            ..
        }: ConnectionClosed,
    ) {
        if let Entry::Occupied(mut entry) = self.connection.entry(peer_id) {
            let list = entry.get_mut();
            list.remove(&connection_id);
            if list.is_empty() {
                self.ledger.remove_peer_haves(peer_id);
                entry.remove();
            }
        }

        if let Entry::Occupied(mut entry) = self.blacklist_connections.entry(peer_id) {
            let list = entry.get_mut();
            list.remove(&connection_id);
            if list.is_empty() {
                entry.remove();
            }
        }

        self.tasks.remove(&(peer_id, connection_id));
    }
}

impl NetworkBehaviour for Behaviour {
    type ConnectionHandler = OneShotHandler<BitswapProtocol, bitswap_pb::Message, Message>;
    type ToSwarm = Event;

    fn handle_pending_inbound_connection(
        &mut self,
        _: ConnectionId,
        _: &Multiaddr,
        _: &Multiaddr,
    ) -> Result<(), ConnectionDenied> {
        Ok(())
    }

    fn handle_pending_outbound_connection(
        &mut self,
        _: ConnectionId,
        _: Option<PeerId>,
        _: &[Multiaddr],
        _: Endpoint,
    ) -> Result<Vec<Multiaddr>, ConnectionDenied> {
        Ok(vec![])
    }

    fn handle_established_inbound_connection(
        &mut self,
        _: ConnectionId,
        _: PeerId,
        _: &Multiaddr,
        _: &Multiaddr,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        Ok(OneShotHandler::default())
    }

    fn handle_established_outbound_connection(
        &mut self,
        _: ConnectionId,
        _: PeerId,
        _: &Multiaddr,
        _: Endpoint,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        Ok(OneShotHandler::default())
    }

    fn on_connection_handler_event(
        &mut self,
        peer_id: PeerId,
        connection_id: ConnectionId,
        event: THandlerOutEvent<Self>,
    ) {
        let message = match event {
            Ok(Message::Receive { message }) => {
                self.blacklist_connections
                    .entry(peer_id)
                    .or_default()
                    .remove(&connection_id);

                message
            }
            Ok(Message::Sent) => {
                self.blacklist_connections
                    .entry(peer_id)
                    .or_default()
                    .remove(&connection_id);
                return;
            }
            Err(_) => {
                //TODO: Depending on the underlining error, maybe blacklist the peer from further sending/receiving
                //      until a valid response or request is produced?
                self.blacklist_connections
                    .entry(peer_id)
                    .or_default()
                    .insert(connection_id);
                return;
            }
        };

        let repo = self.store.clone();
        let ledger = self.ledger.clone();
        let config = self.config;

        let task = async move {
            let mut response = bitswap_pb::Message::default();
            let mut wantlist = bitswap_pb::message::Wantlist::default();

            if !message.payload.is_empty() {
                for block in message.payload {
                    let Ok(prefix) = Prefix::new(&block.prefix) else {
                        continue;
                    };
                    let data = block.data;
                    let Ok(cid) = prefix.to_cid(&data) else {
                        continue;
                    };

                    if !ledger.contains_wants(cid) {
                        continue;
                    }

                    let Ok(block) = Block::new(cid, data) else {
                        continue;
                    };

                    if repo.contains(&cid).await.unwrap_or_default() {
                        continue;
                    }

                    if let Err(_e) = repo.put_block(block).await {
                        continue;
                    }

                    ledger.remove_wants(cid);
                    ledger.remove_have(peer_id, cid);

                    wantlist.entries.push(bitswap_pb::message::wantlist::Entry {
                        cancel: true,
                        sendDontHave: false,
                        priority: 0,
                        wantType: bitswap_pb::message::wantlist::WantType::Block,
                        block: cid.to_bytes(),
                    })
                }
            }

            for block_presence in message.blockPresences {
                match block_presence.type_pb {
                    bitswap_pb::message::BlockPresenceType::Have => {
                        wantlist.entries.push(bitswap_pb::message::wantlist::Entry {
                            cancel: false,
                            sendDontHave: true,
                            priority: 0,
                            wantType: bitswap_pb::message::wantlist::WantType::Block,
                            block: block_presence.cid,
                        });
                    }
                    bitswap_pb::message::BlockPresenceType::DontHave => continue,
                }
            }

            if wantlist.get_size() > 0 {
                response.wantlist = Some(wantlist);
            }

            let Some(list) = message.wantlist else {
                if response.get_size() > 0 {
                    return Ok(Some(response));
                }
                return Err(BitswapError::EmptyWantList);
            };

            if let Some(max_wants) = config.max_wanted_blocks {
                if list.entries.len() > max_wants as _ {
                    if response.get_size() > 0 {
                        return Ok(Some(response));
                    }
                    return Err(BitswapError::MaxEntryExceeded);
                }
            }

            for entry in list.entries {
                let Ok(cid) = Cid::read_bytes(entry.block.as_slice()) else {
                    continue;
                };

                if entry.cancel {
                    //TODO:
                    continue;
                }

                match repo.get_block_now(&cid).await.ok().flatten() {
                    Some(block) => {
                        match entry.wantType {
                            bitswap_pb::message::wantlist::WantType::Block => {
                                let prefix = Prefix::from(cid);
                                response.payload.push(bitswap_pb::message::Block {
                                    prefix: prefix.to_bytes(),
                                    data: block.data().to_vec(),
                                });
                            }
                            bitswap_pb::message::wantlist::WantType::Have => {
                                ledger.add_have(peer_id, cid, entry.priority);
                                response
                                    .blockPresences
                                    .push(bitswap_pb::message::BlockPresence {
                                        cid: cid.to_bytes(),
                                        type_pb: bitswap_pb::message::BlockPresenceType::Have,
                                    })
                            }
                        };
                    }
                    None => {
                        if !entry.sendDontHave {
                            // While we can ignore the request, we should probably always expect this field
                            // and if its not marked true, we should probably score down the peer pr
                            // maybe skip the entry altogether?
                            continue;
                        }

                        response
                            .blockPresences
                            .push(bitswap_pb::message::BlockPresence {
                                cid: cid.to_bytes(),
                                type_pb: bitswap_pb::message::BlockPresenceType::DontHave,
                            })
                    }
                }
            }

            if response.get_size() > 0 {
                return Ok(Some(response));
            }

            Ok(None)
        };

        let task_handler = self
            .tasks
            .iter_mut()
            .find(|((internal_peer_id, internal_connection_id), _)| {
                peer_id == *internal_peer_id && *internal_connection_id == connection_id
            })
            .map(|(_, futs)| futs)
            .expect("connection exist");

        task_handler.push(task.boxed());
    }

    fn on_swarm_event(&mut self, event: FromSwarm) {
        match event {
            FromSwarm::ConnectionEstablished(event) => self.on_connection_established(event),
            FromSwarm::ConnectionClosed(event) => self.on_connection_close(event),
            _ => {}
        }
    }

    fn poll(&mut self, ctx: &mut Context) -> Poll<ToSwarm<Self::ToSwarm, THandlerInEvent<Self>>> {
        if let Some(event) = self.events.pop_front() {
            return Poll::Ready(event);
        }

        loop {
            match self.tasks.poll_next_unpin(ctx) {
                Poll::Ready(Some(((peer_id, connection_id), Ok(Some(message))))) => {
                    return Poll::Ready(ToSwarm::NotifyHandler {
                        peer_id,
                        handler: NotifyHandler::One(connection_id),
                        event: message,
                    })
                }
                Poll::Ready(Some((_, Err(_e)))) => {}
                _ => break,
            }
        }

        Poll::Pending
    }
}

#[derive(Debug, thiserror::Error)]
pub enum BitswapError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("Supplied wantlist is empty")]
    EmptyWantList,
    #[error("Entries exceeded max")]
    MaxEntryExceeded,
}

pub enum BitswapResult {
    Response {
        peer_id: PeerId,
        connection_id: ConnectionId,
        message: bitswap_pb::Message,
    },
    Cancel {
        message: bitswap_pb::Message,
    },
    EmptyResponse,
}
