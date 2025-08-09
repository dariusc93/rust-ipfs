use anyhow::anyhow;
use futures::{
    channel::{mpsc::Receiver, oneshot},
    FutureExt,
};
use pollable_map::optional::Optional;

use crate::{p2p, p2p::MultiaddrExt, Channel};

use crate::repo::{Repo, RepoEvent};
use crate::{config::BOOTSTRAP_NODES, IpfsEvent};

use ipld_core::cid::Cid;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::sync::Arc;

use crate::repo::DefaultStorage;

use connexa::behaviour::peer_store::store::memory::MemoryStore;
use connexa::behaviour::Behaviour as ConnexaBehaviour;
use connexa::prelude::identify::Info;
use connexa::prelude::swarm::{NetworkBehaviour, Swarm};
use connexa::prelude::{Multiaddr, PeerId};
use tokio::sync::Notify;

#[allow(clippy::type_complexity)]
#[allow(dead_code)]
pub struct IpfsContext {
    pub repo_events: Optional<Receiver<RepoEvent>>,
    pub bitswap_cancellable: HashMap<Cid, Vec<Arc<Notify>>>,
    pub repo: Repo<DefaultStorage>,
    pub bootstraps: HashSet<Multiaddr>,
    pub find_peer_identify: HashMap<PeerId, Vec<oneshot::Sender<anyhow::Result<Info>>>>,
    pub relay_listener: HashMap<PeerId, Vec<Channel<()>>>,
}

impl Default for IpfsContext {
    fn default() -> Self {
        Self {
            repo_events: Default::default(),
            bitswap_cancellable: Default::default(),
            repo: Repo::new_memory(),
            bootstraps: Default::default(),
            find_peer_identify: Default::default(),
            relay_listener: Default::default(),
        }
    }
}

impl IpfsContext {
    pub fn new(repo: &Repo<DefaultStorage>) -> Self {
        Self {
            repo_events: Default::default(),
            bitswap_cancellable: Default::default(),
            repo: repo.clone(),
            bootstraps: Default::default(),
            find_peer_identify: Default::default(),
            relay_listener: Default::default(),
        }
    }
}

impl IpfsContext {
    fn custom_behaviour<'a, N: NetworkBehaviour>(
        &self,
        swarm: &'a mut Swarm<ConnexaBehaviour<p2p::Behaviour<N>, MemoryStore>>,
    ) -> &'a mut p2p::Behaviour<N>
    where
        N::ToSwarm: Debug,
    {
        swarm
            .behaviour_mut()
            .custom
            .as_mut()
            .expect("behaviour enabled")
    }

    pub(crate) fn handle_event<N: NetworkBehaviour>(
        &mut self,
        swarm: &mut Swarm<ConnexaBehaviour<p2p::Behaviour<N>, MemoryStore>>,
        event: IpfsEvent,
    ) where
        N::ToSwarm: Debug,
    {
        match event {
            IpfsEvent::AddPeer(opt, ret) => {
                if let Some(kad) = swarm.behaviour_mut().kademlia.as_mut() {
                    let peer_id = opt.peer_id();
                    let addrs = opt.addresses().to_vec();
                    for addr in addrs {
                        kad.add_address(peer_id, addr);
                    }
                }
                let result = match self.custom_behaviour(swarm).add_peer(opt) {
                    true => Ok(()),
                    false => Err(anyhow::anyhow!("unable to add peer")),
                };

                let _ = ret.send(result);
            }
            IpfsEvent::Addresses(ret) => {
                let custom_behaviour = self.custom_behaviour(swarm);
                let addrs = custom_behaviour
                    .peerbook
                    .connected_peers_addrs()
                    .collect::<Vec<_>>();
                let _ = ret.send(Ok(addrs));
            }
            IpfsEvent::RemovePeer(peer_id, addr, ret) => {
                let custom_behaviour = self.custom_behaviour(swarm);
                let result = match addr {
                    Some(addr) => Ok(custom_behaviour.addressbook.remove_address(&peer_id, &addr)),
                    None => Ok(custom_behaviour.addressbook.remove_peer(&peer_id)),
                };

                let _ = ret.send(result);
            }
            IpfsEvent::Protocol(ret) => {
                let info = self.custom_behaviour(swarm).supported_protocols();
                let _ = ret.send(info);
            }
            // IpfsEvent::WantList(peer, ret) => {
            //     let list = if let Some(peer) = peer {
            //         self.swarm
            //             .behaviour_mut()
            //             .bitswap()
            //             .peer_wantlist(&peer)
            //             .unwrap_or_default()
            //     } else {
            //         self.swarm.behaviour_mut().bitswap().local_wantlist()
            //     };
            //     let _ = ret.send(list);
            // }
            // IpfsEvent::BitswapStats(ret) => {
            //     let stats = self.swarm.behaviour_mut().bitswap().stats();
            //     let peers = self.swarm.behaviour_mut().bitswap().peers();
            //     let wantlist = self.swarm.behaviour_mut().bitswap().local_wantlist();
            //     let _ = ret.send((stats, peers, wantlist).into());
            // }
            IpfsEvent::WantList(peer, ret) => {
                let Some(bitswap) = self.custom_behaviour(swarm).bitswap.as_ref() else {
                    let _ = ret.send(Ok(futures::future::ready(vec![]).boxed()));
                    return;
                };
                let list = match peer {
                    Some(peer_id) => bitswap.peer_wantlist(peer_id),
                    None => bitswap.local_wantlist(),
                };
                let _ = ret.send(Ok(futures::future::ready(list).boxed()));
            }
            IpfsEvent::GetBitswapPeers(ret) => {
                let _ = ret.send(Ok(futures::future::ready(vec![]).boxed()));
            }
            IpfsEvent::FindPeerIdentity(peer_id, ret) => {
                let locally_known = self.custom_behaviour(swarm).peerbook.get_peer_info(peer_id);

                let (tx, rx) = oneshot::channel();

                match locally_known {
                    Some(info) => {
                        let _ = tx.send(Ok(info.clone()));
                    }
                    None => {
                        let Some(kad) = swarm.behaviour_mut().kademlia.as_mut() else {
                            let _ = ret.send(Err(anyhow!("kad protocol is disabled")));
                            return;
                        };

                        kad.get_closest_peers(peer_id);
                        self.find_peer_identify.entry(peer_id).or_default().push(tx);
                    }
                }

                let _ = ret.send(Ok(rx));
            }
            IpfsEvent::GetBootstrappers(ret) => {
                let list = Vec::from_iter(self.bootstraps.iter().cloned());
                let _ = ret.send(list);
            }
            IpfsEvent::AddBootstrapper(mut addr, ret) => {
                let Some(kad) = swarm.behaviour_mut().kademlia.as_mut() else {
                    let _ = ret.send(Err(anyhow!("kad protocol is disabled")));
                    return;
                };

                let ret_addr = addr.clone();

                if self.bootstraps.insert(addr.clone()) {
                    if let Some(peer_id) = addr.extract_peer_id() {
                        kad.add_address(&peer_id, addr.clone());
                        self.custom_behaviour(swarm).add_peer((peer_id, addr));
                        // the return value of add_address doesn't implement Debug
                        trace!(peer_id=%peer_id, "tried to add a bootstrapper");
                    }
                }
                let _ = ret.send(Ok(ret_addr));
            }
            IpfsEvent::RemoveBootstrapper(mut addr, ret) => {
                let Some(kad) = swarm.behaviour_mut().kademlia.as_mut() else {
                    let _ = ret.send(Err(anyhow!("kad protocol is disabled")));
                    return;
                };

                let result = addr.clone();

                if self.bootstraps.remove(&addr) {
                    if let Some(peer_id) = addr.extract_peer_id() {
                        let prefix: Multiaddr = addr;

                        if let Some(e) = kad.remove_address(&peer_id, &prefix) {
                            info!(peer_id=%peer_id, status=?e.status, "removed bootstrapper");
                        } else {
                            warn!(peer_id=%peer_id, "attempted to remove an unknown bootstrapper");
                        }
                    }

                    let _ = ret.send(Ok(result));
                }
            }
            IpfsEvent::ClearBootstrappers(ret) => {
                let Some(kad) = swarm.behaviour_mut().kademlia.as_mut() else {
                    let _ = ret.send(Err(anyhow!("kad protocol is disabled")));
                    return;
                };

                let removed = self.bootstraps.drain().collect::<Vec<_>>();
                let mut list = Vec::with_capacity(removed.len());

                for mut addr_with_peer_id in removed {
                    let priginal = addr_with_peer_id.clone();
                    let Some(peer_id) = addr_with_peer_id.extract_peer_id() else {
                        continue;
                    };
                    let prefix: Multiaddr = addr_with_peer_id;

                    if let Some(e) = kad.remove_address(&peer_id, &prefix) {
                        info!(peer_id=%peer_id, status=?e.status, "cleared bootstrapper");
                        list.push(priginal);
                    } else {
                        error!(peer_id=%peer_id, "attempted to clear an unknown bootstrapper");
                    }
                }

                let _ = ret.send(Ok(list));
            }
            IpfsEvent::DefaultBootstrap(ret) => {
                if !swarm.behaviour().kademlia.is_enabled() {
                    let _ = ret.send(Err(anyhow!("kad protocol is disabled")));
                    return;
                };

                let mut rets = Vec::new();
                for addr in BOOTSTRAP_NODES {
                    let mut addr = addr
                        .parse::<Multiaddr>()
                        .expect("see test bootstrap_nodes_are_multiaddr_with_peerid");
                    let original: Multiaddr = addr.clone();
                    if self.bootstraps.insert(addr.clone()) {
                        let Some(peer_id) = addr.extract_peer_id() else {
                            continue;
                        };

                        if self
                            .custom_behaviour(swarm)
                            .add_peer((peer_id, addr.clone()))
                        {
                            trace!(peer_id=%peer_id, "tried to restore a bootstrapper");
                            // report with the peerid
                            rets.push(original);
                        }

                        let kad = swarm
                            .behaviour_mut()
                            .kademlia
                            .as_mut()
                            .expect("kad enabled");

                        kad.add_address(&peer_id, addr.clone());
                    }
                }

                let _ = ret.send(Ok(rets));
            }
            IpfsEvent::AddRelay(peer_id, addr, tx) => {
                let Some(relay) = self.custom_behaviour(swarm).relay_manager.as_mut() else {
                    let _ = tx.send(Err(anyhow::anyhow!("Relay is not enabled")));
                    return;
                };

                relay.add_address(peer_id, addr);

                let _ = tx.send(Ok(()));
            }
            IpfsEvent::RemoveRelay(peer_id, addr, tx) => {
                let Some(relay) = self.custom_behaviour(swarm).relay_manager.as_mut() else {
                    let _ = tx.send(Err(anyhow::anyhow!("Relay is not enabled")));
                    return;
                };

                relay.remove_address(peer_id, addr);

                let _ = tx.send(Ok(()));
            }
            IpfsEvent::EnableRelay(Some(peer_id), tx) => {
                let Some(relay) = self.custom_behaviour(swarm).relay_manager.as_mut() else {
                    let _ = tx.send(Err(anyhow::anyhow!("Relay is not enabled")));
                    return;
                };

                relay.select(peer_id);

                self.relay_listener.entry(peer_id).or_default().push(tx);
            }
            IpfsEvent::EnableRelay(None, tx) => {
                let Some(relay) = self.custom_behaviour(swarm).relay_manager.as_mut() else {
                    let _ = tx.send(Err(anyhow::anyhow!("Relay is not enabled")));
                    return;
                };

                let Some(peer_id) = relay.random_select() else {
                    let _ = tx.send(Err(anyhow::anyhow!(
                        "No relay was selected or was unavailable"
                    )));
                    return;
                };

                self.relay_listener.entry(peer_id).or_default().push(tx);
            }
            IpfsEvent::DisableRelay(peer_id, tx) => {
                let Some(relay) = self.custom_behaviour(swarm).relay_manager.as_mut() else {
                    let _ = tx.send(Err(anyhow::anyhow!("Relay is not enabled")));
                    return;
                };
                relay.disable_relay(peer_id);

                let _ = tx.send(Ok(()));
            }
            IpfsEvent::ListRelays(tx) => {
                let Some(relay) = self.custom_behaviour(swarm).relay_manager.as_ref() else {
                    let _ = tx.send(Err(anyhow::anyhow!("Relay is not enabled")));
                    return;
                };

                let list = relay
                    .list_relays()
                    .map(|(peer_id, addrs)| (*peer_id, addrs.clone()))
                    .collect();

                let _ = tx.send(Ok(list));
            }
            IpfsEvent::ListActiveRelays(tx) => {
                let Some(relay) = self.custom_behaviour(swarm).relay_manager.as_ref() else {
                    let _ = tx.send(Err(anyhow::anyhow!("Relay is not enabled")));
                    return;
                };

                let list = relay.list_active_relays();

                let _ = tx.send(Ok(list));
            }
        }
    }

    pub(crate) fn handle_repo_event<N: NetworkBehaviour>(
        &mut self,
        custom: &mut p2p::Behaviour<N>,
        event: RepoEvent,
    ) where
        N::ToSwarm: Debug,
    {
        match event {
            RepoEvent::WantBlock(cids, peers, timeout, signals) => {
                let Some(bs) = custom.bitswap.as_mut() else {
                    return;
                };
                if let Some(signals) = signals {
                    for (cid, signals) in signals {
                        if signals.is_empty() {
                            continue;
                        }

                        let entries = self.bitswap_cancellable.entry(cid).or_default();
                        entries.extend(signals);
                    }
                }
                bs.gets(cids, &peers, timeout);
            }
            RepoEvent::UnwantBlock(cid) => {
                let Some(bs) = custom.bitswap.as_mut() else {
                    return;
                };
                bs.cancel(cid);
                if let Some(list) = self.bitswap_cancellable.remove(&cid) {
                    for signal in list {
                        signal.notify_waiters();
                    }
                }
            }
            RepoEvent::NewBlock(block) => {
                let Some(bs) = custom.bitswap.as_mut() else {
                    return;
                };
                bs.notify_new_blocks([*block.cid()]);
            }
            RepoEvent::RemovedBlock(_) => {}
        }
    }
}
