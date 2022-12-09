use futures::channel::mpsc as channel;
use futures::stream::{FusedStream, Stream};
use libp2p::gossipsub::error::PublishError;
use libp2p::identity::Keypair;
use std::collections::HashMap;
use std::fmt;
use std::pin::Pin;
use std::task::{Context, Poll};
use tracing::{debug, warn};

use libp2p::core::{
    connection::{ConnectedPoint, ConnectionId},
    transport::ListenerId,
    Multiaddr, PeerId,
};

use libp2p::gossipsub::{
    self, Gossipsub, GossipsubEvent, GossipsubMessage, IdentTopic as Topic, MessageAuthenticity,
    MessageId, TopicHash,
};
use libp2p::swarm::{
    ConnectionHandler, DialError, NetworkBehaviour, NetworkBehaviourAction, PollParameters,
};

/// Currently a thin wrapper around Gossipsub.
/// Allows single subscription to a topic with only unbounded senders. Tracks the peers subscribed
/// to different topics.
pub struct GossipsubStream {
    // Tracks the topic subscriptions.
    streams: HashMap<TopicHash, channel::UnboundedSender<GossipsubMessage>>,

    // Gossipsub protocol
    gossipsub: Gossipsub,

    // the subscription streams implement Drop and will send out their topic through the
    // sender cloned from here if they are dropped before the stream has ended.
    unsubscriptions: (
        channel::UnboundedSender<TopicHash>,
        channel::UnboundedReceiver<TopicHash>,
    ),
}

impl core::ops::Deref for GossipsubStream {
    type Target = Gossipsub;
    fn deref(&self) -> &Self::Target {
        &self.gossipsub
    }
}

impl core::ops::DerefMut for GossipsubStream {
    fn deref_mut(&mut self) -> &mut Gossipsub {
        &mut self.gossipsub
    }
}

/// Stream of a pubsub messages. Implements [`FusedStream`].
pub struct SubscriptionStream {
    on_drop: Option<channel::UnboundedSender<TopicHash>>,
    topic: Option<TopicHash>,
    inner: channel::UnboundedReceiver<GossipsubMessage>,
}

impl Drop for SubscriptionStream {
    fn drop(&mut self) {
        // the on_drop option allows us to disable this unsubscribe on drop once the stream has
        // ended.
        if let Some(sender) = self.on_drop.take() {
            if let Some(topic) = self.topic.take() {
                let _ = sender.unbounded_send(topic);
            }
        }
    }
}

impl fmt::Debug for SubscriptionStream {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        if let Some(topic) = self.topic.as_ref() {
            write!(
                fmt,
                "SubscriptionStream {{ topic: {:?}, is_terminated: {} }}",
                topic,
                self.is_terminated()
            )
        } else {
            write!(
                fmt,
                "SubscriptionStream {{ is_terminated: {} }}",
                self.is_terminated()
            )
        }
    }
}

impl Stream for SubscriptionStream {
    type Item = GossipsubMessage;

    fn poll_next(mut self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Option<Self::Item>> {
        use futures::stream::StreamExt;
        let inner = &mut self.as_mut().inner;
        match inner.poll_next_unpin(ctx) {
            Poll::Ready(None) => {
                // no need to unsubscribe on drop as the stream has already ended, likely via
                // unsubscribe call.
                self.on_drop.take();
                Poll::Ready(None)
            }
            other => other,
        }
    }
}

impl FusedStream for SubscriptionStream {
    fn is_terminated(&self) -> bool {
        self.on_drop.is_none()
    }
}

impl From<Gossipsub> for GossipsubStream {
    fn from(gossipsub: Gossipsub) -> Self {
        let (tx, rx) = channel::unbounded();
        GossipsubStream {
            streams: HashMap::new(),
            gossipsub,
            unsubscriptions: (tx, rx),
        }
    }
}

impl GossipsubStream {
    /// Delegates the `peer_id` over to [`Gossipsub`] and internally only does accounting on
    /// top of the gossip.
    pub fn new(keypair: Keypair) -> anyhow::Result<Self> {
        let (tx, rx) = channel::unbounded();
        let config = gossipsub::GossipsubConfigBuilder::default()
            .build()
            .map_err(|e| anyhow::anyhow!("{}", e))?;

        Ok(GossipsubStream {
            streams: HashMap::new(),
            gossipsub: Gossipsub::new(MessageAuthenticity::Signed(keypair), config)
                .map_err(|e| anyhow::anyhow!("{}", e))?,
            unsubscriptions: (tx, rx),
        })
    }

    /// Subscribes to a currently unsubscribed topic.
    /// Returns a receiver for messages sent to the topic or `None` if subscription existed
    /// already.
    pub fn subscribe(&mut self, topic: impl Into<String>) -> anyhow::Result<SubscriptionStream> {
        use std::collections::hash_map::Entry;
        let topic = Topic::new(topic);

        match self.streams.entry(topic.hash()) {
            Entry::Vacant(ve) => {
                match self.gossipsub.subscribe(&topic) {
                    Ok(true) => {
                        // TODO: this could also be bounded; we could send the message and drop the
                        // subscription if it ever became full.
                        let (tx, rx) = channel::unbounded();
                        let key = ve.key().clone();
                        ve.insert(tx);
                        Ok(SubscriptionStream {
                            on_drop: Some(self.unsubscriptions.0.clone()),
                            topic: Some(key),
                            inner: rx,
                        })
                    }
                    Ok(false) => anyhow::bail!("Already subscribed to topic"),
                    Err(e) => {
                        debug!("{}", e); //"subscribing to a unsubscribed topic should have succeeded"
                        Err(anyhow::Error::from(e))
                    }
                }
            }
            Entry::Occupied(_) => anyhow::bail!("Already subscribed to topic"),
        }
    }

    /// Unsubscribes from a topic. Unsubscription is usually done through dropping the
    /// SubscriptionStream.
    ///
    /// Returns true if an existing subscription was dropped, false otherwise
    pub fn unsubscribe(&mut self, topic: impl Into<String>) -> anyhow::Result<bool> {
        let topic = Topic::new(topic.into());
        if self.streams.remove(&topic.hash()).is_some() {
            Ok(self.gossipsub.unsubscribe(&topic)?)
        } else {
            anyhow::bail!("Unable to unsubscribe from topic.")
        }
    }

    /// Publish to subscribed topic
    pub fn publish(
        &mut self,
        topic: impl Into<String>,
        data: impl Into<Vec<u8>>,
    ) -> Result<MessageId, PublishError> {
        self.gossipsub.publish(Topic::new(topic), data)
    }

    /// Returns the known peers subscribed to any topic
    pub fn known_peers(&self) -> Vec<PeerId> {
        self.all_peers().map(|(peer, _)| *peer).collect()
    }

    /// Returns the peers known to subscribe to the given topic
    pub fn subscribed_peers(&self, topic: &str) -> Vec<PeerId> {
        let topic = Topic::new(topic);
        self.all_peers()
            .filter(|(_, list)| list.contains(&&topic.hash()))
            .map(|(peer_id, _)| *peer_id)
            .collect()
    }

    /// Returns the list of currently subscribed topics. This can contain topics for which stream
    /// has been dropped but no messages have yet been received on the topics after the drop.
    pub fn subscribed_topics(&self) -> Vec<String> {
        self.streams
            .keys()
            .into_iter()
            .map(|t| t.to_string())
            .collect()
    }
}

type GossipsubNetworkBehaviourAction = NetworkBehaviourAction<
    <Gossipsub as NetworkBehaviour>::OutEvent,
    <GossipsubStream as NetworkBehaviour>::ConnectionHandler,
    <<GossipsubStream as NetworkBehaviour>::ConnectionHandler as ConnectionHandler>::InEvent,
>;

#[allow(deprecated)]
//TODO: Remove deprecated functions
impl NetworkBehaviour for GossipsubStream {
    type ConnectionHandler = <Gossipsub as NetworkBehaviour>::ConnectionHandler;
    type OutEvent = GossipsubEvent;

    fn new_handler(&mut self) -> Self::ConnectionHandler {
        self.gossipsub.new_handler()
    }

    fn addresses_of_peer(&mut self, peer_id: &PeerId) -> Vec<Multiaddr> {
        self.gossipsub.addresses_of_peer(peer_id)
    }

    fn inject_connection_established(
        &mut self,
        peer_id: &PeerId,
        connection_id: &ConnectionId,
        endpoint: &ConnectedPoint,
        failed_addresses: Option<&Vec<Multiaddr>>,
        other_established: usize,
    ) {
        self.gossipsub.inject_connection_established(
            peer_id,
            connection_id,
            endpoint,
            failed_addresses,
            other_established,
        )
    }

    fn inject_connection_closed(
        &mut self,
        peer_id: &PeerId,
        connection_id: &ConnectionId,
        endpoint: &ConnectedPoint,
        handler: Self::ConnectionHandler,
        remaining_established: usize,
    ) {
        self.gossipsub.inject_connection_closed(
            peer_id,
            connection_id,
            endpoint,
            handler,
            remaining_established,
        )
    }

    fn inject_event(
        &mut self,
        peer_id: PeerId,
        connection: ConnectionId,
        event: <Self::ConnectionHandler as ConnectionHandler>::OutEvent,
    ) {
        self.gossipsub.inject_event(peer_id, connection, event)
    }

    fn inject_dial_failure(
        &mut self,
        peer_id: Option<PeerId>,
        handler: Self::ConnectionHandler,
        error: &DialError,
    ) {
        self.gossipsub.inject_dial_failure(peer_id, handler, error)
    }

    fn inject_new_listen_addr(&mut self, id: ListenerId, addr: &Multiaddr) {
        self.gossipsub.inject_new_listen_addr(id, addr)
    }

    fn inject_expired_listen_addr(&mut self, id: ListenerId, addr: &Multiaddr) {
        self.gossipsub.inject_expired_listen_addr(id, addr)
    }

    fn inject_new_external_addr(&mut self, addr: &Multiaddr) {
        self.gossipsub.inject_new_external_addr(addr)
    }

    fn inject_listener_error(&mut self, id: ListenerId, err: &(dyn std::error::Error + 'static)) {
        self.gossipsub.inject_listener_error(id, err)
    }

    fn inject_address_change(
        &mut self,
        peer: &PeerId,
        id: &ConnectionId,
        old: &ConnectedPoint,
        new: &ConnectedPoint,
    ) {
        self.gossipsub.inject_address_change(peer, id, old, new)
    }

    fn inject_listen_failure(
        &mut self,
        local_addr: &Multiaddr,
        send_back_addr: &Multiaddr,
        handler: Self::ConnectionHandler,
    ) {
        self.gossipsub
            .inject_listen_failure(local_addr, send_back_addr, handler)
    }

    fn inject_new_listener(&mut self, id: ListenerId) {
        self.gossipsub.inject_new_listener(id)
    }

    fn inject_listener_closed(&mut self, id: ListenerId, reason: Result<(), &std::io::Error>) {
        self.gossipsub.inject_listener_closed(id, reason)
    }

    fn inject_expired_external_addr(&mut self, addr: &Multiaddr) {
        self.gossipsub.inject_expired_external_addr(addr)
    }

    fn poll(
        &mut self,
        ctx: &mut Context,
        poll: &mut impl PollParameters,
    ) -> Poll<GossipsubNetworkBehaviourAction> {
        use futures::stream::StreamExt;
        use std::collections::hash_map::Entry;

        loop {
            match self.unsubscriptions.1.poll_next_unpin(ctx) {
                Poll::Ready(Some(dropped)) => {
                    if self.streams.remove(&dropped).is_some() {
                        debug!("unsubscribing via drop from {:?}", dropped);
                        assert!(
                            self.gossipsub
                                .unsubscribe(&Topic::new(dropped.to_string()))
                                .unwrap_or_default(),
                            "Failed to unsubscribe a dropped subscription"
                        );
                    }
                }
                Poll::Ready(None) => unreachable!("we own the sender"),
                Poll::Pending => break,
            }
        }

        loop {
            match futures::ready!(self.gossipsub.poll(ctx, poll)) {
                NetworkBehaviourAction::GenerateEvent(GossipsubEvent::Message {
                    message, ..
                }) => {
                    let topic = message.topic.clone();
                    if let Entry::Occupied(oe) = self.streams.entry(topic) {
                        if let Err(se) = oe.get().unbounded_send(message) {
                            // receiver has dropped
                            let (topic, _) = oe.remove_entry();
                            debug!("unsubscribing via SendError from {:?}", &topic);
                            assert!(
                                self.gossipsub
                                    .unsubscribe(&Topic::new(topic.to_string()))
                                    .unwrap_or_default(),
                                "Failed to unsubscribe following SendError"
                            );
                            let _ = Some(se.into_inner());
                        }
                    }
                    continue;
                }
                NetworkBehaviourAction::GenerateEvent(GossipsubEvent::Subscribed {
                    peer_id,
                    topic,
                }) => {
                    if self
                        .subscribed_peers(&topic.to_string())
                        .contains(&peer_id)
                    {
                        warn!("Peer is already subscribed to {}", topic);
                        continue;
                    }

                    self.add_explicit_peer(&peer_id);
                    continue;
                }
                NetworkBehaviourAction::GenerateEvent(GossipsubEvent::Unsubscribed {
                    peer_id,
                    topic,
                }) => {
                    if !self
                        .subscribed_peers(&topic.to_string())
                        .contains(&peer_id)
                    {
                        warn!("Peer is not subscribed to {}", topic);
                        continue;
                    };

                    self.remove_explicit_peer(&peer_id);

                    continue;
                }
                NetworkBehaviourAction::GenerateEvent(GossipsubEvent::GossipsubNotSupported {
                    peer_id,
                }) => {
                    warn!("Not supported for {}", peer_id);
                    continue;
                }
                action @ NetworkBehaviourAction::Dial { .. } => {
                    return Poll::Ready(action);
                }
                NetworkBehaviourAction::NotifyHandler {
                    peer_id,
                    event,
                    handler,
                } => {
                    return Poll::Ready(NetworkBehaviourAction::NotifyHandler {
                        peer_id,
                        event,
                        handler,
                    });
                }
                NetworkBehaviourAction::ReportObservedAddr { address, score } => {
                    return Poll::Ready(NetworkBehaviourAction::ReportObservedAddr {
                        address,
                        score,
                    });
                }
                NetworkBehaviourAction::CloseConnection {
                    peer_id,
                    connection,
                } => {
                    return Poll::Ready(NetworkBehaviourAction::CloseConnection {
                        peer_id,
                        connection,
                    });
                }
            }
        }
    }
}
