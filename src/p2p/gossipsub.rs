use futures::channel::mpsc::{self as channel};
use futures::stream::{FusedStream, Stream};
use libp2p::gossipsub::PublishError;
use std::collections::HashMap;
use std::fmt;
use std::pin::Pin;
use std::task::{Context, Poll};
use tracing::debug;

use libp2p::core::{Endpoint, Multiaddr};
use libp2p::identity::PeerId;

use libp2p::gossipsub::{
    Behaviour as Gossipsub, Event as GossipsubEvent, IdentTopic as Topic,
    Message as GossipsubMessage, MessageId, TopicHash,
};
use libp2p::swarm::{
    ConnectionDenied, ConnectionId, NetworkBehaviour, THandler, THandlerInEvent, ToSwarm,
};

/// Currently a thin wrapper around Gossipsub.
/// Allows single subscription to a topic with only unbounded senders. Tracks the peers subscribed
/// to different topics.
pub struct GossipsubStream {
    // Tracks the topic subscriptions.
    streams: HashMap<TopicHash, futures::channel::mpsc::Sender<GossipsubMessage>>,

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
    inner: futures::channel::mpsc::Receiver<GossipsubMessage>,
}

impl Drop for SubscriptionStream {
    fn drop(&mut self) {
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
    /// Subscribes to a currently unsubscribed topic.
    /// Returns a receiver for messages sent to the topic or `None` if subscription existed
    /// already.
    pub fn subscribe(&mut self, topic: impl Into<String>) -> anyhow::Result<SubscriptionStream> {
        let topic = Topic::new(topic);

        if self.streams.contains_key(&topic.hash()) {
            anyhow::bail!("Already subscribed to topic")
        }

        if !self.gossipsub.subscribe(&topic)? {
            anyhow::bail!("Already subscribed to topic")
        }

        let (tx, rx) = futures::channel::mpsc::channel(15000);
        self.streams.insert(topic.hash(), tx);
        Ok(SubscriptionStream {
            on_drop: Some(self.unsubscriptions.0.clone()),
            topic: Some(topic.hash()),
            inner: rx,
        })
    }

    /// Unsubscribes from a topic. Unsubscription is usually done through dropping the
    /// SubscriptionStream.
    ///
    /// Returns true if an existing subscription was dropped, false otherwise
    pub fn unsubscribe(&mut self, topic: impl Into<String>) -> anyhow::Result<bool> {
        let topic = Topic::new(topic);

        if !self.streams.contains_key(&topic.hash()) {
            anyhow::bail!("Unable to unsubscribe from topic.")
        }

        self.streams
            .remove(&topic.hash())
            .expect("subscribed to topic");

        self.gossipsub
            .unsubscribe(&topic)
            .map_err(anyhow::Error::from)
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
    pub fn subscribed_peers(&self, topic: impl Into<String>) -> Vec<PeerId> {
        let topic = Topic::new(topic);
        self.all_peers()
            .filter(|(_, list)| list.contains(&&topic.hash()))
            .map(|(peer_id, _)| *peer_id)
            .collect()
    }
}

impl NetworkBehaviour for GossipsubStream {
    type ConnectionHandler = <Gossipsub as NetworkBehaviour>::ConnectionHandler;
    type ToSwarm = GossipsubEvent;

    fn handle_pending_outbound_connection(
        &mut self,
        connection_id: ConnectionId,
        maybe_peer: Option<PeerId>,
        addresses: &[Multiaddr],
        effective_role: Endpoint,
    ) -> Result<Vec<Multiaddr>, ConnectionDenied> {
        self.gossipsub.handle_pending_outbound_connection(
            connection_id,
            maybe_peer,
            addresses,
            effective_role,
        )
    }

    fn on_swarm_event(&mut self, event: libp2p::swarm::FromSwarm) {
        self.gossipsub.on_swarm_event(event)
    }

    fn on_connection_handler_event(
        &mut self,
        peer_id: PeerId,
        connection_id: libp2p::swarm::ConnectionId,
        event: libp2p::swarm::THandlerOutEvent<Self>,
    ) {
        self.gossipsub
            .on_connection_handler_event(peer_id, connection_id, event)
    }

    fn handle_established_inbound_connection(
        &mut self,
        connection_id: ConnectionId,
        peer: PeerId,
        local_addr: &Multiaddr,
        remote_addr: &Multiaddr,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        self.gossipsub.handle_established_inbound_connection(
            connection_id,
            peer,
            local_addr,
            remote_addr,
        )
    }

    fn handle_established_outbound_connection(
        &mut self,
        connection_id: ConnectionId,
        peer: PeerId,
        addr: &Multiaddr,
        role_override: Endpoint,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        self.gossipsub.handle_established_outbound_connection(
            connection_id,
            peer,
            addr,
            role_override,
        )
    }

    fn poll(
        &mut self,
        ctx: &mut Context,
    ) -> Poll<ToSwarm<libp2p::gossipsub::Event, THandlerInEvent<Self>>> {
        use futures::stream::StreamExt;
        use std::collections::hash_map::Entry;

        loop {
            match self.unsubscriptions.1.poll_next_unpin(ctx) {
                Poll::Ready(Some(dropped)) => {
                    if let Some(mut sender) = self.streams.remove(&dropped) {
                        sender.close_channel();
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
            match futures::ready!(self.gossipsub.poll(ctx)) {
                ToSwarm::GenerateEvent(GossipsubEvent::Message { message, .. }) => {
                    let topic = message.topic.clone();
                    if let Entry::Occupied(mut oe) = self.streams.entry(topic) {
                        if let Err(e) = oe.get_mut().try_send(message) {
                            if e.is_full() {
                                continue;
                            }
                            // receiver has dropped
                            let (topic, _) = oe.remove_entry();
                            debug!("unsubscribing via SendError from {:?}", &topic);
                            assert!(
                                self.gossipsub
                                    .unsubscribe(&Topic::new(topic.to_string()))
                                    .unwrap_or_default(),
                                "Failed to unsubscribe following SendError"
                            );
                        }
                    }
                    continue;
                }
                action => {
                    return Poll::Ready(action);
                }
            }
        }
    }
}
