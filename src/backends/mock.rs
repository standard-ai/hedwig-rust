//! In-memory messaging implementations, meant to imitate distributed messaging services for test
//! purposes.
//!
//! See [`MockPublisher`] for an entry point to the mock system.

use crate::{consumer::AcknowledgeableMessage, Topic, ValidatedMessage};
use async_channel as mpmc;
use futures_util::{
    sink,
    stream::{self, StreamExt},
};
use std::{
    collections::BTreeMap,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

#[derive(Debug, thiserror::Error)]
#[error("{reason}")]
pub struct Error {
    reason: String,
}

/// An in-memory publisher.
///
/// Consumers for the published data can be created using the `new_consumer` method.
///
/// Messages are published to particular [`Topics`](crate::Topic). Each topic may have multiple
/// `Subscriptions`, and every message for a topic will be sent to each of its subscriptions. A
/// subscription, in turn, may have multiple consumers; consumers will take messages from the
/// subscription on a first-polled-first-served basis.
///
/// This publisher can be cloned, allowing multiple publishers to send messages to the same set of
/// topics and subscriptions. Any consumer created with `new_consumer` will receive all on-topic
/// and on-subscription messages from all the associated publishers, regardless of whether the
/// consumer was created from a cloned instance.
#[derive(Debug, Clone)]
pub struct MockPublisher {
    inner: MockSink,
}

impl MockPublisher {
    /// Create a new `MockPublisher`
    pub fn new() -> Self {
        MockPublisher {
            inner: MockSink {
                topics: Arc::new(Mutex::new(BTreeMap::new())),
            },
        }
    }

    /// Create a new consumer which will listen for messages published to the given topic and
    /// subscription by this publisher (or any of its clones)
    pub fn new_consumer(
        &self,
        topic: impl Into<Topic>,
        subscription: impl Into<MockSubscription>,
    ) -> MockConsumer {
        let mut topics = self.inner.topics.lock().expect("lock poisoned!");
        let subscriptions = topics.entry(topic.into()).or_insert_with(BTreeMap::new);

        let channel = subscriptions
            .entry(subscription.into())
            .or_insert_with(|| {
                let (sender, receiver) = mpmc::unbounded();
                Channel { sender, receiver }
            })
            .clone();

        MockConsumer {
            subscription_messages: channel.receiver,
            subscription_resend: channel.sender,
        }
    }
}

impl Default for MockPublisher {
    fn default() -> Self {
        Self::new()
    }
}

impl<M> crate::Publisher<M> for MockPublisher
where
    M: crate::EncodableMessage,
{
    type PublishError = Error;
    type PublishSink = crate::publisher::ValidatorSink<M, M::Validator, MockSink>;

    fn publish_sink(self, validator: M::Validator) -> Self::PublishSink {
        crate::publisher::validator_sink(validator, self.inner)
    }
}

type Subscriptions = BTreeMap<MockSubscription, Channel<ValidatedMessage>>;

/// The sink used by the `MockPublisher`
#[derive(Debug, Clone)]
pub struct MockSink {
    topics: Arc<Mutex<BTreeMap<Topic, Subscriptions>>>,
}

#[derive(Debug, Clone)]
struct Channel<T> {
    sender: mpmc::Sender<T>,
    receiver: mpmc::Receiver<T>,
}

impl sink::Sink<(Topic, ValidatedMessage)> for MockSink {
    type Error = Error;

    fn poll_ready(self: Pin<&mut Self>, _: &mut Context) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(
        self: Pin<&mut Self>,
        (topic, message): (Topic, ValidatedMessage),
    ) -> Result<(), Self::Error> {
        let mut topics = self.topics.lock().expect("lock poisoned!");

        // send the message to every subscription listening on the given topic

        // find the subscriptions for this topic
        let subscriptions = topics.entry(topic).or_insert_with(Subscriptions::new);

        // Send to every subscription that still has consumers. If a subscription's consumers are
        // all dropped, the channel will have been closed and should be removed from the list
        subscriptions.retain(|_subscription_name, channel| {
            match channel.sender.try_send(message.clone()) {
                // if successfully sent, retain the channel
                Ok(()) => true,
                // if the channel has disconnected due to drops, remove it from the list
                Err(mpmc::TrySendError::Closed(_)) => false,
                Err(mpmc::TrySendError::Full(_)) => {
                    unreachable!("unbounded channel should never be full")
                }
            }
        });

        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _: &mut Context) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

/// An opaque identifier for individual subscriptions to a [`MockPublisher`]
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MockSubscription(String);

impl<S> From<S> for MockSubscription
where
    S: Into<String>,
{
    fn from(string: S) -> Self {
        MockSubscription(string.into())
    }
}

/// A consumer for messages from a particular subscription to a [`MockPublisher`]
#[derive(Debug, Clone)]
pub struct MockConsumer {
    // channel receiver to get messages from the subscription
    subscription_messages: mpmc::Receiver<ValidatedMessage>,

    // channel sender to resend messages to the subscription on nack
    subscription_resend: mpmc::Sender<ValidatedMessage>,
}

impl crate::Consumer for MockConsumer {
    type AckToken = MockAckToken;
    type Error = Error;
    type Stream = Self;

    fn stream(self) -> Self::Stream {
        self
    }
}

impl stream::Stream for MockConsumer {
    type Item = Result<AcknowledgeableMessage<MockAckToken, ValidatedMessage>, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        self.subscription_messages
            .poll_next_unpin(cx)
            .map(|opt_message| {
                opt_message.map(|message| {
                    Ok(AcknowledgeableMessage {
                        ack_token: MockAckToken {
                            message: message.clone(),
                            subscription_resend: self.subscription_resend.clone(),
                        },
                        message,
                    })
                })
            })
    }
}

/// An acknowledge token associated with a particular message from a [`MockConsumer`].
///
/// When `nack` is called for a particular message's token, that message will be re-submitted to
/// consumers of the corresponding subscription. Messages otherwise do not have any timeout
/// behavior, so a message is only re-sent to consumers if it is explicitly nack'ed; `ack` and
/// `modify_deadline` have no effect
#[derive(Debug)]
pub struct MockAckToken {
    message: ValidatedMessage,
    subscription_resend: mpmc::Sender<ValidatedMessage>,
}

#[async_trait::async_trait]
impl crate::consumer::AcknowledgeToken for MockAckToken {
    type AckError = Error;
    type NackError = Error;
    type ModifyError = Error;

    async fn ack(self) -> Result<(), Self::AckError> {
        Ok(())
    }

    async fn nack(self) -> Result<(), Self::NackError> {
        self.subscription_resend
            .send(self.message)
            .await
            .map_err(|mpmc::SendError(_message)| Error {
                reason: "Could not nack message because all consumers have been dropped".into(),
            })
    }

    async fn modify_deadline(&mut self, _seconds: u32) -> Result<(), Self::ModifyError> {
        // currently does nothing
        Ok(())
    }
}
