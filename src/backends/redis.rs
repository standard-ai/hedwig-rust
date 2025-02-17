use crate::{consumer::AcknowledgeableMessage, EncodableMessage, Topic, ValidatedMessage};
use async_channel as mpmc;
use futures_util::{
    sink,
    stream::{self, StreamExt},
};
use parking_lot::Mutex;
use pin_project::pin_project;
use std::{
    collections::BTreeMap,
    error::Error as StdError,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

/// Errors originating from Redis publisher and consumer operations
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct Error {
    /// The underlying source of the error
    pub cause: Box<dyn StdError>,
}

impl Error {
    fn from<E>(from: E) -> Self
    where
        Box<dyn StdError>: From<E>,
    {
        Self { cause: from.into() }
    }
}

type Topics = BTreeMap<Topic, Subscriptions>;
type Subscriptions = BTreeMap<RedisSubscription, Channel<ValidatedMessage>>;

#[derive(Debug, Clone)]
pub struct RedisPublisher {
    topics: Arc<Mutex<Topics>>,
}

impl RedisPublisher {
    pub fn new() -> Self {
        RedisPublisher {
            topics: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub fn new_consumer(
        &self,
        topic: impl Into<Topic>,
        subscription: impl Into<RedisSubscription>,
    ) -> RedisConsumer {
        let mut topics = self.topics.lock();
        let subscriptions = topics.entry(topic.into()).or_default();

        let channel = subscriptions
            .entry(subscription.into())
            .or_insert_with(|| {
                let (sender, receiver) = mpmc::unbounded();
                Channel { sender, receiver }
            })
            .clone();

        RedisConsumer {
            subscription_messages: channel.receiver,
            subscription_resend: channel.sender,
        }
    }
}

impl Default for RedisPublisher {
    fn default() -> Self {
        Self::new()
    }
}

impl<M, S> crate::Publisher<M, S> for RedisPublisher
where
    M: crate::EncodableMessage,
    M::Error: StdError + 'static,
    S: sink::Sink<M>,
    S::Error: StdError + 'static,
{
    type PublishError = Error;
    type PublishSink = RedisSink<M, S>;

    fn publish_sink_with_responses(
        self,
        validator: M::Validator,
        response_sink: S,
    ) -> Self::PublishSink {
        RedisSink {
            topics: self.topics,
            validator,
            response_sink,
        }
    }
}

#[pin_project]
#[derive(Debug)]
pub struct RedisSink<M: EncodableMessage, S> {
    topics: Arc<Mutex<Topics>>,
    validator: M::Validator,
    #[pin]
    response_sink: S,
}

#[derive(Debug, Clone)]
struct Channel<T> {
    sender: mpmc::Sender<T>,
    receiver: mpmc::Receiver<T>,
}

impl<M, S> sink::Sink<M> for RedisSink<M, S>
where
    M: EncodableMessage,
    M::Error: StdError + 'static,
    S: sink::Sink<M>,
    S::Error: StdError + 'static,
{
    type Error = Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        self.project()
            .response_sink
            .poll_ready(cx)
            .map_err(Error::from)
    }

    fn start_send(self: Pin<&mut Self>, message: M) -> Result<(), Self::Error> {
        let this = self.project();

        let topic = message.topic();
        let validated_message = message.encode(this.validator).map_err(Error::from)?;

        // lock critical section
        {
            let mut topics = this.topics.lock();

            // send the message to every subscription listening on the given topic

            // find the subscriptions for this topic
            let subscriptions = topics.entry(topic).or_default();

            // Send to every subscription that still has consumers. If a subscription's consumers are
            // all dropped, the channel will have been closed and should be removed from the list
            subscriptions.retain(|_subscription_name, channel| {
                match channel.sender.try_send(validated_message.clone()) {
                    // if successfully sent, retain the channel
                    Ok(()) => true,
                    // if the channel has disconnected due to drops, remove it from the list
                    Err(mpmc::TrySendError::Closed(_)) => false,
                    Err(mpmc::TrySendError::Full(_)) => {
                        unreachable!("unbounded channel should never be full")
                    }
                }
            });
        }

        // notify the caller that the message has been sent successfully
        this.response_sink
            .start_send(message)
            .map_err(Error::from)?;

        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        self.project()
            .response_sink
            .poll_flush(cx)
            .map_err(Error::from)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        self.project()
            .response_sink
            .poll_close(cx)
            .map_err(Error::from)
    }
}

/// An opaque identifier for individual subscriptions to a [`RedisPublisher`]
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RedisSubscription(String);

impl<S> From<S> for RedisSubscription
where
    S: Into<String>,
{
    fn from(string: S) -> Self {
        RedisSubscription(string.into())
    }
}

/// A consumer for messages from a particular subscription to a [`RedisPublisher`]
#[derive(Debug, Clone)]
pub struct RedisConsumer {
    // channel receiver to get messages from the subscription
    subscription_messages: mpmc::Receiver<ValidatedMessage>,

    // channel sender to resend messages to the subscription on nack
    subscription_resend: mpmc::Sender<ValidatedMessage>,
}

impl crate::Consumer for RedisConsumer {
    type AckToken = RedisAckToken;
    type Error = Error;
    type Stream = Self;

    fn stream(self) -> Self::Stream {
        self
    }
}

impl stream::Stream for RedisConsumer {
    type Item = Result<AcknowledgeableMessage<RedisAckToken, ValidatedMessage>, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        self.subscription_messages
            .poll_next_unpin(cx)
            .map(|opt_message| {
                opt_message.map(|message| {
                    Ok(AcknowledgeableMessage {
                        ack_token: RedisAckToken {
                            message: message.clone(),
                            subscription_resend: self.subscription_resend.clone(),
                        },
                        message,
                    })
                })
            })
    }
}

/// An acknowledge token associated with a particular message from a [`RedisConsumer`].
///
/// When `nack` is called for a particular message's token, that message will be re-submitted to
/// consumers of the corresponding subscription. Messages otherwise do not have any timeout
/// behavior, so a message is only re-sent to consumers if it is explicitly nack'ed; `ack` and
/// `modify_deadline` have no effect
#[derive(Debug)]
pub struct RedisAckToken {
    message: ValidatedMessage,
    subscription_resend: mpmc::Sender<ValidatedMessage>,
}

#[async_trait::async_trait]
impl crate::consumer::AcknowledgeToken for RedisAckToken {
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
                cause: "Could not nack message because all consumers have been dropped".into(),
            })
    }

    async fn modify_deadline(&mut self, _seconds: u32) -> Result<(), Self::ModifyError> {
        // currently does nothing
        Ok(())
    }
}
