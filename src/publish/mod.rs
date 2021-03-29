//! Types, traits, and functions necessary to publish messages using hedwig

use crate::{Error, Topic, ValidatedMessage};
use futures_util::{
    ready,
    stream::{self, Stream},
};
use pin_project::pin_project;
use std::{
    collections::BTreeMap,
    pin::Pin,
    task::{Context, Poll},
};

mod publishers;
pub use publishers::*;

#[cfg(feature = "sink")]
#[cfg_attr(docsrs, doc(cfg(feature = "sink")))]
pub mod sink;

/// Message publishers.
///
/// Message publishers deliver a validated message to an endpoint, possibly a remote one. Message
/// publishers may also additionally validate a message for publisher-specific requirements (e.g.
/// size).
pub trait Publisher {
    /// The identifier for a successfully published message.
    type MessageId: 'static;

    /// The error that this publisher returns when publishing of a message fails.
    type MessageError: std::error::Error + Send + Sync + 'static;

    /// The stream of results that the `publish` method returns.
    type PublishStream: Stream<Item = Result<Self::MessageId, Self::MessageError>>;

    /// Publish a batch of messages.
    ///
    /// The output stream shall return a result for each message in `messages` slice in order.
    fn publish<'a, I>(&self, topic: Topic, messages: I) -> Self::PublishStream
    where
        I: Iterator<Item = &'a ValidatedMessage> + DoubleEndedIterator + ExactSizeIterator;
}

/// Types that can be encoded and published.
pub trait EncodableMessage {
    /// The errors that can occur when calling the [`EncodableMessage::encode`] method.
    ///
    /// Will typically match the errors returned by the [`EncodableMessage::Validator`].
    type Error: std::error::Error + Send + Sync + 'static;

    /// The validator to use for this message.
    type Validator;

    /// Topic into which this message shall be published.
    fn topic(&self) -> Topic;

    /// Encode the message payload.
    fn encode(self, validator: &Self::Validator) -> Result<ValidatedMessage, Self::Error>;
}

/// A convenience builder for publishing in batches.
#[derive(Default, Debug)]
pub struct PublishBatch {
    messages: BTreeMap<Topic, Vec<ValidatedMessage>>,
}

impl PublishBatch {
    /// Construct a new batch.
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of messages currently queued.
    pub fn len(&self) -> usize {
        self.messages.iter().fold(0, |acc, (_, v)| acc + v.len())
    }

    /// Whether the batch is empty.
    pub fn is_empty(&self) -> bool {
        self.messages.iter().all(|(_, v)| v.is_empty())
    }

    /// Add an already validated message to be published in this batch.
    pub fn push(&mut self, topic: Topic, validated: ValidatedMessage) -> &mut Self {
        self.messages.entry(topic).or_default().push(validated);
        self
    }

    /// Validate and add a message to be published in this batch.
    pub fn message<M: EncodableMessage>(
        &mut self,
        validator: &M::Validator,
        msg: M,
    ) -> Result<&mut Self, Error> {
        let topic = msg.topic();
        let validated = msg
            .encode(validator)
            .map_err(|e| Error::EncodeMessage(e.into()))?;
        Ok(self.push(topic, validated))
    }

    /// Publish all the enqueued messages, batching them for high efficiency.
    ///
    /// The order in which messages were added to the batch and the order of messages as seen by
    /// the publisher is not strictly preserved. As thus, the output stream will not preserve the
    /// message ordering either.
    ///
    /// Some kinds of errors that occur during publishing may not be transient. An example of such
    /// an error is attempting to publish a too large message with the `GooglePubSubPublisher`.
    /// For
    /// errors like these retrying is most likely incorrect as they would just fail again.
    /// Publisher-specific error types may have methods to make a decision easier.
    pub fn publish<P>(self, publisher: &P) -> PublishBatchStream<P::PublishStream>
    where
        P: Publisher,
        P::PublishStream: Unpin,
    {
        PublishBatchStream(
            self.messages
                .into_iter()
                .map(|(topic, msgs)| TopicPublishStream::new(topic, msgs, publisher))
                .collect::<stream::SelectAll<_>>(),
        )
    }
}

/// The stream returned by the method [`PublishBatch::publish`](PublishBatch::publish)
// This stream and TopicPublishStream are made explicit types instead of combinators like
// map/zip/etc so that callers can refer to a concrete return type instead of `impl Stream`
#[pin_project]
#[derive(Debug)]
pub struct PublishBatchStream<P>(#[pin] stream::SelectAll<TopicPublishStream<P>>);

impl<P> Stream for PublishBatchStream<P>
where
    P: Stream + Unpin,
{
    type Item = (P::Item, Topic, ValidatedMessage);

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        self.project().0.poll_next(cx)
    }
}

#[pin_project]
#[derive(Debug)]
struct TopicPublishStream<P> {
    topic: Topic,
    messages: std::vec::IntoIter<ValidatedMessage>,

    #[pin]
    publish_stream: P,
}

impl<P> TopicPublishStream<P> {
    fn new<Pub>(topic: Topic, messages: Vec<ValidatedMessage>, publisher: &Pub) -> Self
    where
        Pub: Publisher<PublishStream = P>,
        P: Stream<Item = Result<Pub::MessageId, Pub::MessageError>>,
    {
        let publish_stream = publisher.publish(topic, messages.iter());
        Self {
            topic,
            messages: messages.into_iter(),
            publish_stream,
        }
    }
}

impl<P> Stream for TopicPublishStream<P>
where
    P: Stream,
{
    type Item = (P::Item, Topic, ValidatedMessage);

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let this = self.project();

        // `map` has lifetime constraints that aren't nice here
        #[allow(clippy::manual_map)]
        Poll::Ready(match ready!(this.publish_stream.poll_next(cx)) {
            None => None,
            Some(stream_item) => Some((
                stream_item,
                *this.topic,
                this.messages
                    .next()
                    .expect("should be as many messages as publishes"),
            )),
        })
    }
}
