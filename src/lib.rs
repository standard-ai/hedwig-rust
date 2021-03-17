//! Hedwig is a message bus library that works with arbitrary pubsub services such as AWS SNS/SQS
//! or Google Cloud Pubsub. Messages are validated before they are published. The publisher and
//! consumer are de-coupled and fan-out is supported out of the box.
//!
//! The Rust library currently only supports publishing.
//!
//! # Examples
//!
//! Publish a message. Payload encoded with JSON and validated using a JSON Schema.
//!
//! ```
//! use uuid::Uuid;
//! use std::{path::Path, time::SystemTime};
//! use futures_util::stream::StreamExt;
//!
//! # #[cfg(not(feature = "json-schema"))]
//! # fn main() {}
//!
//! # #[cfg(feature = "json-schema")] // example uses a JSON Schema validator.
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let schema = r#"{
//!     "$id": "https://hedwig.corp/schema",
//!     "$schema": "https://json-schema.org/draft-04/schema#",
//!     "description": "Example Schema",
//!     "schemas": {
//!         "user-created": {
//!             "1.*": {
//!                 "description": "A new user was created",
//!                 "type": "object",
//!                 "x-versions": [
//!                     "1.0"
//!                 ],
//!                 "required": [
//!                     "user_id"
//!                 ],
//!                 "properties": {
//!                     "user_id": {
//!                         "$ref": "https://hedwig.corp/schema#/definitions/UserId/1.0"
//!                     }
//!                 }
//!             }
//!         }
//!     },
//!     "definitions": {
//!         "UserId": {
//!             "1.0": {
//!                 "type": "string"
//!             }
//!         }
//!     }
//! }"#;
//!
//! #[derive(serde::Serialize)]
//! struct UserCreatedMessage {
//!     user_id: String,
//! }
//!
//! impl<'a> hedwig::Message for &'a UserCreatedMessage {
//!     type Error = hedwig::validators::JsonSchemaValidatorError;
//!     type Validator = hedwig::validators::JsonSchemaValidator;
//!     fn topic(&self) -> hedwig::Topic { "user.created" }
//!     fn encode(self, validator: &Self::Validator)
//!     -> Result<hedwig::ValidatedMessage, Self::Error> {
//!         validator.validate(
//!             Uuid::new_v4(),
//!             SystemTime::now(),
//!             "https://hedwig.corp/schema#/schemas/user.created/1.0",
//!             hedwig::Headers::new(),
//!             self,
//!         )
//!     }
//! }
//!
//! let publisher = /* Some publisher */
//! # hedwig::publishers::NullPublisher;
//! let validator = hedwig::validators::JsonSchemaValidator::new(schema)?;
//! let mut batch = hedwig::PublishBatch::new();
//! batch.message(&validator, &UserCreatedMessage { user_id: String::from("U_123") });
//! let mut result_stream = batch.publish(&publisher);
//! let mut next_batch = hedwig::PublishBatch::new();
//! async {
//!     while let Some(result) = result_stream.next().await {
//!         match result {
//!             (Ok(id), _, msg) => {
//!                 println!("message {} published successfully: {:?}", msg.uuid(), id);
//!             }
//!             (Err(e), topic, msg) => {
//!                 eprintln!("failed to publish {}: {}", msg.uuid(), e);
//!                 next_batch.push(topic, msg);
//!             }
//!         }
//!     }
//! };
//! # Ok(())
//! # }
//! ```
#![deny(
    missing_docs,
    broken_intra_doc_links,
    clippy::correctness,
    clippy::complexity,
    clippy::perf,
    unsafe_code,
    unreachable_pub
)]
#![cfg_attr(not(test), deny(unused))]
#![cfg_attr(docsrs, feature(doc_cfg))]

use std::{
    collections::BTreeMap,
    pin::Pin,
    task::{Context, Poll},
    time::SystemTime,
};

use futures_util::{
    ready,
    stream::{self, Stream},
};
use pin_project::pin_project;
use uuid::Uuid;

pub mod publishers;
#[cfg(test)]
mod tests;
pub mod validators;

#[cfg(feature = "sink")]
#[cfg_attr(docsrs, doc(cfg(feature = "sink")))]
pub mod sink;

/// A message queue topic name to which messages can be published
pub type Topic = &'static str;

/// All errors that may be returned when operating top level APIs.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// Unable to encode message payload
    #[error("Unable to encode message payload")]
    EncodeMessage(#[source] Box<dyn std::error::Error + Send + Sync>),
}

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
pub trait Message {
    /// The errors that can occur when calling the [`Message::encode`] method.
    ///
    /// Will typically match the errors returned by the [`Message::Validator`].
    type Error: std::error::Error + Send + Sync + 'static;

    /// The validator to use for this message.
    type Validator;

    /// Topic into which this message shall be published.
    fn topic(&self) -> Topic;

    /// Encode the message payload.
    fn encode(self, validator: &Self::Validator) -> Result<ValidatedMessage, Self::Error>;
}

/// Custom headers associated with a message.
pub type Headers = BTreeMap<String, String>;

/// A validated message.
///
/// The only way to construct this is via a validator.
#[derive(Debug, Clone)]
// derive Eq only in tests so that users can't foot-shoot an expensive == over data
#[cfg_attr(test, derive(PartialEq, Eq))]
pub struct ValidatedMessage {
    /// Unique message identifier.
    id: Uuid,
    /// The timestamp when message was created in the publishing service.
    timestamp: SystemTime,
    /// URI of the schema validating this message.
    ///
    /// E.g. `https://hedwig.domain.xyz/schemas#/schemas/user.created/1.0`
    schema: &'static str,
    /// Custom message headers.
    ///
    /// This may be used to track request_id, for example.
    headers: Headers,
    /// The encoded message data.
    data: Vec<u8>,
}

impl ValidatedMessage {
    /// Unique message identifier.
    pub fn uuid(&self) -> &Uuid {
        &self.id
    }

    /// The timestamp when message was created in the publishing service.
    pub fn timestamp(&self) -> &SystemTime {
        &self.timestamp
    }

    /// URI of the schema validating this message.
    ///
    /// E.g. `https://hedwig.domain.xyz/schemas#/schemas/user.created/1.0`
    pub fn schema(&self) -> &str {
        &self.schema
    }

    /// Custom message headers.
    ///
    /// This may be used to track request_id, for example.
    pub fn headers(&self) -> &Headers {
        &self.headers
    }

    /// The encoded message data.
    pub fn data(&self) -> &[u8] {
        &self.data
    }
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
    pub fn message<M: Message>(
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
    /// an error is attempting to publish a too large message with the [`GooglePubSubPublisher`].
    /// For
    /// errors like these retrying is most likely incorrect as they would just fail again.
    /// Publisher-specific error types may have methods to make a decision easier.
    ///
    /// [`GooglePubSubPublisher`]: publishers::GooglePubSubPublisher
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
                this.topic,
                this.messages
                    .next()
                    .expect("should be as many messages as publishes"),
            )),
        })
    }
}
