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
//! use hedwig::{validators, Publisher, Consumer};
//! # use uuid::Uuid;
//! # use std::{path::Path, time::SystemTime};
//! # use futures_util::{sink::SinkExt, stream::StreamExt};
//! # #[cfg(not(all(feature = "protobuf", feature = "mock")))]
//! # fn main() {}
//! # #[cfg(all(feature = "protobuf", feature = "mock"))] // example uses a protobuf validator.
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!
//! #[derive(Clone, PartialEq, Eq, prost::Message)]
//! struct UserCreatedMessage {
//!     #[prost(string, tag = "1")]
//!     user_id: String,
//! }
//!
//! impl<'a> hedwig::EncodableMessage for UserCreatedMessage {
//!     type Error = validators::ProstValidatorError;
//!     type Validator = validators::ProstValidator;
//!     fn topic(&self) -> hedwig::Topic {
//!         "user.created".into()
//!     }
//!     fn encode(self, validator: &Self::Validator) -> Result<hedwig::ValidatedMessage, Self::Error> {
//!         Ok(validator.validate(
//!             uuid::Uuid::new_v4(),
//!             SystemTime::now(),
//!             "user.created/1.0",
//!             Default::default(),
//!             &self,
//!         )?)
//!     }
//! }
//!
//! impl hedwig::DecodableMessage for UserCreatedMessage {
//!     type Error = validators::ProstDecodeError<validators::prost::SchemaMismatchError>;
//!     type Decoder =
//!         validators::ProstDecoder<validators::prost::ExactSchemaMatcher<UserCreatedMessage>>;
//!
//!     fn decode(msg: hedwig::ValidatedMessage, decoder: &Self::Decoder) -> Result<Self, Self::Error> {
//!         decoder.decode(msg)
//!     }
//! }
//!
//!
//! let publisher = /* Some publisher */
//! # hedwig::mock::MockPublisher::new();
//! let consumer = /* Consumer associated to that publisher */
//! # publisher.new_consumer("user.created", "example_subscription");
//!
//! let mut publish_sink = publisher.publish_sink(validators::ProstValidator::new());
//! let mut consumer_stream = consumer.consume::<UserCreatedMessage>(
//!     validators::ProstDecoder::new(validators::prost::ExactSchemaMatcher::new("user.created/1.0")),
//! );
//!
//! publish_sink.send(UserCreatedMessage { user_id: String::from("U_123") }).await?;
//!
//! assert_eq!(
//!     "U_123",
//!     consumer_stream.next().await.unwrap()?.ack().await?.user_id
//! );
//!
//! # Ok(())
//! # }
//! ```
#![cfg_attr(docsrs, feature(doc_cfg))]

use std::{borrow::Cow, collections::BTreeMap, time::SystemTime};
pub use topic::Topic;

use bytes::Bytes;
use uuid::Uuid;

mod backends;
mod consumer;
mod publisher;
mod tests;
mod topic;
pub mod validators;

pub use backends::*;
pub use consumer::*;
pub use publisher::*;

// TODO make these public somewhere?
pub(crate) const HEDWIG_ID: &str = "hedwig_id";
pub(crate) const HEDWIG_MESSAGE_TIMESTAMP: &str = "hedwig_message_timestamp";
pub(crate) const HEDWIG_SCHEMA: &str = "hedwig_schema";
pub(crate) const HEDWIG_PUBLISHER: &str = "hedwig_publisher";
pub(crate) const HEDWIG_FORMAT_VERSION: &str = "hedwig_format_version";

/// All errors that may be returned when operating top level APIs.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// Unable to encode message payload
    #[error("Unable to encode message payload")]
    EncodeMessage(#[source] Box<dyn std::error::Error + Send + Sync>),
}

/// Custom headers associated with a message.
pub type Headers = BTreeMap<String, String>;

/// A validated message.
///
/// These are created by validators after encoding a user message, or when pulling messages from
/// the message service.
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
    schema: Cow<'static, str>,
    /// Custom message headers.
    ///
    /// This may be used to track request_id, for example.
    headers: Headers,
    /// The encoded message data.
    data: Bytes,
}

impl ValidatedMessage {
    /// Create a new validated message
    pub fn new<S, D>(id: Uuid, timestamp: SystemTime, schema: S, headers: Headers, data: D) -> Self
    where
        S: Into<Cow<'static, str>>,
        D: Into<Bytes>,
    {
        Self {
            id,
            timestamp,
            schema: schema.into(),
            headers,
            data: data.into(),
        }
    }

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

    /// Mutable access to the message headers
    pub fn headers_mut(&mut self) -> &mut Headers {
        &mut self.headers
    }

    /// The encoded message data.
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    /// Destructure this message into just the contained data
    pub fn into_data(self) -> Bytes {
        self.data
    }
}
