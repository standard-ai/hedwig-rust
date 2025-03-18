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
//!     fn encode(&self, validator: &Self::Validator) -> Result<hedwig::ValidatedMessage, Self::Error> {
//!         Ok(validator.validate(
//!             uuid::Uuid::new_v4(),
//!             SystemTime::now(),
//!             "user.created/1.0",
//!             Default::default(),
//!             self,
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
//! let mut publish_sink = Publisher::<UserCreatedMessage>::publish_sink(publisher, validators::ProstValidator::new());
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

pub use hedwig_core::{message, Headers, Topic, ValidatedMessage};

mod backends;
mod consumer;
mod publisher;
mod tests;
pub mod validators;

#[allow(unused_imports)]
pub use backends::*;

pub use consumer::*;
pub use publisher::*;

// TODO make these public somewhere?
#[cfg(feature = "google")]
pub(crate) const HEDWIG_ID: &str = "hedwig_id";
#[cfg(feature = "google")]
pub(crate) const HEDWIG_MESSAGE_TIMESTAMP: &str = "hedwig_message_timestamp";
#[cfg(feature = "google")]
pub(crate) const HEDWIG_SCHEMA: &str = "hedwig_schema";
#[cfg(feature = "google")]
pub(crate) const HEDWIG_PUBLISHER: &str = "hedwig_publisher";
#[cfg(feature = "google")]
pub(crate) const HEDWIG_FORMAT_VERSION: &str = "hedwig_format_version";

/// All errors that may be returned when operating top level APIs.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// Unable to encode message payload
    #[error("Unable to encode message payload")]
    EncodeMessage(#[source] Box<dyn std::error::Error + Send + Sync>),
}
