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
//! impl<'a> hedwig::publish::EncodableMessage for &'a UserCreatedMessage {
//!     type Error = hedwig::validators::JsonSchemaValidatorError;
//!     type Validator = hedwig::validators::JsonSchemaValidator;
//!     fn topic(&self) -> hedwig::Topic { "user.created".into() }
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
//! # hedwig::publish::NullPublisher;
//! let validator = hedwig::validators::JsonSchemaValidator::new(schema)?;
//! let mut batch = hedwig::publish::PublishBatch::new();
//! batch.message(&validator, &UserCreatedMessage { user_id: String::from("U_123") });
//! let mut result_stream = batch.publish(&publisher);
//! let mut next_batch = hedwig::publish::PublishBatch::new();
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
    clippy::all,
    unsafe_code,
    unreachable_pub
)]
#![allow(clippy::unknown_clippy_lints)]
#![cfg_attr(not(test), deny(unused))]
#![cfg_attr(docsrs, feature(doc_cfg))]

use std::{collections::BTreeMap, time::SystemTime};
pub use topic::Topic;
use uuid::Uuid;

#[cfg(feature = "publish")]
#[cfg_attr(docsrs, doc(cfg(feature = "publish")))]
pub mod publish;
#[cfg(test)]
mod tests;
mod topic;
pub mod validators;

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
