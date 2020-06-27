#![deny(
    missing_docs,
    unused_import_braces,
    unused_qualifications,
    intra_doc_link_resolution_failure,
    clippy::all
)]
//! A Hedwig library for Rust. Hedwig is a message bus that works with arbitrary pubsub services
//! such as AWS SNS/SQS or Google Cloud Pubsub. Messages are validated using a JSON schema. The
//! publisher and consumer are de-coupled and fan-out is supported out of the box.
//!
//! # Example: publish a new message
//!
//! ```no_run
//! use hedwig::{Hedwig, MajorVersion, MinorVersion, Version, Message, publishers::MockPublisher};
//! # use std::path::Path;
//! # use serde::Serialize;
//! # use strum_macros::IntoStaticStr;
//!
//! fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let schema = r#"
//!     {
//!       "$id": "https://hedwig.standard.ai/schema",
//!       "$schema": "https://json-schema.org/draft-04/schema#",
//!       "description": "Example Schema",
//!       "schemas": {
//!           "user-created": {
//!               "1.*": {
//!                   "description": "A new user was created",
//!                   "type": "object",
//!                   "x-versions": [
//!                       "1.0"
//!                   ],
//!                   "required": [
//!                       "user_id"
//!                   ],
//!                   "properties": {
//!                       "user_id": {
//!                           "$ref": "https://hedwig.standard.ai/schema#/definitions/UserId/1.0"
//!                       }
//!                   }
//!               }
//!           }
//!       },
//!       "definitions": {
//!           "UserId": {
//!               "1.0": {
//!                   "type": "string"
//!               }
//!           }
//!       }
//!     }"#;
//!
//!     # #[derive(Clone, Copy, IntoStaticStr, Hash, PartialEq, Eq)]
//!     # enum MessageType {
//!     #    #[strum(serialize = "user.created")]
//!     #    UserCreated,
//!     # }
//!     #
//!     # #[derive(Serialize)]
//!     # struct UserCreatedData {
//!     #     user_id: String,
//!     # }
//!     #
//!     fn router(t: MessageType, v: MajorVersion) -> Option<&'static str> {
//!         match (t, v) {
//!             (MessageType::UserCreated, MajorVersion(1)) => Some("dev-user-created-v1"),
//!             _ => None,
//!         }
//!     }
//!
//!     // create a publisher instance
//!     let publisher = MockPublisher::default();
//!     let hedwig = Hedwig::new(
//!         schema,
//!         "myapp",
//!         publisher,
//!         router,
//!     )?;
//!
//!     async {
//!         let published_ids = hedwig.publish(Message::new(
//!             MessageType::UserCreated,
//!             Version(MajorVersion(1), MinorVersion(0)),
//!             UserCreatedData { user_id: "U_123".into() }
//!         )).await;
//!     };
//!
//!     # Ok(())
//! # }
//! ```
#![deny(missing_docs, unused_import_braces, unused_qualifications)]
#![warn(trivial_casts, trivial_numeric_casts, unsafe_code, unstable_features)]

use std::{
    collections::HashMap,
    fmt,
    future::Future,
    mem,
    time::{SystemTime, UNIX_EPOCH},
};

use futures::stream::StreamExt;
use uuid::Uuid;
use valico::json_schema::{SchemaError, Scope, ValidationState};

#[cfg(feature = "google")]
mod google_publisher;
mod mock_publisher;
mod null_publisher;

/// Implementations of the Publisher trait
pub mod publishers {
    #[cfg(feature = "google")]
    pub use super::google_publisher::GooglePubSubPublisher;
    pub use super::mock_publisher::MockPublisher;
    pub use super::null_publisher::NullPublisher;
}

const FORMAT_VERSION_V1: Version = Version(MajorVersion(1), MinorVersion(0));

/// All errors that may be returned when instantiating a new Hedwig instance.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// Unable to deserialize schema
    #[error("Unable to deserialize schema")]
    SchemaDeserialize(#[source] serde_json::Error),

    /// Schema failed to compile
    #[error("Schema failed to compile")]
    SchemaCompile(#[from] SchemaError),

    /// Unable to serialize message
    #[error("Unable to serialize message")]
    MessageSerialize(#[source] serde_json::Error),

    /// Message is not routable
    #[error("Message {0} is not routable")]
    MessageRoute(Uuid),

    /// Could not parse a schema URL
    #[error("Could not parse `{1}` as a schema URL")]
    SchemaUrlParse(#[source] url::ParseError, String),

    /// Could not resolve the schema URL
    #[error("Could not resolve `{0}` to a schema")]
    SchemaUrlResolve(url::Url),

    /// Could not validate message data
    #[error("Message data does not validate per the schema: {0}")]
    DataValidation(String),

    /// Publisher failed to publish a message
    #[error("Publisher failed to publish a message batch")]
    Publisher(#[source] Box<dyn std::error::Error + Send + Sync>),

    /// Publisher failed to publish multiple batches of messages
    #[error("Publisher failed to publish multiple batches (total of {} errors)", _1.len() + 1)]
    PublisherMultiple(
        #[source] Box<dyn std::error::Error + Send + Sync>,
        Vec<Box<dyn std::error::Error + Send + Sync>>,
    ),
}

type AnyError = Box<dyn std::error::Error + Send + Sync>;

/// The special result type for [`Publisher::publish`](trait.Publisher.html)
#[derive(Debug)]
pub enum PublisherResult<Id> {
    /// Publisher succeeded.
    ///
    /// Contains a vector of published message IDs.
    Success(Vec<Id>),
    /// Publisher failed to publish any of the messages.
    OneError(AnyError, Vec<ValidatedMessage>),
    /// Publisher failed to publish some of the messages.
    ///
    /// The error type has a per-message granularity.
    PerMessage(Vec<Result<Id, (AnyError, ValidatedMessage)>>),
}

/// Interface for message publishers
pub trait Publisher {
    /// The list of identifiers for successfully published messages
    type MessageId: 'static;
    /// The future that the `publish` method returns
    type PublishFuture: Future<Output = PublisherResult<Self::MessageId>> + Send;

    /// Publish a batch of messages
    ///
    /// # Return value
    ///
    /// Shall return [`PublisherResult::Success`](PublisherResult::Success) only if all of the
    /// messages are successfully published. Otherwise `PublisherResult::OneError` or
    /// `PublisherResult::PerMessage` shall be returned to indicate an error.
    fn publish(&self, topic: &'static str, messages: Vec<ValidatedMessage>) -> Self::PublishFuture;
}

/// Type alias for custom headers associated with a message
type Headers = HashMap<String, String>;

struct Validator {
    scope: Scope,
    schema_id: url::Url,
}

impl Validator {
    fn new(schema: &str) -> Result<Validator, Error> {
        let master_schema: serde_json::Value =
            serde_json::from_str(schema).map_err(Error::SchemaDeserialize)?;

        let mut scope = Scope::new();
        let schema_id = scope.compile(master_schema, false)?;

        Ok(Validator { scope, schema_id })
    }

    fn validate<D, T>(
        &self,
        message: &Message<D, T>,
        schema: &str,
    ) -> Result<ValidationState, Error>
    where
        D: serde::Serialize,
    {
        // convert user.created/1.0 -> user.created/1.*
        let msg_schema_ptr = schema.trim_end_matches(char::is_numeric).to_owned() + "*";
        let msg_schema_url = url::Url::parse(&msg_schema_ptr)
            .map_err(|e| Error::SchemaUrlParse(e, msg_schema_ptr))?;
        let msg_schema = self
            .scope
            .resolve(&msg_schema_url)
            .ok_or_else(|| Error::SchemaUrlResolve(msg_schema_url))?;

        let msg_data = serde_json::to_value(&message.data).map_err(Error::MessageSerialize)?;

        let validation_state = msg_schema.validate(&msg_data);
        if !validation_state.is_strictly_valid() {
            return Err(Error::DataValidation(format!("{:?}", validation_state)));
        }
        Ok(validation_state)
    }
}

/// Major part component in semver
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, serde::Serialize)]
pub struct MajorVersion(pub u8);

impl fmt::Display for MajorVersion {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Minor part component in semver
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, serde::Serialize)]
pub struct MinorVersion(pub u8);

impl fmt::Display for MinorVersion {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A semver version without patch part
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct Version(pub MajorVersion, pub MinorVersion);

impl serde::Serialize for Version {
    fn serialize<S>(
        &self,
        serializer: S,
    ) -> Result<<S as serde::Serializer>::Ok, <S as serde::Serializer>::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(format!("{}.{}", self.0, self.1).as_ref())
    }
}

/// Mapping of message types to Hedwig topics
///
/// # Examples
/// ```
/// # use serde::Serialize;
/// # use strum_macros::IntoStaticStr;
/// use hedwig::{MajorVersion, MessageRouter};
///
/// # #[derive(Clone, Copy, IntoStaticStr, Hash, PartialEq, Eq)]
/// # enum MessageType {
/// #    #[strum(serialize = "user.created")]
/// #    UserCreated,
/// # }
/// #
/// let r: MessageRouter<MessageType> = |t, v| match (t, v) {
///     (MessageType::UserCreated, MajorVersion(1)) => Some("user-created-v1"),
///     _ => None,
/// };
/// ```
pub type MessageRouter<T> = fn(T, MajorVersion) -> Option<&'static str>;

/// The core type in this library
#[allow(missing_debug_implementations)]
pub struct Hedwig<T, P> {
    validator: Validator,
    publisher_name: String,
    message_router: MessageRouter<T>,
    publisher: P,
}

impl<T, P> Hedwig<T, P>
where
    P: Publisher,
{
    /// Creates a new Hedwig instance
    ///
    /// # Arguments
    ///
    /// * `schema`: The JSON schema content. It's up to the caller to read the schema;
    /// * `publisher_name`: Name of the publisher service which will be included in the message
    ///   metadata;
    /// * `publisher`: An implementation of Publisher;
    pub fn new(
        schema: &str,
        publisher_name: &str,
        publisher: P,
        message_router: MessageRouter<T>,
    ) -> Result<Hedwig<T, P>, Error> {
        Ok(Hedwig {
            validator: Validator::new(schema)?,
            publisher_name: String::from(publisher_name),
            message_router,
            publisher,
        })
    }

    /// Create a batch of messages to publish
    ///
    /// This allows to transparently retry failed messages and send them in batches larger than
    /// one, leading to potential throughput gains.
    pub fn build_batch(&self) -> PublishBatch<T, P> {
        PublishBatch {
            hedwig: self,
            messages: Vec::new(),
        }
    }

    /// Publish a single message
    ///
    /// Note, that unlike the batch builder, this does not allow recovering failed-to-publish
    /// messages.
    pub async fn publish<D>(&self, msg: Message<D, T>) -> Result<P::MessageId, Error>
    where
        D: serde::Serialize,
        T: Copy + Into<&'static str>,
    {
        let mut builder = self.build_batch();
        builder.message(msg)?;
        builder.publish_one().await
    }
}

/// A builder for publishing in batches
///
/// Among other things this structure also enables transparent retrying of failed-to-publish
/// messages.
#[allow(missing_debug_implementations)]
pub struct PublishBatch<'hedwig, T, P> {
    hedwig: &'hedwig Hedwig<T, P>,
    messages: Vec<(&'static str, ValidatedMessage)>,
}

impl<'hedwig, T, P> PublishBatch<'hedwig, T, P> {
    /// Add a message to be published in a batch
    pub fn message<D>(&mut self, msg: Message<D, T>) -> Result<&mut Self, Error>
    where
        D: serde::Serialize,
        T: Copy + Into<&'static str>,
    {
        let data_type = msg.data_type;
        let schema_version = msg.data_schema_version;
        let data_type_str = msg.data_type.into();
        let schema_url = format!(
            "{}#/schemas/{}/{}.{}",
            self.hedwig.validator.schema_id, data_type_str, schema_version.0, schema_version.1,
        );
        self.hedwig.validator.validate(&msg, &schema_url)?;
        let converted = msg
            .into_schema(
                self.hedwig.publisher_name.clone(),
                schema_url,
                FORMAT_VERSION_V1,
            )
            .map_err(Error::MessageSerialize)?;
        let route = (self.hedwig.message_router)(data_type, converted.format_version.0)
            .ok_or_else(|| Error::MessageRoute(converted.id))?;
        self.messages.push((route, converted));
        Ok(self)
    }

    /// Publish all the messages
    ///
    /// Does not consume the builder. Will return `Ok` only if and when all of the messages from
    /// the builder have been published successfully. In case of failure, unpublished messages will
    /// remain enqueued in this builder for a subsequent publish call.
    pub async fn publish(&mut self) -> Result<Vec<P::MessageId>, Error>
    where
        P: Publisher,
    {
        let mut message_ids = Vec::with_capacity(self.messages.len());
        // Sort the messages by the topic and group them into batches
        self.messages.sort_by_key(|&(k, _)| k);
        let mut current_topic = "";
        let mut current_batch = Vec::new();
        let mut futures_unordered = futures::stream::FuturesUnordered::new();
        let publisher = &self.hedwig.publisher;
        let make_job = |topic: &'static str, batch: Vec<ValidatedMessage>| async move {
            (topic, publisher.publish(topic, batch).await)
        };

        for (topic, message) in mem::replace(&mut self.messages, Vec::new()) {
            if current_topic != topic && !current_batch.is_empty() {
                let batch = mem::replace(&mut current_batch, Vec::new());
                futures_unordered.push(make_job(current_topic, batch));
            }
            current_topic = topic;
            current_batch.push(message)
        }
        if !current_batch.is_empty() {
            futures_unordered.push(make_job(current_topic, current_batch));
        }

        let mut errors = Vec::new();
        // Extract the results from all the futures
        while let (Some(result), stream) = futures_unordered.into_future().await {
            match result {
                (_, PublisherResult::Success(ids)) => message_ids.extend(ids),
                (topic, PublisherResult::OneError(err, failed_msgs)) => {
                    self.messages
                        .extend(failed_msgs.into_iter().map(|m| (topic, m)));
                    errors.push(err);
                }
                (topic, PublisherResult::PerMessage(vec)) => {
                    for message in vec {
                        match message {
                            Ok(id) => message_ids.push(id),
                            Err((err, failed_msg)) => {
                                self.messages.push((topic, failed_msg));
                                errors.push(err);
                            }
                        }
                    }
                }
            }
            futures_unordered = stream;
        }

        if let Some(first_error) = errors.pop() {
            Err(if errors.is_empty() {
                Error::Publisher(first_error)
            } else {
                Error::PublisherMultiple(first_error, errors)
            })
        } else {
            Ok(message_ids)
        }
    }

    /// Publishes just one message
    ///
    /// Panics if the builder contains anything but 1 message.
    async fn publish_one(mut self) -> Result<P::MessageId, Error>
    where
        P: Publisher,
    {
        let (topic, message) = if let Some(v) = self.messages.pop() {
            assert!(
                self.messages.is_empty(),
                "messages buffer must contain exactly 1 entry!"
            );
            v
        } else {
            panic!("messages buffer must contain exactly 1 entry!")
        };
        match self.hedwig.publisher.publish(topic, vec![message]).await {
            PublisherResult::Success(mut ids) if ids.len() == 1 => Ok(ids.pop().unwrap()),
            PublisherResult::OneError(err, _) => Err(Error::Publisher(err)),
            PublisherResult::PerMessage(mut results) if results.len() == 1 => {
                results.pop().unwrap().map_err(|(e, _)| Error::Publisher(e))
            }
            _ => {
                panic!("Publisher should have returned 1 result only!");
            }
        }
    }
}

/// A message builder
#[derive(Clone, Debug, PartialEq)]
pub struct Message<D, T> {
    /// Message identifier
    id: Option<Uuid>,
    /// Creation timestamp
    timestamp: std::time::Duration,
    /// Message headers
    headers: Option<Headers>,
    /// Message data
    data: D,
    /// Message type
    data_type: T,
    data_schema_version: Version,
}

impl<D, T> Message<D, T> {
    /// Construct a new message
    pub fn new(data_type: T, data_schema_version: Version, data: D) -> Self {
        Message {
            id: None,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time is before the unix epoch"),
            headers: None,
            data,
            data_type,
            data_schema_version,
        }
    }

    /// Overwrite the header map associated with the message
    ///
    /// This may be used to track the `request_id`, for example.
    pub fn headers(mut self, headers: Headers) -> Self {
        self.headers = Some(headers);
        self
    }

    /// Add a custom header to the message
    ///
    /// This may be used to track the `request_id`, for example.
    pub fn header<H, V>(mut self, header: H, value: V) -> Self
    where
        H: Into<String>,
        V: Into<String>,
    {
        if let Some(ref mut hdrs) = self.headers {
            hdrs.insert(header.into(), value.into());
        } else {
            let mut map = HashMap::new();
            map.insert(header.into(), value.into());
            self.headers = Some(map);
        }
        self
    }

    /// Add custom id to the message
    ///
    /// If not called, a random UUID is generated for this message.
    pub fn id(mut self, id: Uuid) -> Self {
        self.id = Some(id);
        self
    }

    fn into_schema(
        self,
        publisher_name: String,
        schema: String,
        format_version: Version,
    ) -> Result<ValidatedMessage, serde_json::Error>
    where
        D: serde::Serialize,
    {
        Ok(ValidatedMessage {
            id: self.id.unwrap_or_else(Uuid::new_v4),
            metadata: Metadata {
                timestamp: self.timestamp.as_millis(),
                publisher: publisher_name,
                headers: self.headers.unwrap_or_else(HashMap::new),
            },
            schema,
            format_version,
            data: serde_json::to_value(self.data)?,
        })
    }
}

/// Additional metadata associated with a message
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
struct Metadata {
    /// The timestamp when message was created in the publishing service
    timestamp: u128,

    /// Name of the publishing service
    publisher: String,

    /// Custom headers
    ///
    /// This may be used to track request_id, for example.
    headers: Headers,
}

/// A validated message
///
/// This data type is the schema or the json messages being sent over the wire.
#[derive(Debug, serde::Serialize)]
pub struct ValidatedMessage {
    /// Unique message identifier
    id: Uuid,
    /// The metadata associated with the message
    metadata: Metadata,
    /// URI of the schema validating this message
    ///
    /// E.g. `https://hedwig.domain.xyz/schemas#/schemas/user.created/1.0`
    schema: String,
    /// Format of the message schema used
    format_version: Version,
    /// The message data
    data: serde_json::Value,
}

#[cfg(test)]
mod tests {
    use super::*;

    use strum_macros::IntoStaticStr;

    #[derive(Clone, Copy, Debug, IntoStaticStr, Hash, PartialEq, Eq)]
    enum MessageType {
        #[strum(serialize = "user.created")]
        UserCreated,

        #[strum(serialize = "invalid.schema")]
        InvalidSchema,

        #[strum(serialize = "invalid.route")]
        InvalidRoute,
    }

    #[derive(Clone, Debug, serde::Serialize, PartialEq)]
    struct UserCreatedData {
        user_id: String,
    }

    const VERSION_1_0: Version = Version(MajorVersion(1), MinorVersion(0));

    const SCHEMA: &str = r#"
{
  "$id": "https://hedwig.standard.ai/schema",
  "$schema": "https://json-schema.org/draft-04/schema#",
  "description": "Example Schema",
  "schemas": {
      "user.created": {
          "1.*": {
              "description": "A new user was created",
              "type": "object",
              "x-versions": [
                  "1.0"
              ],
              "required": [
                  "user_id"
              ],
              "properties": {
                  "user_id": {
                      "$ref": "https://hedwig.standard.ai/schema#/definitions/UserId/1.0"
                  }
              }
          }
      },
      "invalid.route": {
          "1.*": {}
      }
  },
  "definitions": {
      "UserId": {
          "1.0": {
              "type": "string"
          }
      }
  }
}"#;

    fn router(t: MessageType, v: MajorVersion) -> Option<&'static str> {
        match (t, v) {
            (MessageType::UserCreated, MajorVersion(1)) => Some("dev-user-created-v1"),
            (MessageType::InvalidSchema, MajorVersion(1)) => Some("invalid-schema"),
            _ => None,
        }
    }

    fn mock_hedwig() -> Hedwig<MessageType, publishers::MockPublisher> {
        Hedwig::new(
            SCHEMA,
            "myapp",
            publishers::MockPublisher::default(),
            router,
        )
        .unwrap()
    }

    #[test]
    fn message_constructor() {
        let data = UserCreatedData {
            user_id: "U_123".into(),
        };
        let message = Message::new(MessageType::UserCreated, VERSION_1_0, data.clone());
        assert_eq!(None, message.headers);
        assert_eq!(data, message.data);
        assert_eq!(MessageType::UserCreated, message.data_type);
        assert_eq!(VERSION_1_0, message.data_schema_version);
    }

    #[test]
    fn message_set_headers() {
        let request_id = Uuid::new_v4().to_string();
        let message = Message::new(
            MessageType::UserCreated,
            VERSION_1_0,
            UserCreatedData {
                user_id: "U_123".into(),
            },
        )
        .header("request_id", request_id.clone());
        assert_eq!(
            request_id,
            message
                .headers
                .unwrap()
                .get(&"request_id".to_owned())
                .unwrap()
                .as_str()
        );
    }

    #[test]
    fn message_with_id() {
        let id = uuid::Uuid::new_v4();
        let message = Message::new(
            MessageType::UserCreated,
            VERSION_1_0,
            UserCreatedData {
                user_id: "U_123".into(),
            },
        )
        .id(id);
        assert_eq!(id, message.id.unwrap());
    }

    #[tokio::test]
    async fn publish() {
        let hedwig = mock_hedwig();
        let mut custom_headers = Headers::new();
        let request_id = Uuid::new_v4().to_string();
        let msg_id = Uuid::new_v4();
        let message = Message::new(
            MessageType::UserCreated,
            VERSION_1_0,
            UserCreatedData {
                user_id: "U_123".into(),
            },
        )
        .header("request_id", request_id.clone())
        .id(msg_id);
        custom_headers.insert("request_id".to_owned(), request_id);
        let mut builder = hedwig.build_batch();
        builder.message(message.clone()).unwrap();
        builder.publish().await.unwrap();
        hedwig
            .publisher
            .assert_message_published("dev-user-created-v1", msg_id);
    }

    #[tokio::test]
    async fn publish_empty() {
        let hedwig = mock_hedwig();
        let mut builder = hedwig.build_batch();
        builder.publish().await.unwrap();
    }

    #[test]
    fn validator_deserialization_error() {
        let r = Validator::new("bad json");
        assert!(matches!(r.err(), Some(Error::SchemaDeserialize(_))));
    }

    #[test]
    fn validator_schema_compile_error() {
        const BAD_SCHEMA: &str = r#"
{
  "$schema": "https://json-schema.org/draft-04/schema#",
  "definitions": {
      "UserId": {
          "1.0": {
              "type": "bad value"
          }
      }
  }
}"#;

        let r = Validator::new(BAD_SCHEMA);
        assert!(matches!(r.err(), Some(Error::SchemaCompile(_))));
    }

    #[test]
    fn message_serialization_error() {
        let hedwig = mock_hedwig();
        #[derive(serde::Serialize)]
        struct BadUserCreatedData {
            user_ids: HashMap<Vec<i32>, String>,
        };
        let mut user_ids = HashMap::new();
        user_ids.insert(vec![32, 64], "U_123".to_owned());
        let data = BadUserCreatedData { user_ids };
        let m = Message::new(MessageType::UserCreated, VERSION_1_0, data);
        let mut builder = hedwig.build_batch();
        assert!(matches!(builder.message(m).err(), Some(Error::MessageSerialize(_))));
    }

    #[test]
    fn message_router_error() {
        let hedwig = mock_hedwig();
        let m = Message::new(MessageType::InvalidRoute, VERSION_1_0, ());
        let mut builder = hedwig.build_batch();
        assert!(matches!(builder.message(m).err(), Some(Error::MessageRoute(_))));
    }

    #[test]
    fn message_invalid_schema_error() {
        let hedwig = mock_hedwig();
        let m = Message::new(MessageType::InvalidSchema, VERSION_1_0, ());
        let mut builder = hedwig.build_batch();
        assert!(matches!(builder.message(m).err(), Some(Error::SchemaUrlResolve(_))));
    }

    #[test]
    fn message_data_validation_eror() {
        let hedwig = mock_hedwig();
        #[derive(serde::Serialize)]
        struct BadUserCreatedData {
            user_ids: Vec<i32>,
        };
        let data = BadUserCreatedData { user_ids: vec![1] };
        let m = Message::new(MessageType::UserCreated, VERSION_1_0, data);
        let mut builder = hedwig.build_batch();
        assert!(matches!(builder.message(m).err(), Some(Error::DataValidation(_))));
    }

    #[test]
    fn errors_send_sync() {
        fn assert_error<T: std::error::Error + Send + Sync + 'static>() {}
        assert_error::<Error>();
        assert_error::<Error>();
    }
}
