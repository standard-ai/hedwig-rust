#![deny(missing_docs, unused_import_braces, unused_qualifications)]
#![warn(
    missing_debug_implementations,
    missing_copy_implementations,
    trivial_casts,
    trivial_numeric_casts,
    unsafe_code,
    unstable_features
)]

//! A Hedwig library for Rust. Hedwig is a message bus that works with AWS SNS/SQS Google Cloud Pubsub, with messages
//! validated using JSON schema. The publisher and consumer are de-coupled and fan-out is supported out of the box.
//!
//! # Examples
//!
//! Publish a new message
//!
//! ```no_run
//! use hedwig::{Hedwig, MajorVersion, MinorVersion, Version};
//! # #[cfg(feature = "google")]
//! # use hedwig::GooglePublisher;
//! # #[cfg(feature = "mock")]
//! # use hedwig::MockPublisher;
//! # use std::path::Path;
//! # use serde::Serialize;
//! # use strum_macros::IntoStaticStr;
//!
//! fn main() -> Result<(), failure::Error> {
//! let schema = r#"
//!     {
//!       "$id": "https://hedwig.standard.ai/schema",
//!       "$schema": "http://json-schema.org/draft-04/schema#",
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
//!     fn router(t: &MessageType, v: &MajorVersion) -> Option<&'static str> {
//!         match (t, v) {
//!             (&MessageType::UserCreated, &MajorVersion(1)) => Some("dev-user-created-v1"),
//!             _ => None,
//!         }
//!     }
//!
//!     // create a publisher instance
//!     # #[cfg(feature = "google")]
//!     # let publisher = GooglePublisher::new(String::from("/home/.google-key.json"), "myproject".into())?;
//!     # #[cfg(feature = "mock")]
//!     # let publisher = MockPublisher::default();
//!     # #[cfg(any(feature = "google", feature="mock"))]
//!
//!     let hedwig = Hedwig::new(
//!         schema,
//!         "myapp",
//!         publisher,
//!         router,
//!     )?;
//!
//!     # #[cfg(any(feature = "google", feature="mock"))]
//!     let message = hedwig.message(
//!         MessageType::UserCreated,
//!         Version(MajorVersion(1), MinorVersion(0)),
//!         UserCreatedData { user_id: "U_123".into() },
//!     )?;
//!
//!     # #[cfg(any(feature = "google", feature="mock"))]
//!     hedwig.publish(message)?;
//!     # Ok(())
//! # }
//! ```

#[cfg(feature = "mock")]
use std::cell::RefCell;
use std::collections::HashMap;
use std::convert::From;
#[cfg(feature = "google")]
use std::default::Default;
use std::fmt;
use std::io;
#[cfg(feature = "google")]
use std::path::Path;
use std::result::Result;
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(feature = "google")]
use base64;
use failure::Fail;
#[cfg(feature = "google")]
use google_pubsub1::{self as pubsub1, Pubsub};
#[cfg(feature = "google")]
use hyper::{self, net::HttpsConnector, Client};
#[cfg(feature = "google")]
use hyper_rustls;
use serde::Serialize;
use serde_json;
use uuid::Uuid;
use valico::json_schema::{SchemaError, Scope, ValidationState};
#[cfg(feature = "google")]
use yup_oauth2 as oauth2;

/// All errors that may be returned when instantiating a new Hedwig instance
#[allow(missing_docs)]
#[derive(Debug, Fail)]
pub enum HedwigError {
    #[fail(display = "Credentials path is invalid: {}", _0)]
    CredentialsPathError(String),

    #[fail(display = "Credentials file couldn't be read")]
    CredentialsIOError(#[cause] io::Error),

    #[fail(display = "Unable to deserialize schema")]
    DeserializationError(#[cause] serde_json::Error),

    #[fail(display = "Schema failed to compile")]
    // can't specify #[cause] since SchemaError doesn't implement Error yet (see https://github.com/rustless/valico/issues/24)
    SchemaCompileError(SchemaError),
}

impl From<SchemaError> for HedwigError {
    fn from(e: SchemaError) -> Self {
        HedwigError::SchemaCompileError(e)
    }
}

/// All errors that may be returned while instantiating a new Message instance
#[allow(missing_docs)]
#[derive(Debug, Fail)]
pub enum MessageError {
    #[fail(display = "Unable to serialize message data")]
    SerializationError(#[cause] serde_json::Error),

    #[fail(display = "Router failed to route message correctly")]
    RouterError(&'static str),

    #[fail(display = "Message has invalid schema declaration: {}", _0)]
    MessageInvalidSchemaError(String),

    #[fail(display = "Message data doesn't validate per the schema")]
    MessageDataValidationError(String),
}

/// All errors that may be returned while publishing a message
#[allow(missing_docs)]
#[derive(Debug, Fail)]
pub enum PublishError {
    #[fail(display = "Unable to serialize message")]
    SerializationError(#[cause] serde_json::Error),

    #[fail(display = "API failure occurred when publishing message")]
    PublishAPIFailure(String),

    #[fail(display = "Invalid from publish API: can't find published message id")]
    InvalidResponseNoMessageId(&'static str),
}

/// A trait for message publishers. This may be used to implement custom behavior such as publish to \<insert your
/// favorite cloud platform\>.
pub trait Publisher {
    /// Publish a Hedwig message.
    fn publish<D, T>(&self, message: Message<D, T>) -> Result<String, PublishError>
    where
        D: Serialize;
}

/// A publisher that uses Google PubSub. To use this class, add feature `google`.
///
/// # Examples
///
/// ```no_run
/// # #[cfg(feature = "google")]
/// # use hedwig::{Publisher, GooglePublisher};
/// # fn main() -> Result<(), failure::Error> {
/// # #[cfg(feature = "google")]
/// let publisher = GooglePublisher::new(String::from("/home/.google-key.json"), "myproject".into())?;
/// # Ok(())
/// # }
/// ```
#[cfg(feature = "google")]
#[allow(missing_debug_implementations)]
pub struct GooglePublisher {
    client: Pubsub<Client, oauth2::ServiceAccountAccess<Client>>,
    google_cloud_project: String,
}

#[cfg(feature = "google")]
impl GooglePublisher {
    /// Create a new instance of GooglePublisher
    /// For now, this requires explicitly passing in the path to your credentials file, as well as the name of the
    /// project where pubsub infra lives.
    ///
    /// # Arguments
    ///
    /// * google_application_credentials - Path to the google credentials file.
    /// * google_cloud_project - The Google Cloud project where your pubsub resources lie - this /may be/ different
    /// from the credentials.
    pub fn new<P>(
        google_application_credentials: P,
        google_cloud_project: String,
    ) -> Result<GooglePublisher, HedwigError>
    where
        P: AsRef<Path>,
    {
        let client_secret = oauth2::service_account_key_from_file(
            &google_application_credentials
                .as_ref()
                .to_string_lossy()
                .into(),
        )
        .map_err(HedwigError::CredentialsIOError)?;
        let auth_https = HttpsConnector::new(hyper_rustls::TlsClient::new());
        let auth_client = hyper::Client::with_connector(auth_https);

        let access = oauth2::ServiceAccountAccess::new(client_secret, auth_client);

        let https = HttpsConnector::new(hyper_rustls::TlsClient::new());
        let pubsub_client = hyper::Client::with_connector(https);

        let client = Pubsub::new(pubsub_client, access);

        Ok(GooglePublisher {
            client,
            google_cloud_project: String::from(google_cloud_project),
        })
    }
}

#[cfg(feature = "google")]
impl Publisher for GooglePublisher {
    /// Publishes a message on Google Pubsub and returns a pubsub id (usually an integer).
    fn publish<D, T>(&self, message: Message<D, T>) -> Result<String, PublishError>
    where
        D: Serialize,
    {
        let raw_message =
            serde_json::to_string(&message).map_err(PublishError::SerializationError)?;

        let pubsub_message = pubsub1::PubsubMessage {
            data: Some(base64::encode(&raw_message)),
            attributes: Some(message.headers()),
            ..Default::default()
        };

        let topic_path = format!(
            "projects/{}/topics/hedwig-{}",
            self.google_cloud_project, message.topic
        );
        let request = pubsub1::PublishRequest {
            messages: Some(vec![pubsub_message]),
        };
        let result = self
            .client
            .projects()
            .topics_publish(request, topic_path.as_ref())
            .doit();

        match result {
            Err(e) => Err(PublishError::PublishAPIFailure(format!(
                "Publish error: {}",
                e
            ))),
            Ok((_, response)) => {
                let not_found = "Published message, but can't find message id";

                // find the first item from the returned vector
                response
                    .message_ids
                    .ok_or(PublishError::InvalidResponseNoMessageId(not_found))
                    .map(|v| v.into_iter().next())
                    .transpose()
                    .unwrap_or(Err(PublishError::InvalidResponseNoMessageId(not_found)))
            }
        }
    }
}

#[cfg(feature = "mock")]
#[derive(Debug, Default)]
/// A mock publisher that doesn't publish messages, but just stores them in-memory for later verification
/// This is useful primarily in tests. To use this class, add feature `mock`.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "mock")]
/// use hedwig::MockPublisher;
///
/// # #[cfg(feature = "mock")]
/// let publisher = MockPublisher::default();
/// ```
pub struct MockPublisher {
    // `RefCell` for interior mutability
    published_messages: RefCell<HashMap<Uuid, (String, HashMap<String, String>)>>,
}

#[cfg(feature = "mock")]
impl MockPublisher {
    /// Verify that a message was published. This method asserts that the message you expected to be published, was
    /// indeed published
    pub fn assert_message_published<D, T>(
        &self,
        message: &Message<D, T>,
        headers: &HashMap<String, String>,
    ) where
        D: Serialize,
    {
        let published_messages = self.published_messages.borrow();
        let (published, published_headers) = published_messages
            .get(&message.id)
            .expect("message not found");
        let serialized = serde_json::to_string(&message).unwrap();
        assert_eq!(published, &serialized);
        assert_eq!(published_headers, headers);
    }
}

#[cfg(feature = "mock")]
impl Publisher for MockPublisher {
    fn publish<D, T>(&self, message: Message<D, T>) -> Result<String, PublishError>
    where
        D: Serialize,
    {
        let serialized =
            serde_json::to_string(&message).map_err(PublishError::SerializationError)?;
        self.published_messages
            .borrow_mut()
            .insert(message.id, (serialized, message.headers()));
        Ok(String::new())
    }
}

struct Validator {
    scope: Scope,
    schema_id: url::Url,
}

impl Validator {
    fn new(schema: &str) -> Result<Validator, HedwigError> {
        let master_schema: serde_json::Value =
            serde_json::from_str(schema).map_err(HedwigError::DeserializationError)?;

        let mut scope = Scope::new();
        let schema_id = scope.compile(master_schema, false)?;

        Ok(Validator { scope, schema_id })
    }

    fn validate<D, T>(&self, message: &Message<D, T>) -> Result<ValidationState, MessageError>
    where
        D: Serialize,
    {
        // convert user.created/1.0 -> user.created/1.*
        let msg_schema_ptr = message.schema.trim_end_matches(char::is_numeric).to_owned() + "*";

        let msg_schema_url = match url::Url::parse(msg_schema_ptr.as_str()) {
            Ok(u) => u,
            Err(_) => return Err(MessageError::MessageInvalidSchemaError(msg_schema_ptr)),
        };

        let msg_schema = match self.scope.resolve(&msg_schema_url) {
            None => return Err(MessageError::MessageInvalidSchemaError(msg_schema_ptr)),
            Some(s) => s,
        };

        let msg_data =
            serde_json::to_value(&message.data).map_err(MessageError::SerializationError)?;

        let validation_state = msg_schema.validate(&msg_data);
        if !validation_state.is_strictly_valid() {
            return Err(MessageError::MessageDataValidationError(format!(
                "validation_state: {:#?}",
                validation_state
            )));
        }
        Ok(validation_state)
    }
}

/// Major part component in semver
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize)]
pub struct MajorVersion(pub u8);

impl fmt::Display for MajorVersion {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Minor part component in semver
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize)]
pub struct MinorVersion(pub u8);

impl fmt::Display for MinorVersion {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A semver version without patch part
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct Version(pub MajorVersion, pub MinorVersion);

impl Serialize for Version {
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

/// MessageRouter is a function that can route messages of a given type and version to a Hedwig topic.
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
///     (&MessageType::UserCreated, &MajorVersion(1)) => Some("user-created-v1"),
///     _ => None,
/// };
/// ```
pub type MessageRouter<T> = fn(&T, &MajorVersion) -> Option<&'static str>;

/// Central instance to access all Hedwig related resources
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
    /// Creates a new Hedwig instance. Application credentials cannot be auto-discovered in Google Cloud environment
    /// unlike the Python library.
    ///
    /// # Arguments
    ///
    /// * schema: The JSON schema content. It's up to the caller to read the schema from a file.
    /// * publisher name: Name of the publisher service. This will be part of the metadata in the message.
    /// * publisher - An implementation of Publisher
    /// * message_router - A function that can route messages to topics
    pub fn new(
        schema: &str,
        publisher_name: &str,
        publisher: P,
        message_router: MessageRouter<T>,
    ) -> Result<Hedwig<T, P>, HedwigError> {
        Ok(Hedwig {
            validator: Validator::new(schema)?,
            publisher_name: String::from(publisher_name),
            message_router,
            publisher,
        })
    }

    /// Creates a new message with given data type, schema version and data object.
    ///
    /// # Arguments
    ///
    /// * data_type -  An Enum instance with static str representation
    /// * data_schema_version - Version of the data object
    /// * data - The message data - must be serializable
    pub fn message<D>(
        &self,
        data_type: T,
        data_schema_version: Version,
        data: D,
    ) -> Result<Message<D, T>, MessageError>
    where
        D: Serialize,
        T: Copy + Into<&'static str>,
    {
        Message::new(&self, data_type, data_schema_version, data)
    }

    /// Publish a message using the configured publisher. Returns the publish id if successful. The publish id depends
    /// on your publisher.
    ///
    /// # Arguments
    ///
    /// * `message` - the message to publish
    pub fn publish<D>(&self, message: Message<D, T>) -> Result<String, PublishError>
    where
        D: Serialize,
    {
        self.publisher.publish(message)
    }
}

/// Additional metadata associated with a message
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Metadata {
    /// The timestamp when message was created in the publishing service
    pub timestamp: u128,

    /// Name of the publishing service
    pub publisher: String,

    /// Custom headers. This may be used to track request_id, for example.
    pub headers: HashMap<String, String>,
}

const FORMAT_VERSION_V1: Version = Version(MajorVersion(1), MinorVersion(0));

/// Message represents an instance of a message on the message bus.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Message<D, T> {
    /// Message identifier
    pub id: Uuid,

    /// Metadata associated with the message
    pub metadata: Metadata,

    /// Message schema, e.g. `http://hedwig.standard.ai/schemas#/schemas/user.created/1.0`
    pub schema: String,

    /// Associated message data
    pub data: D,

    /// Format version for the message container
    pub format_version: Version,

    /// Type of data represented by this message
    #[serde(skip)]
    pub data_type: T,

    /// Schema version of the data object. This should follow semver.
    #[serde(skip)]
    pub data_schema_version: Version,

    #[serde(skip)]
    topic: String,
}

impl<D, T> Message<D, T> {
    fn new<P>(
        hedwig: &Hedwig<T, P>,
        data_type: T,
        data_schema_version: Version,
        data: D,
    ) -> Result<Self, MessageError>
    where
        D: Serialize,
        T: Copy + Into<&'static str>,
    {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");

        let topic = (hedwig.message_router)(&data_type, &data_schema_version.0)
            .ok_or(MessageError::RouterError("Topic not found"))?
            .to_owned();

        let message = Message {
            id: Uuid::new_v4(),
            metadata: Metadata {
                headers: HashMap::new(),
                publisher: hedwig.publisher_name.clone(),
                timestamp: timestamp.as_millis(),
            },
            schema: format!(
                "{}#/schemas/{}/{}.{}",
                hedwig.validator.schema_id,
                data_type.into(),
                data_schema_version.0,
                data_schema_version.1,
            ),
            data,
            format_version: FORMAT_VERSION_V1,
            data_type,
            data_schema_version,
            topic,
        };
        hedwig.validator.validate(&message)?;
        Ok(message)
    }

    /// Add custom headers to the message. This may be used to track `request_id`, for example.
    pub fn with_headers(&mut self, headers: HashMap<String, String>) -> &mut Self {
        (&mut self.metadata).headers = headers;
        self
    }

    /// Add custom id to the message. If not provided, a new uuid v4 is used.
    pub fn with_id(&mut self, id: Uuid) -> &mut Self {
        self.id = id;
        self
    }

    fn headers(&self) -> HashMap<String, String> {
        self.metadata.headers.clone()
    }
}

#[cfg(test)]
#[cfg(feature = "mock")]
mod tests {
    use super::*;

    use strum_macros::IntoStaticStr;

    #[derive(Clone, Copy, Debug, IntoStaticStr, Hash, PartialEq, Eq)]
    enum MessageType {
        #[strum(serialize = "user.created")]
        UserCreated,
    }

    #[derive(Clone, Debug, Serialize, PartialEq)]
    struct UserCreatedData {
        user_id: String,
    }

    const VERSION_1_0: Version = Version(MajorVersion(1), MinorVersion(0));

    const SCHEMA: &str = r#"
{
  "$id": "https://hedwig.standard.ai/schema",
  "$schema": "http://json-schema.org/draft-04/schema#",
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

    fn router(t: &MessageType, v: &MajorVersion) -> Option<&'static str> {
        match (t, v) {
            (&MessageType::UserCreated, &MajorVersion(1)) => Some("dev-user-created-v1"),
            _ => None,
        }
    }

    fn mock_hedwig() -> Hedwig<MessageType, MockPublisher> {
        Hedwig::new(SCHEMA, "myapp", MockPublisher::default(), router).unwrap()
    }

    #[test]
    fn message_constructor() {
        let hedwig = mock_hedwig();
        let data = UserCreatedData {
            user_id: "U_123".into(),
        };
        let message = hedwig
            .message(MessageType::UserCreated, VERSION_1_0, data.clone())
            .unwrap();
        assert_eq!(HashMap::new(), message.metadata.headers);
        assert_eq!(hedwig.publisher_name, message.metadata.publisher);
        assert_eq!(data, message.data);
        assert_eq!(MessageType::UserCreated, message.data_type);
        assert_eq!(VERSION_1_0, message.data_schema_version);
        assert_eq!(
            "https://hedwig.standard.ai/schema#/schemas/user.created/1.0",
            message.schema
        );
        assert_eq!(FORMAT_VERSION_V1, message.format_version);
    }

    #[test]
    fn message_set_headers() {
        let mut custom_headers = HashMap::new();
        let request_id = Uuid::new_v4().to_string();
        custom_headers.insert("request_id".to_owned(), request_id.clone());
        let hedwig = mock_hedwig();
        let mut message = hedwig
            .message(
                MessageType::UserCreated,
                VERSION_1_0,
                UserCreatedData {
                    user_id: "U_123".into(),
                },
            )
            .unwrap();
        message.with_headers(custom_headers);
        assert_eq!(
            request_id,
            message
                .metadata
                .headers
                .get(&"request_id".to_owned())
                .unwrap()
                .as_str()
        );
    }

    #[test]
    fn message_with_id() {
        let id = uuid::Uuid::new_v4();
        let hedwig = mock_hedwig();
        let mut message = hedwig
            .message(
                MessageType::UserCreated,
                VERSION_1_0,
                UserCreatedData {
                    user_id: "U_123".into(),
                },
            )
            .unwrap();
        message.with_id(id.clone());
        assert_eq!(id, message.id);
    }

    #[test]
    fn publish() {
        let hedwig = mock_hedwig();
        let mut custom_headers = HashMap::new();
        let request_id = Uuid::new_v4().to_string();
        custom_headers.insert("request_id".to_owned(), request_id.clone());
        let mut message = hedwig
            .message(
                MessageType::UserCreated,
                VERSION_1_0,
                UserCreatedData {
                    user_id: "U_123".into(),
                },
            )
            .unwrap();
        message.with_headers(custom_headers.clone());
        hedwig.publish(message.clone()).unwrap();
        hedwig
            .publisher
            .assert_message_published(&message, &custom_headers);
    }
}
