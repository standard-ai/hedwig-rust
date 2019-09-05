#![deny(missing_docs, unused_import_braces, unused_qualifications)]
#![warn(
    missing_debug_implementations,
    missing_copy_implementations,
    trivial_casts,
    trivial_numeric_casts,
    unsafe_code,
    unstable_features
)]

//! A Hedwig library for Rust. Hedwig is a message bus that works with arbitrary pubsub services
//! such as AWS SNS/SQS or Google Cloud Pubsub. Messages are validated using a JSON schema. The
//! publisher and consumer are de-coupled and fan-out is supported out of the box.
//!
//! # Example: publish a new message
//!
//! ```no_run
//! use hedwig::{Hedwig, MajorVersion, MinorVersion, Version, Message};
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
//!     let message = hedwig.start_publish().message(
//!         Message::new(MessageType::UserCreated,
//!         Version(MajorVersion(1), MinorVersion(0)),
//!         UserCreatedData { user_id: "U_123".into() })
//!     )?.publish()?;
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

const FORMAT_VERSION_V1: Version = Version(MajorVersion(1), MinorVersion(0));

/// All errors that may be returned when instantiating a new Hedwig instance.
#[allow(missing_docs)]
#[derive(Debug, Fail)]
pub enum HedwigError {
    #[fail(display = "Credentials file {:?} couldn't be read", _1)]
    CannotOpenCredentialsFile(#[cause] io::Error, std::path::PathBuf),

    #[fail(display = "Credentials file {:?} couldn't be parsed", _1)]
    CannotParseCredentialsFile(#[cause] serde_json::Error, std::path::PathBuf),

    #[fail(display = "Unable to deserialize schema")]
    DeserializationError(#[cause] serde_json::Error),

    #[fail(display = "Schema failed to compile")]
    SchemaCompileError(#[cause] SchemaError),
}

impl From<SchemaError> for HedwigError {
    fn from(e: SchemaError) -> Self {
        HedwigError::SchemaCompileError(e)
    }
}

/// All errors that may be returned while publishing a message.
#[allow(missing_docs)]
#[derive(Debug, Fail)]
pub enum PublishError {
    #[fail(display = "Unable to serialize message")]
    SerializationError(#[cause] serde_json::Error),

    #[fail(display = "Message {} is not routable", _0)]
    RouteError(Uuid),

    #[fail(display = "API failure occurred when publishing message")]
    PublishAPIFailure(#[cause] failure::Error),

    #[fail(display = "Invalid from publish API: can't find published message id")]
    InvalidResponseNoMessageId,

    #[fail(display = "Could not parse `{}` as a schema URL", _1)]
    InvalidSchemaUrl(#[cause] url::ParseError, String),

    #[fail(display = "Could not resolve `{}` to a schema", _0)]
    UnresolvableSchemaUrl(url::Url),

    #[fail(display = "Message data doesn't validate per the schema")]
    DataValidationError(failure::Error),
}

/// Message publishers.
///
/// This is used to interface with arbitrary pub-sub services.
pub trait Publisher {
    /// Publish a Hedwig message.
    fn publish(&self, message: Vec<(&'static str, MessageSchema)>) -> Result<(), PublishError>;
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
        let path = google_application_credentials.as_ref();
        let f = std::fs::OpenOptions::new().read(true).open(path).map_err(|e|
            HedwigError::CannotOpenCredentialsFile(e, path.into()))?;
        let client_secret: oauth2::ServiceAccountKey = serde_json::from_reader(f)
            .map_err(|e| HedwigError::CannotParseCredentialsFile(e, path.into()))?;

        let auth_https = HttpsConnector::new(hyper_rustls::TlsClient::new());
        let auth_client = hyper::Client::with_connector(auth_https);

        let access = oauth2::ServiceAccountAccess::new(client_secret, auth_client);

        let https = HttpsConnector::new(hyper_rustls::TlsClient::new());
        let pubsub_client = hyper::Client::with_connector(https);

        let client = Pubsub::new(pubsub_client, access);

        Ok(GooglePublisher {
            client,
            google_cloud_project,
        })
    }

    fn publish_batch(
        &self,
        topic: &str,
        batch: Vec<pubsub1::PubsubMessage>,
    ) -> Result<(), PublishError> {
        self.client
            .projects()
            .topics_publish(
                pubsub1::PublishRequest {
                    messages: Some(batch),
                },
                format!(
                    "projects/{}/topics/hedwig-{}",
                    self.google_cloud_project, topic
                )
                .as_ref(),
            )
            .doit()
            .map_err(|e| PublishError::PublishAPIFailure(failure::err_msg(format!("{}", e))))
            .map(|_| ())
    }
}

#[cfg(feature = "google")]
impl Publisher for GooglePublisher {
    /// Publishes a message on Google Pubsub and returns a pubsub id (usually an integer).
    fn publish(
        &self,
        mut messages: Vec<(&'static str, MessageSchema)>,
    ) -> Result<(), PublishError> {
        // First sort the messages by the route
        messages.sort_by_key(|&(k, _)| k);

        let mut current_topic = "";
        let mut current_batch = Vec::new();

        for (topic, message) in messages {
            if current_topic != topic && !current_batch.is_empty() {
                self.publish_batch(current_topic, current_batch)?;
                current_batch = Vec::new();
            }
            current_topic = topic;

            let raw_message =
                serde_json::to_string(&message).map_err(PublishError::SerializationError)?;
            current_batch.push(pubsub1::PubsubMessage {
                data: Some(base64::encode(&raw_message)),
                attributes: Some(message.metadata.headers),
                ..Default::default()
            })
        }

        if !current_batch.is_empty() {
            self.publish_batch(current_topic, current_batch)?;
        }

        Ok(())
    }
}

/// Type alias for custom headers associated with a message
type Headers = HashMap<String, String>;

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
#[cfg(feature = "mock")]
#[derive(Debug, Default)]
pub struct MockPublisher {
    // `RefCell` for interior mutability
    published_messages: RefCell<HashMap<Uuid, (String, Headers)>>,
}

#[cfg(feature = "mock")]
impl Publisher for MockPublisher {
    fn publish(&self, messages: Vec<(&'static str, MessageSchema)>) -> Result<(), PublishError> {
        for (_, message) in messages {
            let serialized =
                serde_json::to_string(&message).map_err(PublishError::SerializationError)?;
            self.published_messages
                .borrow_mut()
                .insert(message.id, (serialized, message.metadata.headers));
        }
        Ok(())
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

    fn validate<D, T>(
        &self,
        message: &Message<D, T>,
        schema: &str,
    ) -> Result<ValidationState, PublishError>
    where
        D: Serialize,
    {
        // convert user.created/1.0 -> user.created/1.*
        let msg_schema_ptr = schema.trim_end_matches(char::is_numeric).to_owned() + "*";
        let msg_schema_url = url::Url::parse(&msg_schema_ptr)
            .map_err(|e| PublishError::InvalidSchemaUrl(e, msg_schema_ptr))?;
        let msg_schema = self
            .scope
            .resolve(&msg_schema_url)
            .ok_or_else(|| PublishError::UnresolvableSchemaUrl(msg_schema_url))?;

        let msg_data =
            serde_json::to_value(&message.data).map_err(PublishError::SerializationError)?;

        let validation_state = msg_schema.validate(&msg_data);
        if !validation_state.is_strictly_valid() {
            return Err(PublishError::DataValidationError(failure::err_msg(
                format!("{:?}", validation_state),
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

/// `MessageRouter` is a function that maps messages to Hedwig topics.
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

    /// Create a builder for a batch publish construction.
    pub fn start_publish(&self) -> HedwigPublishBuilder<T, P> {
        HedwigPublishBuilder {
            hedwig: self,
            messages: Vec::new(),
        }
    }
}

/// A builder for batch publish.
#[allow(missing_debug_implementations)]
pub struct HedwigPublishBuilder<'hedwig, T, P> {
    hedwig: &'hedwig Hedwig<T, P>,
    messages: Vec<(&'static str, MessageSchema)>,
}

impl<'hedwig, T, P> HedwigPublishBuilder<'hedwig, T, P> {
    /// Add a message to be published in a batch.
    pub fn message<D>(mut self, msg: Message<D, T>) -> Result<Self, PublishError>
    where
        D: Serialize,
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
            .to_schema(
                self.hedwig.publisher_name.clone(),
                schema_url,
                FORMAT_VERSION_V1,
            )
            .map_err(PublishError::SerializationError)?;
        let route = (self.hedwig.message_router)(data_type, converted.format_version.0)
            .ok_or_else(|| PublishError::RouteError(converted.id))?;
        self.messages.push((route, converted));
        Ok(self)
    }

    /// Publish all the messages.
    pub fn publish(self) -> Result<(), PublishError>
    where
        P: Publisher,
    {
        self.hedwig.publisher.publish(self.messages)
    }
}

/// A message builder.
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
    /// Construct a new message.
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

    /// Overwrite the header map associated with the message.
    ///
    /// This may be used to track the `request_id`, for example.
    pub fn headers(mut self, headers: Headers) -> Self {
        self.headers = Some(headers);
        self
    }

    /// Add a custom header to the message.
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

    /// Add custom id to the message. If not provided, a random UUID is generated.
    pub fn id(mut self, id: Uuid) -> Self {
        self.id = Some(id);
        self
    }

    fn to_schema(
        self,
        publisher_name: String,
        schema: String,
        format_version: Version,
    ) -> Result<MessageSchema, serde_json::Error>
    where
        D: Serialize,
    {
        Ok(MessageSchema {
            id: self.id.unwrap_or_else(Uuid::new_v4),
            metadata: MetadataSchema {
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
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct MetadataSchema {
    /// The timestamp when message was created in the publishing service
    pub timestamp: u128,

    /// Name of the publishing service
    pub publisher: String,

    /// Custom headers. This may be used to track request_id, for example.
    pub headers: Headers,
}

/// A validated message.
///
/// This data type is the schema or the json messages being sent over the wire.
#[derive(Debug, Serialize)]
pub struct MessageSchema {
    /// An unique message identifier.
    id: Uuid,
    /// The metadata associated with the message.
    metadata: MetadataSchema,
    /// URI of the schema validating this message.
    ///
    /// E.g. `https://hedwig.domain.xyz/schemas#/schemas/user.created/1.0`
    schema: String,
    /// Format of the message schema used.
    format_version: Version,
    /// The message data
    data: serde_json::Value,
}

#[cfg(test)]
mod tests {
    use super::*;

    use assert_matches::assert_matches;
    use strum_macros::IntoStaticStr;

    #[cfg(feature = "mock")]
    impl MockPublisher {
        /// Verify that a message was published. This method asserts that the message you expected to be published, was
        /// indeed published
        pub fn assert_message_published<D, T>(&self, message: &Message<D, T>, headers: &Headers)
        where
            D: Serialize + Clone,
            T: Copy + Into<&'static str>,
        {
            let published_messages = self.published_messages.borrow();
            let (published, published_headers) = published_messages
                .get(
                    &message
                        .id
                        .expect("asserted messages should have specified uuid"),
                )
                .expect("message not found");
            let data_type_str = message.data_type.into();
            let encoded = message.clone().to_schema(
                String::from("myapp"),
                format!(
                    "https://hedwig.standard.ai/schema#/schemas/{}/1.0",
                    data_type_str
                ),
                VERSION_1_0,
            );

            let serialized = serde_json::to_string(&encoded.unwrap()).unwrap();
            assert_eq!(published, &serialized);
            assert_eq!(published_headers, headers);
        }
    }

    #[derive(Clone, Copy, Debug, IntoStaticStr, Hash, PartialEq, Eq)]
    enum MessageType {
        #[strum(serialize = "user.created")]
        UserCreated,

        #[strum(serialize = "invalid.schema")]
        InvalidSchema,

        #[strum(serialize = "invalid.route")]
        InvalidRoute,
    }

    #[derive(Clone, Debug, Serialize, PartialEq)]
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

    #[cfg(feature = "mock")]
    fn mock_hedwig() -> Hedwig<MessageType, MockPublisher> {
        Hedwig::new(SCHEMA, "myapp", MockPublisher::default(), router).unwrap()
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
        .header("request_id", &request_id);
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

    #[test]
    #[cfg(feature = "mock")]
    fn publish() {
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
        .header("request_id", &request_id)
        .id(msg_id);
        custom_headers.insert("request_id".to_owned(), request_id);
        hedwig
            .start_publish()
            .message(message.clone())
            .unwrap()
            .publish()
            .unwrap();
        hedwig
            .publisher
            .assert_message_published(&message, &custom_headers);
    }

    #[test]
    #[cfg(feature = "google")]
    fn google_publisher_credentials_error() {
        let r = GooglePublisher::new("path does not exist", "myproject".into());
        assert_matches!(r.err(), Some(HedwigError::CredentialsIOError(_)));
    }

    #[test]
    fn validator_deserialization_error() {
        let r = Validator::new("bad json");
        assert_matches!(r.err(), Some(HedwigError::DeserializationError(_)));
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
        assert_matches!(r.err(), Some(HedwigError::SchemaCompileError(_)));
    }

    #[test]
    #[cfg(feature = "mock")]
    fn message_serialization_error() {
        let hedwig = mock_hedwig();
        #[derive(Serialize)]
        struct BadUserCreatedData {
            user_ids: HashMap<Vec<i32>, String>,
        };
        let mut user_ids = HashMap::new();
        user_ids.insert(vec![32, 64], "U_123".to_owned());
        let data = BadUserCreatedData { user_ids };
        let m = Message::new(MessageType::UserCreated, VERSION_1_0, data);
        let r = hedwig.start_publish().message(m);
        assert_matches!(r.err(), Some(PublishError::SerializationError(_)));
    }

    #[test]
    #[cfg(feature = "mock")]
    fn message_router_error() {
        let hedwig = mock_hedwig();
        let m = Message::new(MessageType::InvalidRoute, VERSION_1_0, ());
        let r = hedwig.start_publish().message(m);
        assert_matches!(r.err(), Some(PublishError::RouteError(_)));
    }

    #[test]
    #[cfg(feature = "mock")]
    fn message_invalid_schema_error() {
        let hedwig = mock_hedwig();
        let m = Message::new(MessageType::InvalidSchema, VERSION_1_0, ());
        let r = hedwig.start_publish().message(m);
        assert_matches!(r.err(), Some(PublishError::UnresolvableSchemaUrl(_)));
    }

    #[test]
    #[cfg(feature = "mock")]
    fn message_data_validation_eror() {
        let hedwig = mock_hedwig();
        #[derive(Serialize)]
        struct BadUserCreatedData {
            user_ids: Vec<i32>,
        };
        let data = BadUserCreatedData { user_ids: vec![1] };
        let m = Message::new(MessageType::UserCreated, VERSION_1_0, data);
        let r = hedwig.start_publish().message(m);
        assert_matches!(r.err(), Some(PublishError::DataValidationError(_)));
    }
}
