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

use std::collections::HashMap;
use std::convert::From;
use std::default::Default;
use std::fmt;
use std::io;
use std::path::Path;
use std::result::Result;
use std::time::{SystemTime, UNIX_EPOCH};

use base64;
use failure::Fail;
use google_pubsub1::{self as pubsub1, Pubsub};
use hyper::{self, net::HttpsConnector, Client};
use hyper_rustls;
use serde::Serialize;
use serde_json;
use uuid::Uuid;
use valico::json_schema::{SchemaError, Scope, ValidationState};
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
    SchemaCompileError(#[cause] SchemaError),
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

    #[fail(display = "Router failed to route message correctly")]
    RouterError(&'static str),

    #[fail(display = "Invalid from publish API: can't find published message id")]
    InvalidResponseNoMessageId(&'static str),
}

struct GooglePublisher {
    client: Pubsub<Client, oauth2::ServiceAccountAccess<Client>>,
}

impl GooglePublisher {
    fn new(google_application_credentials: &Path) -> Result<GooglePublisher, HedwigError> {
        let client_secret = oauth2::service_account_key_from_file(
            &google_application_credentials.to_string_lossy().into(),
        )
        .map_err(HedwigError::CredentialsIOError)?;
        let auth_https = HttpsConnector::new(hyper_rustls::TlsClient::new());
        let auth_client = hyper::Client::with_connector(auth_https);

        let access = oauth2::ServiceAccountAccess::new(client_secret, auth_client);

        let https = HttpsConnector::new(hyper_rustls::TlsClient::new());
        let pubsub_client = hyper::Client::with_connector(https);

        let client = Pubsub::new(pubsub_client, access);

        Ok(GooglePublisher { client })
    }

    fn publish<D, T>(
        &self,
        message: Message<D, T>,
        hedwig: &Hedwig<T>,
    ) -> Result<String, PublishError>
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

        let request = pubsub1::PublishRequest {
            messages: Some(vec![pubsub_message]),
        };
        let result = self
            .client
            .projects()
            .topics_publish(request, message.topic(hedwig)?.as_str())
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
/// This may be implemented using a Hashmap
pub type MessageRouter<T> = fn(&T, &MajorVersion) -> Option<&'static str>;

/// Central instance to access all Hedwig related resources
///
/// # Examples
///
/// Publish a new message
///
/// ```no_run
/// use hedwig::{Hedwig, MajorVersion, MinorVersion, Version};
/// # use std::path::Path;
/// # use serde::Serialize;
/// # use strum_macros::IntoStaticStr;
///
/// fn main() -> Result<(), failure::Error> {
/// let schema = r#"
///     {
///       "$id": "https://hedwig.standard.ai/schema",
///       "$schema": "http://json-schema.org/draft-04/schema#",
///       "description": "Example Schema",
///       "schemas": {
///           "user-created": {
///               "1.*": {
///                   "description": "A new user was created",
///                   "type": "object",
///                   "x-versions": [
///                       "1.0"
///                   ],
///                   "required": [
///                       "user_id"
///                   ],
///                   "properties": {
///                       "user_id": {
///                           "$ref": "https://hedwig.standard.ai/schema#/definitions/UserId/1.0"
///                       }
///                   }
///               }
///           }
///       },
///       "definitions": {
///           "UserId": {
///               "1.0": {
///                   "type": "string"
///               }
///           }
///       }
///     }"#;
///
///     # #[derive(Clone, Copy, IntoStaticStr, Hash, PartialEq, Eq)]
///     # enum MessageType {
///     #    #[strum(serialize = "user.created")]
///     #    UserCreated,
///     # }
///     #
///     # #[derive(Serialize)]
///     # struct UserCreatedData {
///     #     user_id: String,
///     # }
///     #
///     fn router(t: &MessageType, v: &MajorVersion) -> Option<&'static str> {
///         match (t, v) {
///             (&MessageType::UserCreated, &MajorVersion(1)) => Some("dev-user-created-v1"),
///             _ => None,
///         }
///     }
///
///     let hedwig = Hedwig::new(
///         schema,
///         "myapp",
///         String::from("/home/.google-key.json"),
///         "myproject".into(),
///         router,
///     )?;
///
///     let message = hedwig.message(
///         MessageType::UserCreated,
///         Version(MajorVersion(1), MinorVersion(0)),
///         UserCreatedData { user_id: "U_123".into() },
///     )?;
///
///     hedwig.publish(message)?;
///     # Ok(())
/// # }
/// ```
#[allow(missing_debug_implementations)]
pub struct Hedwig<T> {
    validator: Validator,
    publisher_name: String,
    google_cloud_project: String,
    message_router: MessageRouter<T>,
    publisher: GooglePublisher,
}

impl<T> Hedwig<T> {
    /// Creates a new Hedwig instance. Application credentials cannot be auto-discovered in Google Cloud environment
    /// unlike the Python library.
    ///
    /// # Arguments
    ///
    /// * schema: The JSON schema content. It's up to the caller to read the schema from a file.
    /// * publisher name: Name of the publisher service. This will be part of the metadata in the message.
    /// * google_application_credentials - Path to the google credentials file.
    /// * google_cloud_project - The Google Cloud project where your pubsub resources lie - this /may be/ different
    /// from the credentials.
    /// * message_router - A function that can route messages to topics
    pub fn new<P: AsRef<Path>>(
        schema: &str,
        publisher_name: &str,
        google_application_credentials: P,
        google_cloud_project: String,
        message_router: MessageRouter<T>,
    ) -> Result<Hedwig<T>, HedwigError> {
        Ok(Hedwig {
            validator: Validator::new(schema)?,
            publisher_name: String::from(publisher_name),
            google_cloud_project,
            message_router,
            publisher: GooglePublisher::new(google_application_credentials.as_ref())?,
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

    /// Publish a message using the configured publisher. Returns the Pubsub message id if successful.
    ///
    /// # Arguments
    ///
    /// * `message` - the message to publish
    pub fn publish<D>(&self, message: Message<D, T>) -> Result<String, PublishError>
    where
        D: Serialize,
        T: Copy + Eq,
    {
        self.publisher.publish(message, self)
    }
}

/// Additional metadata associated with a message
#[derive(Debug, Serialize)]
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
#[derive(Debug, Serialize)]
pub struct Message<D, T> {
    /// Message identifier. Automatically created by the library.
    pub id: Uuid,

    /// Metadata associated with the message
    pub metadata: Metadata,

    /// Message schema, e.g. http://hedwig.standard.ai/schemas#/schemas/user.created/1.0
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
}

impl<D, T> Message<D, T> {
    fn new(
        hedwig: &Hedwig<T>,
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
        };
        hedwig.validator.validate(&message)?;
        Ok(message)
    }

    /// Add custom headers to the message
    pub fn with_headers(&mut self, headers: HashMap<String, String>) -> &mut Self {
        (&mut self.metadata).headers = headers;
        self
    }

    /// Add custom id to the message
    pub fn with_id(&mut self, id: Uuid) -> &mut Self {
        self.id = id;
        self
    }

    fn headers(&self) -> HashMap<String, String> {
        self.metadata.headers.clone()
    }
}

impl<D, T> Message<D, T> {
    fn topic(&self, hedwig: &Hedwig<T>) -> Result<String, PublishError> {
        let topic = match (hedwig.message_router)(&self.data_type, &self.data_schema_version.0) {
            Some(t) => t,
            None => return Err(PublishError::RouterError("Topic not found")),
        };
        Ok(format!(
            "projects/{}/topics/hedwig-{}",
            hedwig.google_cloud_project, topic
        ))
    }
}

#[cfg(test)]
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

    fn mock_hedwig() -> Hedwig<MessageType> {
        Hedwig::new(
            SCHEMA,
            "myapp",
            String::from("tests/google-key.json"),
            "myproject".into(),
            router,
        )
        .unwrap()
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
        custom_headers.insert("request_id".to_owned(), "foo".to_owned());
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
            "foo",
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
}
