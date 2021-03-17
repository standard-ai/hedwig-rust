#![cfg(feature = "prost")]

use std::time::SystemTime;
use uuid::Uuid;

use crate::{Headers, ValidatedMessage};

/// Errors that may occur when validating ProtoBuf messages.
#[derive(thiserror::Error, Debug)]
#[error("unable to encode the protobuf payload")]
#[cfg_attr(docsrs, doc(cfg(feature = "prost")))]
pub struct ProstValidatorError(#[source] prost::EncodeError);

/// Errors that may occur when decoding ProtoBuf messages.
#[derive(thiserror::Error, Debug, PartialEq, Eq)]
#[cfg_attr(docsrs, doc(cfg(feature = "prost")))]
pub enum ProstDecodeError<E: std::error::Error + 'static> {
    /// The message's schema did not match the decoded message type
    #[error("invalid schema for decoded message type")]
    InvalidSchema(#[source] E),

    /// The message failed to decode from protobuf
    #[error(transparent)]
    Decode(#[from] prost::DecodeError),
}

#[derive(Default)]
struct UseNewToConstruct;

/// Validator that encodes data into protobuf payloads using [`prost`].
#[derive(Default)]
#[cfg_attr(docsrs, doc(cfg(feature = "prost")))]
pub struct ProstValidator(UseNewToConstruct);

impl ProstValidator {
    /// Construct a new validator.
    pub fn new() -> Self {
        ProstValidator(UseNewToConstruct)
    }

    /// Validate and construct a [`ValidatedMessage`] with a protobuf payload.
    pub fn validate<M, S>(
        &self,
        id: Uuid,
        timestamp: SystemTime,
        schema: S,
        headers: Headers,
        data: &M,
    ) -> Result<ValidatedMessage, ProstValidatorError>
    where
        M: prost::Message,
        S: Into<std::borrow::Cow<'static, str>>,
    {
        let mut bytes = bytes::BytesMut::new();
        data.encode(&mut bytes).map_err(ProstValidatorError)?;
        Ok(ValidatedMessage::new(id, timestamp, schema, headers, bytes))
    }
}

/// Validator that decodes data from protobuf payloads using [`prost`].
pub struct ProstDecoder<S> {
    schema_matcher: S,
}

impl<S> ProstDecoder<S> {
    /// Create a new decoder with the given [`SchemaMatcher`]
    pub fn new(schema_matcher: S) -> Self {
        Self { schema_matcher }
    }

    /// Decode the given protobuf-encoded message into its structured data
    pub fn decode<M>(
        &self,
        msg: ValidatedMessage,
    ) -> Result<M, ProstDecodeError<S::InvalidSchemaError>>
    where
        S: SchemaMatcher<M>,
        M: prost::Message + Default,
    {
        self.schema_matcher
            .try_match_schema(msg.schema())
            .map_err(ProstDecodeError::InvalidSchema)?;

        Ok(M::decode(msg.data)?)
    }
}

/// A means of asserting that an incoming message's [`schema`](ValidatedMessage::schema) matches
/// a given message type's deserialized format.
///
/// For example, an implementation could check that `struct MyMessage { ... }` is a valid
/// deserialization for an incoming message with schemas like `"my_message_v1.proto"` or
/// `"my_message_v2.proto"`
pub trait SchemaMatcher<MessageType> {
    /// The error returned when a given schema does not match the message type
    type InvalidSchemaError: std::error::Error + Send + Sync + 'static;

    /// Check whether messages with the given schema are valid for deserializing into the trait's
    /// generic message type.
    ///
    /// Returns an error if the schema does not match
    fn try_match_schema(&self, schema: &str) -> Result<(), Self::InvalidSchemaError>;
}

// blanket impl SchemaMatcher over closures for convenience
impl<T, F, E> SchemaMatcher<T> for F
where
    F: Fn(&str) -> Result<(), E>,
    E: std::error::Error + Send + Sync + 'static,
{
    type InvalidSchemaError = E;

    fn try_match_schema(&self, schema: &str) -> Result<(), Self::InvalidSchemaError> {
        (self)(schema)
    }
}

/// An error indicating that a received message had a schema which did not match the deserialized
/// message type
#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
#[error("deserialized schema {encountered} does not match expected schema {expected} for type {message_type}")]
pub struct SchemaMismatchError {
    expected: &'static str,
    encountered: String,
    message_type: &'static str,
}

impl SchemaMismatchError {
    /// Create a new error for the given message type
    pub fn new<MessageType>(expected: &'static str, encountered: String) -> Self {
        SchemaMismatchError {
            expected,
            encountered,
            message_type: std::any::type_name::<MessageType>(),
        }
    }
}

/// A [`SchemaMatcher`] which expects all incoming schemas to match exactly one string for the
/// given message type
pub struct ExactSchemaMatcher<T> {
    expected_schema: &'static str,
    _message_type: std::marker::PhantomData<fn(T)>, // <fn(T)> instead of <T> to make Send + Sync unconditional
}

impl<T> ExactSchemaMatcher<T> {
    /// Create a new schema matcher with the given expected schema
    pub fn new(expected_schema: &'static str) -> Self {
        Self {
            expected_schema,
            _message_type: std::marker::PhantomData,
        }
    }
}

impl<T> SchemaMatcher<T> for ExactSchemaMatcher<T> {
    type InvalidSchemaError = SchemaMismatchError;

    fn try_match_schema(&self, schema: &str) -> Result<(), Self::InvalidSchemaError> {
        if self.expected_schema == schema {
            Ok(())
        } else {
            Err(SchemaMismatchError::new::<T>(
                self.expected_schema,
                schema.to_owned(),
            ))
        }
    }
}
