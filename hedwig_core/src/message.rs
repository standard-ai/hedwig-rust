//! Message related types

use bytes::Bytes;
use std::{borrow::Cow, time::SystemTime};
use uuid::Uuid;

use crate::{Headers, Topic};

/// A validated message.
///
/// These are created by validators after encoding a user message, or when pulling messages from
/// the message service.
#[derive(Debug, Clone)]
// derive Eq only in tests so that users can't foot-shoot an expensive == over data
#[cfg_attr(test, derive(PartialEq, Eq))]
pub struct ValidatedMessage<M> {
    /// Unique message identifier.
    pub(crate) id: Uuid,
    /// The timestamp when message was created in the publishing service.
    pub(crate) timestamp: SystemTime,
    /// URI of the schema validating this message.
    ///
    /// E.g. `https://hedwig.domain.xyz/schemas#/schemas/user.created/1.0`
    pub(crate) schema: Cow<'static, str>,
    /// Custom message headers.
    ///
    /// This may be used to track request_id, for example.
    pub(crate) headers: Headers,
    /// The message data.
    pub(crate) data: M,
}

impl ValidatedMessage<Bytes> {
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
}

impl<M> ValidatedMessage<M> {
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

    /// The message data.
    pub fn data(&self) -> &M {
        &self.data
    }

    /// Destructure this message into just the contained data
    pub fn into_data(self) -> M {
        self.data
    }
}

/// Messages which can be decoded from a [`ValidatedMessage`] stream.
pub trait DecodableMessage {
    /// The error returned when a message fails to decode
    type Error;

    /// The decoder used to decode a validated message
    type Decoder;

    /// Decode the given message, using the given decoder, into its structured type
    fn decode(msg: ValidatedMessage<Bytes>, decoder: &Self::Decoder) -> Result<Self, Self::Error>
    where
        Self: Sized;
}

impl<M> DecodableMessage for ValidatedMessage<M>
where
    M: DecodableMessage,
{
    /// The error returned when a message fails to decode
    type Error = M::Error;

    /// The decoder used to decode a validated message
    type Decoder = M::Decoder;

    /// Decode the given message, using the given decoder, into its structured type
    fn decode(msg: ValidatedMessage<Bytes>, decoder: &Self::Decoder) -> Result<Self, Self::Error>
    where
        Self: Sized,
    {
        let message = M::decode(msg.clone(), decoder)?;
        Ok(Self {
            id: msg.id,
            timestamp: msg.timestamp,
            schema: msg.schema,
            headers: msg.headers,
            data: message,
        })
    }
}

/// Types that can be encoded and published.
pub trait EncodableMessage {
    /// The errors that can occur when calling the [`EncodableMessage::encode`] method.
    ///
    /// Will typically match the errors returned by the [`EncodableMessage::Validator`].
    type Error;

    /// The validator to use for this message.
    type Validator;

    /// Topic into which this message shall be published.
    fn topic(&self) -> Topic;

    /// Encode the message payload.
    fn encode(&self, validator: &Self::Validator) -> Result<ValidatedMessage<Bytes>, Self::Error>;
}
