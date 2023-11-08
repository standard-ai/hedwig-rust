//! Message related types

use bytes::Bytes;
use std::{borrow::Cow, time::SystemTime};
use uuid::Uuid;

use crate::Headers;

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
