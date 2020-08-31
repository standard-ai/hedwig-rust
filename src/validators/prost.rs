#![cfg(feature = "prost")]

use std::time::SystemTime;
use uuid::Uuid;

use crate::{Headers, ValidatedMessage};

/// Errors that may occur when validating ProtoBuf messages.
#[derive(thiserror::Error, Debug)]
#[error("unable to encode the protobuf payload")]
#[cfg_attr(docsrs, doc(cfg(feature = "prost")))]
pub struct ProstValidatorError(#[source] prost::EncodeError);

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
    pub fn validate<M: prost::Message>(
        &self,
        id: Uuid,
        timestamp: SystemTime,
        schema: &'static str,
        headers: Headers,
        data: &M,
    ) -> Result<ValidatedMessage, ProstValidatorError> {
        let mut bytes = Vec::new();
        data.encode(&mut bytes).map_err(ProstValidatorError)?;
        Ok(ValidatedMessage {
            id,
            schema,
            headers,
            timestamp,
            data: bytes,
        })
    }
}
