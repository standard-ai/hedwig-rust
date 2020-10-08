use std::time::SystemTime;
use uuid::Uuid;
use valico::json_schema::Scope;

use crate::{Headers, ValidatedMessage};

/// Errors that may occur when validating messages using a JSON schema.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
#[cfg_attr(docsrs, doc(cfg(feature = "json-schema")))]
pub enum JsonSchemaValidatorError {
    /// Unable to deserialize the schema
    #[error("unable to deserialize the schema")]
    SchemaDeserialize(#[source] serde_json::Error),
    /// Unable to compile the schema
    #[error("unable to compile the schema")]
    SchemaCompile(#[source] valico::json_schema::SchemaError),
    /// Could not parse a schema URL
    #[error("could not parse `{1}` as a schema URL")]
    SchemaUrlParse(#[source] url::ParseError, String),
    /// Could not resolve the schema URL
    #[error("could not resolve `{0}` to a schema")]
    SchemaUrlResolve(url::Url),
    /// Could not serialize message data
    #[error("could not serialize the message data")]
    SerializeData(#[source] serde_json::Error),
    /// Could not validate message data
    #[error("message data does not validate per the schema: {0}")]
    ValidateData(String),
}

/// Validator that validates JSON payloads according to a provided [JSON Schema].
///
/// [JSON Schema]: https://json-schema.org/
#[cfg_attr(docsrs, doc(cfg(feature = "json-schema")))]
pub struct JsonSchemaValidator {
    scope: Scope,
}

impl JsonSchemaValidator {
    /// Construct a new JSON schema validator.
    ///
    /// The `schema` argument must contain the JSON-encoded JSON-schema.
    pub fn new(schema: &str) -> Result<JsonSchemaValidator, JsonSchemaValidatorError> {
        Self::from_reader(std::io::Cursor::new(schema))
    }

    /// Construct a new JSON schema validator.
    pub fn from_reader<R>(schema: R) -> Result<JsonSchemaValidator, JsonSchemaValidatorError>
    where
        R: std::io::Read,
    {
        Self::from_json(
            serde_json::from_reader(schema).map_err(JsonSchemaValidatorError::SchemaDeserialize)?,
        )
    }

    /// Construct a new JSON schema validator.
    pub fn from_json(
        schema: serde_json::Value,
    ) -> Result<JsonSchemaValidator, JsonSchemaValidatorError> {
        let mut scope = Scope::new();
        scope
            .compile(schema, false)
            .map_err(JsonSchemaValidatorError::SchemaCompile)?;
        Ok(JsonSchemaValidator { scope })
    }

    /// Validate the JSON payload using JSON schema and construct a [`ValidatedMessage`].
    pub fn validate<M: serde::Serialize>(
        &self,
        id: Uuid,
        timestamp: SystemTime,
        schema: &'static str,
        headers: Headers,
        data: &M,
    ) -> Result<ValidatedMessage, JsonSchemaValidatorError> {
        // convert user.created/1.0 -> user.created/1.*
        let msg_schema_trimmed = schema.trim_end_matches(char::is_numeric);
        let msg_schema_url = if msg_schema_trimmed != schema {
            let wildcard_url = String::from(msg_schema_trimmed) + "*";
            url::Url::parse(&wildcard_url)
                .map_err(|e| JsonSchemaValidatorError::SchemaUrlParse(e, wildcard_url))?
        } else {
            url::Url::parse(&schema)
                .map_err(|e| JsonSchemaValidatorError::SchemaUrlParse(e, schema.into()))?
        };
        let msg_schema = self
            .scope
            .resolve(&msg_schema_url)
            .ok_or(JsonSchemaValidatorError::SchemaUrlResolve(msg_schema_url))?;
        let value = serde_json::to_value(data).map_err(JsonSchemaValidatorError::SerializeData)?;
        let validation_state = msg_schema.validate(&value);
        if !validation_state.is_strictly_valid() {
            return Err(JsonSchemaValidatorError::ValidateData(format!(
                "{:?}",
                validation_state
            )));
        }
        Ok(ValidatedMessage {
            id,
            schema,
            headers,
            timestamp,
            data: serde_json::to_vec(&value).unwrap(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{tests::*, Message};
    use uuid::Uuid;

    #[test]
    fn invalid_data_type() {
        let validator = JsonSchemaValidator::new(SCHEMA).unwrap();
        let message = JsonUserCreatedMessage {
            uuid: Uuid::new_v4(),
            schema: "https://hedwig.corp/schema#/schemas/user.created/1.0",
            user_id: 123u64,
            time: SystemTime::now(),
            headers: Headers::new(),
        };
        assert!(matches!(
            message.encode(&validator).err(),
            Some(JsonSchemaValidatorError::ValidateData(_))
        ));
    }

    #[test]
    fn missing_schema() {
        let validator = JsonSchemaValidator::new(SCHEMA).unwrap();
        let message = JsonUserCreatedMessage {
            uuid: Uuid::new_v4(),
            schema: "https://hedwig.corp/schema#/schemas/user.created/2.0",
            user_id: String::from("123"),
            time: SystemTime::now(),
            headers: Headers::new(),
        };
        assert!(matches!(
            message.encode(&validator).err(),
            Some(JsonSchemaValidatorError::SchemaUrlResolve(_))
        ));
    }

    #[test]
    fn overbroad_schema_url() {
        let validator = JsonSchemaValidator::new(SCHEMA).unwrap();
        let message = JsonUserCreatedMessage {
            uuid: Uuid::new_v4(),
            schema: "https://hedwig.corp/schema#/schemas/user.created/*",
            user_id: String::from("123"),
            time: SystemTime::now(),
            headers: Headers::new(),
        };
        assert!(matches!(
            message.encode(&validator).err(),
            Some(JsonSchemaValidatorError::SchemaUrlResolve(_))
        ));
    }

    #[test]
    fn exact_schema_url_wildcard() {
        let validator = JsonSchemaValidator::new(SCHEMA).unwrap();
        let message = JsonUserCreatedMessage {
            uuid: Uuid::new_v4(),
            schema: "https://hedwig.corp/schema#/schemas/user.created/1.*",
            user_id: String::from("123"),
            time: SystemTime::now(),
            headers: Headers::new(),
        };
        message.encode(&validator).expect("should work");
    }

    #[test]
    fn invalid_schema_url() {
        let validator = JsonSchemaValidator::new(SCHEMA).unwrap();
        let message = JsonUserCreatedMessage {
            uuid: Uuid::new_v4(),
            schema: "hedwig.corp/schema#/schemas/user.created/1.*",
            user_id: String::from("123"),
            time: SystemTime::now(),
            headers: Headers::new(),
        };
        assert!(matches!(
            message.encode(&validator).err(),
            Some(JsonSchemaValidatorError::SchemaUrlParse(..))
        ));
    }

    #[test]
    fn errors_send_sync() {
        assert_error::<JsonSchemaValidatorError>();
    }

    #[test]
    fn validation_retains_timestamp() {
        let validator = JsonSchemaValidator::new(SCHEMA).unwrap();
        let timestamp = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(42);
        let validated = validator
            .validate(
                Uuid::new_v4(),
                timestamp,
                "https://hedwig.corp/schema#/schemas/user.created/1.*",
                Headers::new(),
                &serde_json::json!({ "user_id": "123" }),
            )
            .expect("ok");
        assert_eq!(validated.timestamp(), &timestamp);
    }

    #[test]
    fn validation_retains_headers() {
        let validator = JsonSchemaValidator::new(SCHEMA).unwrap();
        let headers = vec![("hello", "world"), ("123", "456")]
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect::<Headers>();
        let validated = validator
            .validate(
                Uuid::new_v4(),
                SystemTime::UNIX_EPOCH,
                "https://hedwig.corp/schema#/schemas/user.created/1.*",
                headers.clone(),
                &serde_json::json!({ "user_id": "123" }),
            )
            .expect("ok");
        assert_eq!(validated.headers(), &headers);
    }

    #[test]
    fn validation_retains_uuid() {
        let validator = JsonSchemaValidator::new(SCHEMA).unwrap();
        let uuid = Uuid::new_v4();
        let validated = validator
            .validate(
                uuid,
                SystemTime::UNIX_EPOCH,
                "https://hedwig.corp/schema#/schemas/user.created/1.*",
                Headers::new(),
                &serde_json::json!({ "user_id": "123" }),
            )
            .expect("ok");
        assert_eq!(validated.uuid(), &uuid);
    }
}
