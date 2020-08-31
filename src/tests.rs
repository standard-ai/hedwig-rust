#![allow(unused)]

use crate::{validators, Headers, Message, ValidatedMessage};

use futures_util::stream::StreamExt;
use std::time::SystemTime;
use uuid::Uuid;

pub(crate) const SCHEMA: &str = r#"{
    "$id": "https://hedwig.corp/schema",
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
                        "$ref": "https://hedwig.corp/schema#/definitions/UserId/1.0"
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

#[derive(serde::Serialize)]
pub(crate) struct JsonUserCreatedMessage<I> {
    #[serde(skip)]
    pub(crate) uuid: uuid::Uuid,
    #[serde(skip)]
    pub(crate) schema: &'static str,
    #[serde(skip)]
    pub(crate) headers: Headers,
    #[serde(skip)]
    pub(crate) time: SystemTime,
    pub(crate) user_id: I,
}

impl JsonUserCreatedMessage<String> {
    pub(crate) fn new_valid<V: Into<String>>(id: V) -> Self {
        JsonUserCreatedMessage {
            uuid: Uuid::new_v4(),
            schema: "https://hedwig.corp/schema#/schemas/user.created/1.0",
            user_id: id.into(),
            headers: Default::default(),
            time: SystemTime::now(),
        }
    }
}

#[cfg(feature = "json-schema")]
impl<'a, I: serde::Serialize> Message for &'a JsonUserCreatedMessage<I> {
    type Error = validators::JsonSchemaValidatorError;
    type Validator = validators::JsonSchemaValidator;

    fn topic(&self) -> &'static str {
        "user.created"
    }
    fn encode(self, validator: &Self::Validator) -> Result<ValidatedMessage, Self::Error> {
        validator.validate(
            self.uuid,
            self.time,
            self.schema,
            self.headers.clone(),
            self,
        )
    }
}

pub(crate) fn assert_error<T: std::error::Error + Send + Sync + 'static>() {}
pub(crate) fn assert_send<T: Send>() {}
pub(crate) fn assert_send_val<T: Send>(_: &T) {}

#[tokio::test]
async fn publish_empty_batch() {
    let publisher = crate::publishers::MockPublisher::new();
    let batch = super::PublishBatch::new();
    let mut stream = batch.publish(&publisher);
    assert!(matches!(stream.next().await, None));
    assert!(publisher.is_empty());
}

#[cfg(feature = "json-schema")]
#[tokio::test]
async fn publish_batch() {
    let validator = crate::validators::JsonSchemaValidator::new(SCHEMA).unwrap();
    let publisher = crate::publishers::MockPublisher::new();
    let mut batch = super::PublishBatch::new();
    let message_one = JsonUserCreatedMessage::new_valid("U123");
    let message_two = JsonUserCreatedMessage::new_valid("U124");
    let message_three = JsonUserCreatedMessage::new_valid("U126");
    let message_invalid = JsonUserCreatedMessage {
        uuid: Uuid::new_v4(),
        schema: "https://hedwig.corp/schema#/schemas/user.created/1.0",
        user_id: 125u64,
        time: SystemTime::now(),
        headers: Headers::new(),
    };
    batch
        .message(&validator, &message_one)
        .expect("adding valid message");
    assert!(matches!(
        batch.message(&validator, &message_invalid).err(),
        Some(_)
    ));
    batch
        .message(&validator, &message_two)
        .expect("adding valid message");
    batch
        .message(&validator, &message_three)
        .expect("adding valid message");
    let mut stream = batch.publish(&publisher);
    // Stream should return the message ids that are actually being published.
    //
    // The ordering doesn't necessarily need to be preserved, but for the purpose of this test we
    // know that `MockPublisher` does.
    assert_eq!(stream.next().await.map(|x| x.0), Some(Ok(message_one.uuid)));
    assert_eq!(stream.next().await.map(|x| x.0), Some(Ok(message_two.uuid)));
    assert_eq!(
        stream.next().await.map(|x| x.0),
        Some(Ok(message_three.uuid))
    );
    assert_eq!(stream.next().await.map(|x| x.0), None);
    assert_eq!(publisher.len(), 3);
    publisher.assert_message_published("user.created", &message_one.uuid);
    publisher.assert_message_published("user.created", &message_two.uuid);
    publisher.assert_message_published("user.created", &message_three.uuid);
}

#[test]
fn publish_stream_is_send() {
    let publisher = crate::publishers::MockPublisher::new();
    let batch = super::PublishBatch::new();
    let stream = batch.publish(&publisher);
    assert_send_val(&stream);
}
