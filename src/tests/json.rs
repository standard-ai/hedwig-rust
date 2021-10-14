#![cfg(feature = "json-schema")]

use crate::{
    mock::MockPublisher, validators, validators::JsonSchemaValidatorError, Consumer,
    DecodableMessage, EncodableMessage, Headers, Publisher, Topic, ValidatedMessage,
};

use futures_util::{sink::SinkExt, stream::StreamExt};
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

#[derive(Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct JsonUserCreatedMessage<I> {
    #[serde(skip)]
    pub(crate) uuid: uuid::Uuid,
    #[serde(skip)]
    pub(crate) schema: &'static str,
    #[serde(skip)]
    pub(crate) headers: Headers,
    #[serde(skip, default = "SystemTime::now")]
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

impl<'a, I: serde::Serialize> EncodableMessage for &'a JsonUserCreatedMessage<I> {
    type Error = validators::JsonSchemaValidatorError;
    type Validator = validators::JsonSchemaValidator;

    fn topic(&self) -> Topic {
        "user.created".into()
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

impl DecodableMessage for JsonUserCreatedMessage<String> {
    type Error = serde_json::Error;
    type Decoder = ();

    fn decode(msg: ValidatedMessage, _: &()) -> Result<Self, Self::Error> {
        println!("{:?}", String::from_utf8_lossy(msg.data()));
        Ok(JsonUserCreatedMessage {
            uuid: *msg.uuid(),
            headers: msg.headers().clone(),
            schema: "https://hedwig.corp/schema#/schemas/user.created/1.0",
            time: *msg.timestamp(),
            ..serde_json::from_slice(msg.data())?
        })
    }
}

#[tokio::test]
async fn publish_messages() -> Result<(), Box<dyn std::error::Error>> {
    let publisher = MockPublisher::new();
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

    // prepare a consumer to read any sent messages
    let mut consumer = publisher
        .new_consumer((&message_one).topic(), "subscription1")
        .consume::<JsonUserCreatedMessage<String>>(());

    // publishing the message with a u64 id should error on trying to send
    let mut publish_sink = <MockPublisher as Publisher<&JsonUserCreatedMessage<u64>>>::publish_sink(
        publisher.clone(),
        crate::validators::JsonSchemaValidator::new(SCHEMA).unwrap(),
    );
    assert!(matches!(
        publish_sink.send(&message_invalid).await,
        Err(either::Either::Left(
            JsonSchemaValidatorError::ValidateData { .. }
        ))
    ));

    // publishing the type with string ids should work
    let mut publish_sink =
        <MockPublisher as Publisher<&JsonUserCreatedMessage<String>>>::publish_sink(
            publisher.clone(),
            crate::validators::JsonSchemaValidator::new(SCHEMA).unwrap(),
        );

    assert!(publish_sink.send(&message_one).await.is_ok());
    assert!(publish_sink.send(&message_two).await.is_ok());
    assert!(publish_sink.send(&message_three).await.is_ok());

    // The ordering doesn't necessarily need to be preserved, but for the purpose of this test we
    // know that `MockPublisher` does.
    assert_eq!(
        message_one,
        consumer.next().await.unwrap().unwrap().ack().await.unwrap()
    );

    Ok(())
}

#[test]
fn publish_sink_is_send() {
    let publisher = MockPublisher::new();
    let sink = <MockPublisher as Publisher<&JsonUserCreatedMessage<String>>>::publish_sink(
        publisher,
        crate::validators::JsonSchemaValidator::new(SCHEMA).unwrap(),
    );
    crate::tests::assert_send_val(&sink);
}
