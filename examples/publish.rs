use std::env;

use failure;
use hedwig::{GooglePublisher, Headers, Hedwig, MajorVersion, MinorVersion, Version};
use serde::Serialize;
use strum_macros::IntoStaticStr;

#[derive(Clone, Copy, Debug, IntoStaticStr, Hash, PartialEq, Eq)]
pub enum MessageType {
    #[strum(serialize = "user.created")]
    UserCreated,
}

#[derive(Serialize)]
struct UserCreatedData {
    user_id: String,
}

const VERSION_1_0: Version = Version(MajorVersion(1), MinorVersion(0));

const PUBLISHER: &str = "myapp";

fn router(t: MessageType, v: MajorVersion) -> Option<&'static str> {
    match (t, v) {
        (MessageType::UserCreated, MajorVersion(1)) => Some("dev-user-created-v1"),
        _ => None,
    }
}

fn main() -> Result<(), failure::Error> {
    let google_credentials = env::var("GOOGLE_APPLICATION_CREDENTIALS")
        .expect("env var GOOGLE_APPLICATION_CREDENTIALS is required");
    let google_project =
        env::var("GOOGLE_CLOUD_PROJECT").expect("env var GOOGLE_CLOUD_PROJECT is required");

    let schema = r#"
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

    let publisher = GooglePublisher::new(google_credentials, google_project)?;

    let hedwig = Hedwig::new(schema, PUBLISHER, publisher, router)?;

    let mut headers = Headers::new();
    headers.insert("request_id".to_string(), uuid::Uuid::new_v4().to_string());

    let data = UserCreatedData {
        user_id: "U_123".into(),
    };

    let mut message = hedwig.message(MessageType::UserCreated, VERSION_1_0, data)?;
    message.with_headers(headers);

    let message_id = message.id;

    let publish_id = hedwig.publish(message)?;

    println!(
        "Published message {} with pubsub id: {}",
        message_id, publish_id
    );

    Ok(())
}
