use std::env;

use hedwig::{
    publishers::GooglePubSubPublisher, Hedwig, MajorVersion, Message, MinorVersion, Version,
};
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

const SCHEMA: &str = r#"{
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

fn router(t: MessageType, v: MajorVersion) -> Option<&'static str> {
    match (t, v) {
        (MessageType::UserCreated, MajorVersion(1)) => Some("dev-user-created-v1"),
        _ => None,
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error + 'static>> {
    let google_project =
        env::var("GOOGLE_CLOUD_PROJECT").expect("env var GOOGLE_CLOUD_PROJECT is required");
    let google_credentials = env::var("GOOGLE_APPLICATION_CREDENTIALS")
        .expect("env var GOOGLE_APPLICATION_CREDENTIALS is required");
    let secret = yup_oauth2::read_service_account_key(google_credentials)
        .await
        .expect("$GOOGLE_APPLICATION_CREDENTIALS is not a valid service account key");

    let client = hyper::Client::builder().build(hyper_openssl::HttpsConnector::new()?);
    let authenticator = yup_oauth2::ServiceAccountAuthenticator::builder(secret)
        .hyper_client(client.clone())
        .build()
        .await
        .expect("could not create an authenticator");

    let publisher = GooglePubSubPublisher::new(google_project, client, authenticator);

    let hedwig = Hedwig::new(SCHEMA, PUBLISHER, publisher, router)?;

    let data = UserCreatedData {
        user_id: "U_123".into(),
    };

    let message_id = uuid::Uuid::new_v4();
    let mut builder = hedwig.build_batch();
    builder.message(
        Message::new(MessageType::UserCreated, VERSION_1_0, data)
            .id(message_id)
            .header("request_id", uuid::Uuid::new_v4().to_string()),
    )?;

    println!("Published messages {:?}", builder.publish().await?);

    Ok(())
}

fn main() {
    let mut rt = tokio::runtime::Builder::new()
        .basic_scheduler()
        .enable_all()
        .build()
        .expect("runtime builds");
    match rt.block_on(run()) {
        Ok(_) => std::process::exit(0),
        Err(e) => {
            eprintln!("error: {}", e);
            let mut source = e.source();
            while let Some(src) = source {
                eprintln!("  caused by: {}", src);
                source = src.source();
            }
            std::process::exit(1);
        }
    }
}
