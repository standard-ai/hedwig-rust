use futures_util::stream::StreamExt;
use hedwig::{publishers::GooglePubSubPublisher, Headers, Message, Publisher};
use std::{env, sync::Arc, time::SystemTime};

#[derive(serde::Serialize)]
struct UserCreatedMessage {
    #[serde(skip)]
    uuid: uuid::Uuid,
    user_id: String,
}

impl<'a> Message for &'a UserCreatedMessage {
    type Error = hedwig::validators::JsonSchemaValidatorError;
    type Validator = hedwig::validators::JsonSchemaValidator;
    fn topic(&self) -> &'static str {
        "user.created"
    }
    fn encode(self, validator: &Self::Validator) -> Result<hedwig::ValidatedMessage, Self::Error> {
        Ok(validator
            .validate(
                self.uuid,
                SystemTime::now(),
                "https://hedwig.corp/schema#/schemas/user.created/1.0",
                Headers::new(),
                self,
            )
            .unwrap())
    }
}

const PUBLISHER: &str = "myapp";

const SCHEMA: &str = r#"{
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

async fn run() -> Result<(), Box<dyn std::error::Error + 'static>> {
    let google_project =
        env::var("GOOGLE_CLOUD_PROJECT").expect("env var GOOGLE_CLOUD_PROJECT is required");
    let google_credentials = env::var("GOOGLE_APPLICATION_CREDENTIALS")
        .expect("env var GOOGLE_APPLICATION_CREDENTIALS is required");
    let secret = yup_oauth2::read_service_account_key(google_credentials)
        .await
        .expect("$GOOGLE_APPLICATION_CREDENTIALS is not a valid service account key");

    let client = hyper::Client::builder().build(hyper_tls::HttpsConnector::new());
    let authenticator = Arc::new(
        yup_oauth2::ServiceAccountAuthenticator::builder(secret)
            .hyper_client(client.clone())
            .build()
            .await
            .expect("could not create an authenticator"),
    );

    let publisher = GooglePubSubPublisher::new(
        PUBLISHER.into(),
        google_project.into(),
        client,
        authenticator,
    );
    let validator = hedwig::validators::JsonSchemaValidator::new(SCHEMA).unwrap();
    let message = UserCreatedMessage {
        uuid: uuid::Uuid::new_v4(),
        user_id: "U_123".into(),
    };
    let topic = Message::topic(&&message);
    let validated = message.encode(&validator).unwrap();
    let mut publish = publisher.publish(topic, [validated].iter());
    while let Some(r) = publish.next().await {
        println!("publish result: {:?}", r?);
    }

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
