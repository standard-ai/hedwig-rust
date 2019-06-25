# hedwig-rust
Hedwig library for Rust

Usage:
```rust
    let schema = r#"
{
  "$id": "https://hedwig.standard.ai/schema",
  "$schema": "http://json-schema.org/draft-04/schema#",
  "description": "Example Schema",
  "schemas": {
      "user-created": {
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

    fn router(t: &MessageType, v: &MajorVersion) -> Option<&'static str> {
        match (t, v) {
            (&MessageType::UserCreated, &MajorVersion(1)) => Some("dev-user-created-v1"),
            _ => None,
        }
    }

    let hedwig = Hedwig::new(
        schema,
        "myapp",
        google_credentials,
        google_project,
        router,
    )?;

    let message = hedwig.message(
        MessageType::UserCreated,
        Version(MajorVersion(1), MinorVersion(0)),
        UserCreatedData { user_id: "U_123" },
    )?;

    let message_id = message.id;

    let publish_id = hedwig.publish(message)?;
    println!(
        "Published message {} with pubsub id: {}",
        message_id, publish_id
    );
```

See [the full running example](examples/publish.rs) in the repo.
