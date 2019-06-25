# Hedwig library for Rust

[![Build Status](https://travis-ci.com/standard-ai/hedwig-rust.svg?branch=master)](https://travis-ci.com/standard-ai/hedwig-rust)
[![Latest Version](https://img.shields.io/crates/v/hedwig.svg?style=flat-square)](https://crates.io/crates/hedwig)

## What is it?

Hedwig is a inter-service communication bus that works on AWS and GCP, while keeping things pretty simple and
straight forward. It uses [json schema](http://json-schema.org/) [draft v4](http://json-schema.org/specification-links.html#draft-4)
for schema validation so all incoming and outgoing messages are validated against pre-defined schema.

Hedwig allows separation of concerns between consumers and publishers so your services are loosely coupled, and the
contract is enforced by the schema validation. Hedwig may also be used to build asynchronous APIs.

Support exists for [Python](https://github.com/Automatic/hedwig-python) and [Golang](https://github.com/Automatic/hedwig-go).

For intra-service messaging, see [Taskhawk](https://github.com/Automatic/taskhawk-python).

## Quick Start

### Installation

Add to Cargo.toml:
```
[dependencies]
hedwig = "*"
```

### Usage

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

There's also a [full running example](examples/publish.rs) in the repo.

## Getting Help

We use GitHub issues for tracking bugs and feature requests.

* If it turns out that you may have found a bug, please [open an issue](https://github.com/standard-ai/hedwig-rust/issues/new)
