use async_trait::async_trait;
use futures_util::stream;
use hedwig_core::Topic;
use pin_project::pin_project;
use redis::{
    aio::{ConnectionManager, MultiplexedConnection},
    streams::{StreamReadOptions, StreamReadReply},
    AsyncCommands, RedisResult,
};
use std::{
    pin::Pin,
    task::{Context, Poll},
    time::SystemTime,
};
use tracing::{info, trace, warn};

use crate::{
    redis::{PAYLOAD_KEY, SCHEMA_KEY},
    Headers, ValidatedMessage,
};

use super::{EncodedMessage, StreamName};

#[derive(Debug, Clone)]
pub struct ConsumerClient {
    client: redis::Client,
}

impl ConsumerClient {
    pub fn from_client(client: redis::Client) -> Self {
        ConsumerClient { client }
    }
}

async fn xgroup_create_mkstream(
    con: &mut MultiplexedConnection,
    stream_name: &StreamName,
    group_name: &GroupName,
) -> RedisResult<()> {
    // The special ID $ is the ID of the last entry in the stream
    let id = "$";

    info!(
        "xgroup_create_mkstream {} {}",
        &stream_name.0, &group_name.0
    );

    con.xgroup_create_mkstream(&stream_name.0, &group_name.0, id)
        .await
}

async fn xread(
    con: &mut ConnectionManager,
    stream_name: &StreamName,
    stream_read_options: &StreamReadOptions,
) -> RedisResult<StreamReadReply> {
    trace!("xreadgroup {}", &stream_name.0,);

    con.xread_options(&[&stream_name.0], &[">"], stream_read_options)
        .await
}

impl ConsumerClient {
    pub async fn create_subscription(&mut self, config: &Group) -> RedisResult<()> {
        let mut con = self.client.get_multiplexed_async_connection().await?;
        let stream_name = &config.stream_name;
        let group_name = &config.group_name;
        xgroup_create_mkstream(&mut con, stream_name, group_name).await
    }

    pub async fn delete_subscription(&mut self, _subscription: Group) -> RedisResult<()> {
        // TODO
        Ok(())
    }

    pub async fn stream_subscription(&mut self, subscription: Group) -> RedisStream {
        let stream_name = subscription.stream_name;
        let group_name = subscription.group_name;
        let consumer_name = ConsumerName::new();

        let stream_name = stream_name.clone();

        let client = self.client.clone();

        // TODO: SW-19526 Implement reliability
        // The NOACK subcommand can be used to avoid adding the message to the PEL in cases where reliability is not
        // a requirement and the occasional message loss is acceptable. This is equivalent to acknowledging the
        // message when it is read.
        let stream_read_options = StreamReadOptions::default()
            .group(&group_name.0, &consumer_name.0)
            // Try to read batches.
            .count(10)
            // Block for up to 1 second (default behavior is to return immediately) for a single message. This does not
            // block until the batch is complete, just for a single message.
            .block(1_000)
            .noack();

        let (tx, rx) = tokio::sync::mpsc::channel(1);

        tokio::spawn(async move {
            loop {
                if tx.is_closed() {
                    break;
                }

                if let Ok(mut con) = ConnectionManager::new_with_config(
                    client.clone(),
                    super::connection_manager_config(),
                )
                .await
                {
                    info!("Redis connected");

                    loop {
                        // Read from the stream

                        let result: RedisResult<StreamReadReply> =
                            xread(&mut con, &stream_name, &stream_read_options).await;

                        match result {
                            Ok(entry) => {
                                for stream_key in entry.keys {
                                    for message in stream_key.ids {
                                        // TODO Do not ack immediately, use ack token instead
                                        let _: Result<(), _> = con
                                            .xack(&stream_name.0, &group_name.0, &[&message.id])
                                            .await;

                                        if let (
                                            Some(redis::Value::BulkString(b64_data)),
                                            Some(redis::Value::BulkString(schema)),
                                        ) = (
                                            message.map.get(PAYLOAD_KEY),
                                            message.map.get(SCHEMA_KEY),
                                        ) {
                                            let schema = String::from_utf8(schema.clone())
                                                .expect("Expecting utf8 encoded schema")
                                                .into();
                                            let topic = Topic::from(stream_name.as_topic());
                                            let b64_data = String::from_utf8(b64_data.clone())
                                                .expect("Expecting utf8 encoded payload");

                                            if let Err(err) = tx
                                                .send(EncodedMessage {
                                                    schema,
                                                    topic,
                                                    b64_data,
                                                })
                                                .await
                                            {
                                                warn!(err = ?err, "Internal error");
                                            }
                                        } else {
                                            // TODO Handle error instead of warn
                                            warn!(message = ?message, "Invalid message");
                                        }
                                    }
                                }
                            }
                            Err(err) => {
                                warn!(err = ?err, "Stream error");
                                if err.is_io_error() {
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        });

        RedisStream { receiver: rx }
    }
}
/// A message received from Redis
pub type RedisMessage<T> = crate::consumer::AcknowledgeableMessage<AcknowledgeToken, T>;

/// Errors encountered while streaming messages
#[derive(Debug, thiserror::Error)]
pub enum RedisStreamError {
    /// An error from the underlying stream
    #[error(transparent)]
    Stream(#[from] redis::RedisError),

    /// An error from a missing hedwig attribute
    #[error("missing expected attribute: {key}")]
    MissingAttribute {
        /// the missing attribute
        key: &'static str,
    },

    /// An error from a hedwig attribute with an invalid value
    #[error("invalid attribute value for {key}: {invalid_value}")]
    InvalidAttribute {
        /// the invalid attribute
        key: &'static str,
        /// the invalid value
        invalid_value: String,
        /// the error describing the invalidity
        #[source]
        source: BoxError,
    },

    #[error("malformed message")]
    MalformedMessage,
}

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// A stream of messages from a subscription
///
/// Created by [`ConsumerClient::stream_subscription`]
#[pin_project]
pub struct RedisStream {
    receiver: tokio::sync::mpsc::Receiver<EncodedMessage>,
}

impl stream::Stream for RedisStream {
    type Item = Result<RedisMessage<ValidatedMessage>, RedisStreamError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let this = self.as_mut().project();
        this.receiver.poll_recv(cx).map(|opt| {
            opt.map(|encoded_message| {
                let validated_message = redis_to_hedwig(encoded_message).unwrap();
                Ok(RedisMessage {
                    ack_token: AcknowledgeToken,
                    message: validated_message,
                })
            })
        })
    }
}

fn redis_to_hedwig(encoded_message: EncodedMessage) -> Result<ValidatedMessage, RedisStreamError> {
    use base64::Engine;

    let b64_data = &encoded_message.b64_data;
    let schema = encoded_message.schema;

    let data = base64::engine::general_purpose::STANDARD
        .decode(b64_data)
        .map_err(|_| RedisStreamError::MalformedMessage)?;

    let id = uuid::Uuid::new_v4();
    let timestamp = SystemTime::now();
    let headers = Headers::new();

    Ok(ValidatedMessage::new(id, timestamp, schema, headers, data))
}

#[derive(Debug)]
pub struct AcknowledgeToken;

#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
#[error("failed to ack/nack/modify")]
pub struct AcknowledgeError;

#[async_trait]
impl crate::consumer::AcknowledgeToken for AcknowledgeToken {
    type AckError = AcknowledgeError;
    type ModifyError = AcknowledgeError;
    type NackError = AcknowledgeError;

    async fn ack(self) -> Result<(), Self::AckError> {
        Ok(())
    }

    async fn nack(self) -> Result<(), Self::NackError> {
        Ok(())
    }

    async fn modify_deadline(&mut self, _seconds: u32) -> Result<(), Self::ModifyError> {
        Ok(())
    }
}

impl crate::consumer::Consumer for RedisStream {
    type AckToken = AcknowledgeToken;
    type Error = RedisStreamError;
    type Stream = RedisStream;

    fn stream(self) -> Self::Stream {
        self
    }
}

struct ConsumerName(String);

impl ConsumerName {
    fn new() -> Self {
        Self(uuid::Uuid::new_v4().to_string())
    }
}

#[derive(Debug, Clone)]
pub struct GroupName(String);

impl GroupName {
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }
}

#[derive(Debug, Clone)]
pub struct Group {
    group_name: GroupName,
    stream_name: StreamName,
}

impl Group {
    pub fn new(name: GroupName, stream_name: StreamName) -> Self {
        Self {
            group_name: name,
            stream_name,
        }
    }
}
