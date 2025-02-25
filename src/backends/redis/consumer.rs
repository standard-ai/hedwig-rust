use async_trait::async_trait;
use futures_util::stream;
use pin_project::pin_project;
use redis::{
    aio::MultiplexedConnection,
    streams::{StreamReadOptions, StreamReadReply},
    AsyncCommands, RedisResult,
};
use std::{
    pin::Pin,
    task::{Context, Poll},
    time::SystemTime,
};
use tracing::warn;

use crate::{redis::PAYLOAD_KEY, Headers, ValidatedMessage};

use super::StreamName;

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

    con.xgroup_create_mkstream(&stream_name.0, &group_name.0, id)
        .await
}

impl ConsumerClient {
    pub async fn create_subscription(&mut self, config: &Group) -> RedisResult<()> {
        let mut con = self
            .client
            .get_multiplexed_async_connection()
            .await
            .unwrap();
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

        // TODO: SW-19526 Implement reliability
        // The NOACK subcommand can be used to avoid adding the message to the PEL in cases where reliability is not
        // a requirement and the occasional message loss is acceptable. This is equivalent to acknowledging the
        // message when it is read.
        let stream_read_options = StreamReadOptions::default()
            .group(&group_name.0, &consumer_name.0)
            .noack();

        let mut con = self
            .client
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        let (tx, rx) = tokio::sync::mpsc::channel(1000);

        tokio::spawn(async move {
            loop {
                // Read from the stream
                let result: RedisResult<StreamReadReply> = con
                    .xread_options(&[&stream_name.0], &[">"], &stream_read_options)
                    .await;

                match result {
                    Ok(entry) => {
                        for stream_key in entry.keys {
                            for message in stream_key.ids {
                                // TODO Do not ack immediately, use ack token instead
                                let _: Result<(), _> = con
                                    .xack(&stream_name.0, &group_name.0, &[&message.id])
                                    .await;

                                if let Some(redis::Value::BulkString(vec)) =
                                    message.map.get(PAYLOAD_KEY)
                                {
                                    tx.send(vec.clone()).await.unwrap()
                                } else {
                                    // TODO Handle error instead of warn
                                    warn!(message = ?message, "Unexpected message");
                                }
                            }
                        }
                    }
                    Err(err) => {
                        warn!(err = ?err, "Stream error");
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
    receiver: tokio::sync::mpsc::Receiver<Vec<u8>>,
}

impl stream::Stream for RedisStream {
    type Item = Result<RedisMessage<ValidatedMessage>, RedisStreamError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let this = self.as_mut().project();
        this.receiver.poll_recv(cx).map(|opt| {
            opt.map(|encoded_message| {
                let validated_message = redis_to_hedwig(&encoded_message).unwrap();
                Ok(RedisMessage {
                    ack_token: AcknowledgeToken,
                    message: validated_message,
                })
            })
        })
    }
}

fn redis_to_hedwig(payload: &[u8]) -> Result<ValidatedMessage, RedisStreamError> {
    use base64::Engine;
    let data = base64::engine::general_purpose::STANDARD
        .decode(payload)
        .map_err(|_| RedisStreamError::MalformedMessage)?;

    let id = uuid::Uuid::new_v4();
    let timestamp = SystemTime::now();
    let schema = "user.created/1.0"; // TODO
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
