use async_trait::async_trait;
use futures_util::stream;
use pin_project::pin_project;
use redis::{
    streams::{StreamReadOptions, StreamReadReply},
    AsyncCommands, RedisResult,
};
use std::{
    borrow::Cow,
    pin::Pin,
    task::{Context, Poll},
    time::SystemTime,
};
use tracing::{debug, warn};

use crate::{redis::PAYLOAD_KEY, Headers, ValidatedMessage};

use super::StreamName;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SubscriptionName<'s>(Cow<'s, str>);

impl<'s> SubscriptionName<'s> {
    pub fn new(subscription: impl Into<Cow<'s, str>>) -> Self {
        Self(subscription.into())
    }
}

#[derive(Debug, Clone)]
pub struct ConsumerClient {
    client: redis::Client,
    queue: String,
}

impl ConsumerClient {
    pub fn from_client(client: redis::Client, queue: String) -> Self {
        ConsumerClient { client, queue }
    }
}

impl ConsumerClient {
    pub async fn create_subscription(
        &mut self,
        config: SubscriptionConfig<'_>,
    ) -> Result<(), redis::RedisError> {
        let mut con = self
            .client
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        let stream_name = &config.stream_name;
        let group_name = GroupName::from_queue(&self.queue);

        // The special ID $ is the ID of the last entry in the stream
        let id = "$";

        match con
            .xgroup_create_mkstream(&stream_name.0, &group_name.0, id)
            .await
        {
            Ok(()) => debug!(
                group_name = &group_name.0,
                stream_name = &stream_name.0,
                "redis consumer group created"
            ),
            Err(err) => warn!(
                err = err.to_string(),
                group_name = &group_name.0,
                stream_name = &stream_name.0,
                "cannot create consumer group"
            ),
        }
        Ok(())
    }

    pub async fn delete_subscription(
        &mut self,
        _subscription: SubscriptionName<'_>,
    ) -> Result<(), redis::RedisError> {
        // TODO
        Ok(())
    }

    pub async fn stream_subscription(&mut self, subscription: SubscriptionName<'_>) -> RedisStream {
        let topic = "user.created";
        let stream_name = format!("hedwig:{topic}");
        let group_name = subscription.0.to_string();
        let consumer_name = ConsumerName::new();

        let stream_name = stream_name.clone();

        // TODO: SW-19526 Implement reliability
        // The NOACK subcommand can be used to avoid adding the message to the PEL in cases where reliability is not
        // a requirement and the occasional message loss is acceptable. This is equivalent to acknowledging the
        // message when it is read.
        let stream_read_options = StreamReadOptions::default()
            .group(&group_name, &consumer_name.0)
            .noack();

        debug!("Created stream: {stream_name}");
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
                    .xread_options(&[&stream_name], &[">"], &stream_read_options)
                    .await;

                match result {
                    Ok(entry) => {
                        for stream_key in entry.keys {
                            for message in stream_key.ids {
                                // TODO Do not ack immediately, use ack token instead
                                let _: Result<(), _> =
                                    con.xack(&stream_name, &group_name, &[&message.id]).await;

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
#[derive(Debug, Clone)]
pub struct SubscriptionConfig<'s> {
    pub stream_name: StreamName,
    pub name: SubscriptionName<'s>,
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

pub struct AcknowledgeError;
pub struct ModifyAcknowledgeError;

#[async_trait]
impl crate::consumer::AcknowledgeToken for AcknowledgeToken {
    type AckError = AcknowledgeError;
    type ModifyError = ModifyAcknowledgeError;
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

struct GroupName(String);

impl GroupName {
    fn from_queue(queue: impl Into<String>) -> Self {
        Self(queue.into())
    }
}
