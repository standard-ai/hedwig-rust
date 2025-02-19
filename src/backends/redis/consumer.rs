use crate::{Headers, ValidatedMessage};
use async_trait::async_trait;
use futures_util::{stream, FutureExt, TryFutureExt};
use pin_project::pin_project;
use redis::{
    streams::{StreamReadOptions, StreamReadReply},
    AsyncCommands, RedisResult,
};
use std::error::Error as _;
use std::{
    borrow::Cow,
    fmt::Display,
    ops::Bound,
    pin::Pin,
    str::FromStr,
    task::{Context, Poll},
    time::{Duration, SystemTime},
};
use tracing::{debug, info, warn};
use uuid::Uuid;

use super::TopicName;

/// A PubSub subscription name.
///
/// This will be used to internally construct the expected
/// `projects/{project}/subscriptions/hedwig-{queue}-{subscription_name}` format for API calls
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SubscriptionName<'s>(Cow<'s, str>);

impl<'s> SubscriptionName<'s> {
    /// Create a new `SubscriptionName`
    pub fn new(subscription: impl Into<Cow<'s, str>>) -> Self {
        Self(subscription.into())
    }

    // /// Create a new `SubscriptionName` for a cross-project subscription, i.e. a subscription that subscribes to a topic
    // /// from another project.
    // pub fn with_cross_project(
    //     project: impl Into<Cow<'s, str>>,
    //     subscription: impl Into<Cow<'s, str>>,
    // ) -> Self {
    //     // the cross-project is effectively part of a compound subscription name
    //     Self(format!("{}-{}", project.into(), subscription.into()).into())
    // }
    //
    // /// Construct a full project and subscription name with this name
    // fn into_project_subscription_name(
    //     self,
    //     project_name: impl Display,
    //     queue_name: impl Display,
    // ) -> pubsub::ProjectSubscriptionName {
    //     pubsub::ProjectSubscriptionName::new(
    //         project_name,
    //         std::format_args!(
    //             "hedwig-{queue}-{subscription}",
    //             queue = queue_name,
    //             subscription = self.0
    //         ),
    //     )
    // }
}

/// A client through which PubSub consuming operations can be performed.
///
/// This includes managing subscriptions and reading data from subscriptions. Created using
/// [`build_consumer`](super::ClientBuilder::build_consumer)
#[derive(Debug, Clone)]
pub struct ConsumerClient {
    client: redis::Client,
    queue: String,
}

impl ConsumerClient {
    /// Create a new consumer from an existing pubsub client.
    ///
    /// This function is useful for client customization; most callers should typically use the
    /// defaults provided by [`build_consumer`](super::ClientBuilder::build_consumer)
    pub fn from_client(client: redis::Client, queue: String) -> Self {
        ConsumerClient { client, queue }
    }

    fn queue(&self) -> &str {
        &self.queue
    }

    /// Construct a fully formatted project and subscription name for the given subscription
    // pub fn format_subscription(
    //     &self,
    //     subscription: SubscriptionName<'_>,
    // ) -> pubsub::ProjectSubscriptionName {
    //     subscription.into_project_subscription_name(self.project(), self.queue())
    // }
    //
    // /// Construct a fully formatted project and topic name for the given topic
    // pub fn format_topic(&self, topic: TopicName<'_>) -> pubsub::ProjectTopicName {
    //     topic.into_project_topic_name(self.project())
    // }

    /// Get a reference to the underlying pubsub client
    pub fn inner(&self) -> &redis::Client {
        &self.client
    }

    /// Get a mutable reference to the underlying pubsub client
    pub fn inner_mut(&mut self) -> &mut redis::Client {
        &mut self.client
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
        let topic = config.topic.0.to_string();
        let stream_name = format!("hedwig:{topic}");
        let group_name = "test";
        let id = "0";

        debug!(
            topic = topic,
            stream_name = stream_name,
            "create_subscription"
        );
        debug!(
            "xgroup_create_mkstream({:?}, {:?}, {:?}",
            stream_name, group_name, id
        );

        match con
            .xgroup_create_mkstream(stream_name.clone(), group_name, id)
            .await
        {
            Ok(()) => debug!(
                group_name = group_name,
                stream_name = stream_name,
                "redis consumer group created"
            ),
            Err(err) => warn!(
                err = err.to_string(),
                group_name = group_name,
                stream_name = stream_name,
                "cannot create consumer group"
            ),
        }
        Ok(())
    }

    /// Delete an existing PubSub subscription.
    ///
    /// See the GCP documentation on subscriptions [here](https://cloud.google.com/pubsub/docs/subscriber)
    pub async fn delete_subscription(
        &mut self,
        subscription: SubscriptionName<'_>,
    ) -> Result<(), redis::RedisError> {
        // TODO
        Ok(())
    }

    /// Connect to PubSub and start streaming messages from the given subscription
    pub async fn stream_subscription(&mut self, subscription: SubscriptionName<'_>) -> RedisStream {
        let con = self
            .client
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        let topic = "user.created";
        let stream_name = format!("hedwig:{topic}");
        let group_name = "test";
        let consumer_name = "asd-asd-asd";

        let stream_read_options = StreamReadOptions::default().group(group_name, consumer_name);

        debug!("Created stream: {stream_name}");

        let receiver = {
            let mut con = self
                .client
                .get_multiplexed_async_connection()
                .await
                .unwrap();

            // TODO SW-19526 sketch, simple implementation with channels
            let (tx, rx) = tokio::sync::mpsc::channel(1000);

            let stream_name = stream_name.clone();
            let stream_read_options = StreamReadOptions::default().group(group_name, consumer_name);
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
                                        con.xack(&stream_name, group_name, &[&message.id]).await;

                                    if let Some(redis::Value::BulkString(vec)) =
                                        message.map.get("hedwig_payload")
                                    {
                                        tx.send(vec.clone()).await.unwrap()
                                    } else {
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

            rx
        };

        RedisStream {
            con,
            stream_name,
            stream_read_options,
            receiver,
        }
    }
}
#[derive(Debug, Clone)]
pub struct SubscriptionConfig<'s> {
    pub topic: TopicName<'s>,
    pub name: SubscriptionName<'s>,
}

/// A message received from Redis
pub type RedisMessage<T> = crate::consumer::AcknowledgeableMessage<AcknowledgeToken, T>;

/// Errors encountered while streaming messages from PubSub
#[derive(Debug, thiserror::Error)]
#[cfg_attr(docsrs, doc(cfg(feature = "google")))]
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
    con: redis::aio::MultiplexedConnection,
    stream_name: String,
    stream_read_options: StreamReadOptions,
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
