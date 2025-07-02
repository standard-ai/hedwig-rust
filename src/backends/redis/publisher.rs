use base64::Engine;
use core::fmt;
use futures_util::sink::Sink;
use pin_project::pin_project;
use redis::{
    aio::ConnectionManager,
    streams::{StreamTrimStrategy, StreamTrimmingMode},
    AsyncCommands, RedisResult,
};
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use tracing::warn;

use crate::{redis::EncodedMessage, EncodableMessage};

use super::{
    encode_timestamp, RedisError, FORMAT_VERSION_ATTR, ID_KEY, MESSAGE_TIMESTAMP_KEY, PAYLOAD_KEY,
    PUBLISHER_KEY, SCHEMA_KEY,
};
use super::{StreamName, ENCODING_ATTR};

/// Publisher client
#[derive(Debug, Clone)]
pub struct PublisherClient {
    client: redis::Client,
    publisher_id: PublisherId,
}

impl PublisherClient {
    /// Create a new publisher client from a Redis client
    pub fn from_client(client: redis::Client, publisher_id: impl Into<String>) -> Self {
        let publisher_id = PublisherId::new(publisher_id);
        PublisherClient {
            client,
            publisher_id,
        }
    }
}

/// Errors which can occur while publishing a message
#[derive(Debug)]
pub enum PublishError<M: EncodableMessage, E> {
    /// An error from publishing
    Publish {
        /// The cause of the error
        cause: RedisError,

        /// The batch of messages which failed to be published
        messages: Vec<M>,
    },

    /// An error from submitting a successfully published message to the user-provided response
    /// sink
    Response(E),

    /// An error from validating the given message
    InvalidMessage {
        /// The cause of the error
        cause: M::Error,

        /// The message which failed to be validated
        message: M,
    },
}

impl<M: EncodableMessage, E> fmt::Display for PublishError<M, E>
where
    M::Error: fmt::Display,
    E: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PublishError::Publish { messages, .. } => f.write_fmt(format_args!(
                "could not publish {} messages",
                messages.len()
            )),
            PublishError::Response(..) => f.write_str(
                "could not forward response for a successfully published message to the sink",
            ),
            PublishError::InvalidMessage { .. } => f.write_str("could not validate message"),
        }
    }
}

impl<M: EncodableMessage, E> std::error::Error for PublishError<M, E>
where
    M: fmt::Debug,
    M::Error: std::error::Error + 'static,
    E: std::error::Error + 'static,
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            PublishError::Publish { cause, .. } => Some(cause),
            PublishError::Response(cause) => Some(cause as &_),
            PublishError::InvalidMessage { cause, .. } => Some(cause as &_),
        }
    }
}

/// Topic configuration
pub struct TopicConfig {
    /// The topic name
    pub name: StreamName,
}

impl PublisherClient {
    /// Create a new publisher
    pub async fn publisher(&self) -> Publisher {
        let client = self.client.clone();
        let publisher_id = self.publisher_id.clone();

        let (tx, mut rx) = tokio::sync::mpsc::channel(1);

        tokio::spawn(async move {
            loop {
                if rx.is_closed() {
                    break;
                }

                let con_res = ConnectionManager::new_with_config(
                    client.clone(),
                    super::connection_manager_config(),
                )
                .await;

                if let Ok(mut con) = con_res {
                    if rx.is_closed() {
                        break;
                    }

                    while let Some(EncodedMessage {
                        id,
                        topic,
                        b64_data,
                        schema,
                        timestamp,
                    }) = rx.recv().await
                    {
                        let key = StreamName::from(topic);
                        let b64_data = b64_data.as_str();

                        let res = push(
                            &mut con,
                            &key,
                            b64_data,
                            &schema,
                            &id,
                            &publisher_id.0,
                            timestamp,
                        )
                        .await;

                        if let Err(err) = res {
                            warn!("{:?}", err);
                            if err.is_io_error() {
                                break;
                            }
                        }
                    }
                }
            }
        });

        Publisher { sender: tx }
    }
}

async fn push(
    con: &mut ConnectionManager,
    key: &StreamName,
    payload: &str,
    schema: &str,
    hedwig_id: &str,
    publisher_id: &str,
    timestamp: std::time::SystemTime,
) -> RedisResult<()> {
    // TODO trimming mode should not be needed if everything is set up correctly
    // Workaround to prevent increasing indefinitely the queue
    let options = redis::streams::StreamAddOptions::default().trim(StreamTrimStrategy::maxlen(
        StreamTrimmingMode::Approx,
        1_000,
    ));

    let timestamp = encode_timestamp(&timestamp);

    con.xadd_options(
        &key.0,
        "*",
        &[
            (PAYLOAD_KEY, payload),
            FORMAT_VERSION_ATTR,
            (ID_KEY, hedwig_id),
            (MESSAGE_TIMESTAMP_KEY, &timestamp),
            (PUBLISHER_KEY, publisher_id),
            (SCHEMA_KEY, schema),
            ENCODING_ATTR,
        ],
        &options,
    )
    .await
}

#[derive(Debug, Clone)]
struct PublisherId(String);

impl PublisherId {
    fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }
}

/// Redis publisher
#[derive(Clone)]
pub struct Publisher {
    sender: tokio::sync::mpsc::Sender<EncodedMessage>,
}

impl<M, S> crate::publisher::Publisher<M, S> for Publisher
where
    M: EncodableMessage + Send + 'static,
    S: Sink<M> + Send + 'static,
{
    type PublishError = PublishError<M, S::Error>;
    type PublishSink = PublishSink<M, S>;

    // TODO For reliability, implement response sink, so users can ack messages
    fn publish_sink_with_responses(
        self,
        validator: M::Validator,
        _response_sink: S,
    ) -> Self::PublishSink {
        PublishSink {
            validator,
            sender: self.sender.clone(),
            _m: std::marker::PhantomData,
            buffer: None,
        }
    }
}

/// Publish sink
#[pin_project]
pub struct PublishSink<M: EncodableMessage, S: Sink<M>> {
    validator: M::Validator,
    sender: tokio::sync::mpsc::Sender<EncodedMessage>,
    _m: std::marker::PhantomData<(M, S)>,
    buffer: Option<M>,
}

impl<M, S> Sink<M> for PublishSink<M, S>
where
    M: EncodableMessage + Send + 'static,
    S: Sink<M> + Send + 'static,
{
    type Error = PublishError<M, S::Error>;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.poll_flush_buffered_message(cx)
    }

    fn start_send(mut self: Pin<&mut Self>, message: M) -> Result<(), Self::Error> {
        let this = self.as_mut().project();

        if this.buffer.replace(message).is_some() {
            panic!("each `start_send` must be preceded by a successful call to `poll_ready`");
        }

        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.poll_flush_buffered_message(cx)
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

fn encode_message<M>(
    validator: &M::Validator,
    message: M,
) -> Result<EncodedMessage, PublishError<M, std::convert::Infallible>>
where
    M: EncodableMessage + Send + 'static,
{
    let validated = match message.encode(validator) {
        Ok(validated_msg) => validated_msg,
        Err(err) => {
            return Err(PublishError::InvalidMessage {
                cause: err,
                message,
            })
        }
    };

    let bytes = validated.data();
    let schema = validated.schema().to_string().into();

    // Encode as base64, because Redis needs it
    let b64_data = base64::engine::general_purpose::STANDARD.encode(bytes);
    let id = validated.uuid().to_string();
    let timestamp = *validated.timestamp();

    Ok(EncodedMessage {
        id,
        schema,
        topic: message.topic(),
        b64_data,
        timestamp,
    })
}

impl<M, S> PublishSink<M, S>
where
    M: EncodableMessage + Send + 'static,
    S: Sink<M> + Send + 'static,
{
    fn poll_flush_buffered_message(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), PublishError<M, S::Error>>> {
        let this = self.project();

        if this.sender.capacity() == 0 {
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }

        let Some(message) = this.buffer.take() else {
            // Nothing pending
            return Poll::Ready(Ok(()));
        };

        let Ok(encoded_message) = encode_message(this.validator, message) else {
            // TODO Handle errors
            return Poll::Ready(Ok(()));
        };

        // Cannot fail here, we checked capacity before
        this.sender.try_send(encoded_message).unwrap();
        Poll::Ready(Ok(()))
    }
}
