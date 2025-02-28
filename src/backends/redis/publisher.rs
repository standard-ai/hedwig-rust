use base64::Engine;
use core::fmt;
use futures_util::sink::Sink;
use hedwig_core::Topic;
use pin_project::pin_project;
use redis::{aio::MultiplexedConnection, AsyncCommands, RedisResult};
use std::{
    pin::Pin,
    task::{Context, Poll},
};

use crate::EncodableMessage;

use super::{RedisError, PAYLOAD_KEY};
use super::{StreamName, ENCODING_ATTRIBUTE};

#[derive(Debug, Clone)]
pub struct PublisherClient {
    client: redis::Client,
}

impl PublisherClient {
    pub fn from_client(client: redis::Client) -> Self {
        PublisherClient { client }
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

pub struct TopicConfig {
    pub name: StreamName,
}

impl PublisherClient {
    pub async fn create_topic(&mut self, _topic: TopicConfig) -> Result<(), RedisError> {
        // TODO SW-19526 Implement create_topic
        Ok(())
    }

    pub async fn delete_topic(&mut self, _topic: StreamName) -> Result<(), RedisError> {
        // TODO SW-19526 Implement delete_topic
        Ok(())
    }

    pub async fn publisher(&self) -> Publisher {
        let mut con = self
            .client
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        // TODO SW-19526 It should be possible to refactor this without using mpsc
        let (tx, mut rx) = tokio::sync::mpsc::channel(1000);

        tokio::spawn(async move {
            while let Some(EncodedMessage { topic, data }) = rx.recv().await {
                let key = StreamName::from(topic);
                // Encode as base64, because Redis needs it
                let payload = base64::engine::general_purpose::STANDARD.encode(data);
                // TODO SW-19526 Add error handling
                let _: Result<(), _> = push(&mut con, &key, payload.as_str()).await;
            }
        });

        Publisher { sender: tx }
    }
}

async fn push(con: &mut MultiplexedConnection, key: &StreamName, payload: &str) -> RedisResult<()> {
    // Use * as the id for the current timestamp
    let id = "*";

    con.xadd(&key.0, id, &[(PAYLOAD_KEY, payload), ENCODING_ATTRIBUTE])
        .await
}

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

    // TODO SW-19526 For reliability, implement response sink, so users can ack messages
    fn publish_sink_with_responses(
        self,
        validator: M::Validator,
        _response_sink: S,
    ) -> Self::PublishSink {
        PublishSink {
            validator,
            sender: self.sender.clone(),
            _m: std::marker::PhantomData,
        }
    }
}

#[pin_project]
pub struct PublishSink<M: EncodableMessage, S: Sink<M>> {
    validator: M::Validator,
    sender: tokio::sync::mpsc::Sender<EncodedMessage>,
    _m: std::marker::PhantomData<(M, S)>,
}

impl<M, S> Sink<M> for PublishSink<M, S>
where
    M: EncodableMessage + Send + 'static,
    S: Sink<M> + Send + 'static,
{
    type Error = PublishError<M, S::Error>;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // TODO SW-19526 Improve implementation by following trait doc
        // Attempts to prepare the `Sink` to receive a value.
        //
        // This method must be called and return `Poll::Ready(Ok(()))` prior to
        // each call to `start_send`.
        //
        // This method returns `Poll::Ready` once the underlying sink is ready to
        // receive data. If this method returns `Poll::Pending`, the current task
        // is registered to be notified (via `cx.waker().wake_by_ref()`) when `poll_ready`
        // should be called again.
        //
        // In most cases, if the sink encounters an error, the sink will
        // permanently be unable to receive items.
        Poll::Ready(Ok(()))
    }

    fn start_send(mut self: Pin<&mut Self>, message: M) -> Result<(), Self::Error> {
        // TODO SW-19526 Improve implementation by following trait doc
        // Begin the process of sending a value to the sink.
        // Each call to this function must be preceded by a successful call to
        // `poll_ready` which returned `Poll::Ready(Ok(()))`.
        //
        // As the name suggests, this method only *begins* the process of sending
        // the item. If the sink employs buffering, the item isn't fully processed
        // until the buffer is fully flushed. Since sinks are designed to work with
        // asynchronous I/O, the process of actually writing out the data to an
        // underlying object takes place asynchronously. **You *must* use
        // `poll_flush` or `poll_close` in order to guarantee completion of a
        // send**.
        //
        // Implementations of `poll_ready` and `start_send` will usually involve
        // flushing behind the scenes in order to make room for new messages.
        // It is only necessary to call `poll_flush` if you need to guarantee that
        // *all* of the items placed into the `Sink` have been sent.
        //
        // In most cases, if the sink encounters an error, the sink will
        // permanently be unable to receive items.

        let this = self.as_mut().project();

        let validated = match message.encode(this.validator) {
            Ok(validated_msg) => validated_msg,
            Err(err) => {
                return Err(PublishError::InvalidMessage {
                    cause: err,
                    message,
                })
            }
        };

        // TODO SW-19526 Better create an intermediate sink for encoding, see googlepubsub
        let bytes = validated.into_data();
        let encoded_message = EncodedMessage {
            topic: message.topic(),
            data: bytes,
        };

        self.get_mut()
            .sender
            .try_send(encoded_message)
            .map_err(|cause| PublishError::Publish {
                cause: RedisError::GenericError(Box::new(cause)),
                messages: vec![message],
            })
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // TODO SW-19526 Improve implementation by following trait doc
        // Flush any remaining output from this sink.
        //
        // Returns `Poll::Ready(Ok(()))` when no buffered items remain. If this
        // value is returned then it is guaranteed that all previous values sent
        // via `start_send` have been flushed.
        //
        // Returns `Poll::Pending` if there is more work left to do, in which
        // case the current task is scheduled (via `cx.waker().wake_by_ref()`) to wake up when
        // `poll_flush` should be called again.
        //
        // In most cases, if the sink encounters an error, the sink will
        // permanently be unable to receive items.
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // TODO SW-19526 Improve implementation by following trait doc
        // Flush any remaining output and close this sink, if necessary.
        //
        // Returns `Poll::Ready(Ok(()))` when no buffered items remain and the sink
        // has been successfully closed.
        //
        // Returns `Poll::Pending` if there is more work left to do, in which
        // case the current task is scheduled (via `cx.waker().wake_by_ref()`) to wake up when
        // `poll_close` should be called again.
        //
        // If this function encounters an error, the sink should be considered to
        // have failed permanently, and no more `Sink` methods should be called.
        Poll::Ready(Ok(()))
    }
}

struct EncodedMessage {
    topic: Topic,
    data: bytes::Bytes,
}
