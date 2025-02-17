#![allow(unused_variables)]
#![allow(unused_imports)]

use crate::{EncodableMessage, Topic, ValidatedMessage};
use futures_util::{
    ready,
    sink::{Sink, SinkExt},
};
use pin_project::pin_project;
use std::{borrow::Cow, fmt::Display};
use std::{
    collections::{BTreeMap, VecDeque},
    fmt,
    pin::Pin,
    task::{Context, Poll},
    time::SystemTime,
};

use super::RedisError;
use super::TopicName;

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
        cause: redis::RedisError,

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
            PublishError::Publish { cause, .. } => Some(cause as &_),
            PublishError::Response(cause) => Some(cause as &_),
            PublishError::InvalidMessage { cause, .. } => Some(cause as &_),
        }
    }
}

// impl<M: EncodableMessage, E> From<TopicSinkError<M, E>> for PublishError<M, E> {
//     fn from(from: TopicSinkError<M, E>) -> Self {
//         match from {
//             TopicSinkError::Publish(cause, messages) => PublishError::Publish { cause, messages },
//             TopicSinkError::Response(err) => PublishError::Response(err),
//         }
//     }
// }

pub struct TopicConfig<'s> {
    pub name: TopicName<'s>,
}

impl PublisherClient {
    pub async fn create_topic(&mut self, topic: TopicConfig<'_>) -> Result<(), RedisError> {
        todo!()
    }

    pub async fn delete_topic(&mut self, topic: TopicName<'_>) -> Result<(), RedisError> {
        todo!()
    }

    pub fn publisher(&self) -> Publisher {
        Publisher {
            client: self.clone(),
        }
    }
}

/// A publisher for sending messages to PubSub topics
pub struct Publisher {
    client: PublisherClient,
}

impl<M, S> crate::publisher::Publisher<M, S> for Publisher
where
    M: EncodableMessage + Send + 'static,
    S: Sink<M> + Send + 'static,
{
    type PublishError = PublishError<M, S>;
    type PublishSink = PublishSink<M, S>;

    fn publish_sink_with_responses(
        self,
        validator: M::Validator,
        response_sink: S,
    ) -> Self::PublishSink {
        PublishSink {
            validator,
            client: self.client,
            _m: std::marker::PhantomData,
        }
    }
}

#[pin_project]
pub struct PublishSink<M: EncodableMessage, S: Sink<M>> {
    validator: M::Validator,
    client: PublisherClient,
    _m: std::marker::PhantomData<(M, S)>,
}

impl<M, S> Sink<M> for PublishSink<M, S>
where
    M: EncodableMessage + Send + 'static,
    S: Sink<M> + Send + 'static,
{
    type Error = PublishError<M, S>;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        todo!()
    }

    fn start_send(self: Pin<&mut Self>, item: M) -> Result<(), Self::Error> {
        todo!()
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        todo!()
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        todo!()
    }
}
