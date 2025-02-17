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

#[derive(Debug)]
pub enum PublishError {}

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

    // TODO list_topics (paginated, nontrivial)
    // TODO list_topic_subscriptions (same)
    // TODO list_topic_snapshots (same)
    // TODO update_topic
    // TODO get_topic
    // TODO detach_subscription
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
    type PublishError = PublishError;
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
    type Error = PublishError;

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
