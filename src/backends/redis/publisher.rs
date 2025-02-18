#![allow(unused_variables)]
#![allow(unused_imports)]

use crate::{EncodableMessage, Topic, ValidatedMessage};
use futures_util::{
    ready,
    sink::{Sink, SinkExt},
};
use pin_project::pin_project;
use redis::AsyncCommands;
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
#[derive(Debug, thiserror::Error)]
pub enum PublishError<M: EncodableMessage> {
    Publish(M),
}

pub struct TopicConfig<'s> {
    pub name: TopicName<'s>,
}

impl PublisherClient {
    pub async fn create_topic(&mut self, topic: TopicConfig<'_>) -> Result<(), RedisError> {
        // TODO SW-19526 Implement create_topic
        Ok(())
    }

    pub async fn delete_topic(&mut self, topic: TopicName<'_>) -> Result<(), RedisError> {
        // TODO SW-19526 Implement delete_topic
        Ok(())
    }

    pub async fn publisher(&self) -> Publisher {
        let mut con = self
            .client
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        // TODO SW-19526 sketch, simple implementation with channels
        let (tx, mut rx) = tokio::sync::mpsc::channel(1000);

        tokio::spawn(async move {
            while let Some(message) = rx.recv().await {
                let key: &'static str = "hedwig:user-created";
                let items: [(&str, &str); 1] = [("hedwig_payload", "")];
                let _: Result<(), _> = con.xadd(key, "*", &items).await;
            }
        });

        Publisher { sender: tx }
    }
}

/// A publisher for sending messages to PubSub topics
pub struct Publisher {
    sender: tokio::sync::mpsc::Sender<Vec<u8>>,
}

impl<M, S> crate::publisher::Publisher<M, S> for Publisher
where
    M: EncodableMessage + Send + 'static,
    S: Sink<M> + Send + 'static,
{
    type PublishError = PublishError<M>;
    type PublishSink = PublishSink<M, S>;

    fn publish_sink_with_responses(
        self,
        validator: M::Validator,
        response_sink: S,
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
    sender: tokio::sync::mpsc::Sender<Vec<u8>>,
    _m: std::marker::PhantomData<(M, S)>,
}

impl<M, S> Sink<M> for PublishSink<M, S>
where
    M: EncodableMessage + Send + 'static,
    S: Sink<M> + Send + 'static,
{
    type Error = PublishError<M>;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: M) -> Result<(), Self::Error> {
        // TODO SW-19526 trivial mpsc implementation
        // TODO SW-19526 encode
        use tokio::sync::mpsc::error::TrySendError;
        let buf = Vec::<u8>::new();
        self.get_mut()
            .sender
            .try_send(buf)
            .map_err(|_| PublishError::Publish(item))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // TODO SW-19526 trivial mpsc implementation
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // TODO SW-19526 trivial mpsc implementation
        Poll::Ready(Ok(()))
    }
}
