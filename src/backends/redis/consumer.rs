#![allow(unused_variables)]
#![allow(unused_imports)]

use crate::{AcknowledgeableMessage, Headers, ValidatedMessage};
use async_trait::async_trait;
use futures_util::stream;
use pin_project::pin_project;
use std::{
    borrow::Cow,
    fmt::Display,
    ops::Bound,
    pin::Pin,
    str::FromStr,
    task::{Context, Poll},
    time::{Duration, SystemTime},
};
use tracing::debug;
use uuid::Uuid;

use super::RedisError;
use super::TopicName;

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
}

impl ConsumerClient {
    pub fn from_client(client: redis::Client) -> Self {
        ConsumerClient { client }
    }
}

impl ConsumerClient {
    pub async fn create_subscription(
        &mut self,
        config: SubscriptionConfig<'_>,
    ) -> Result<(), RedisError> {
        todo!();
    }

    pub async fn delete_subscription(
        &mut self,
        subscription: SubscriptionName<'_>,
    ) -> Result<(), RedisError> {
        todo!();
    }

    pub fn stream_subscription(&mut self, subscription: SubscriptionName<'_>) -> RedisStream {
        todo!()
    }
}

#[derive(Debug, Clone)]
pub struct SubscriptionConfig<'s> {
    pub name: SubscriptionName<'s>,
    pub topic: TopicName<'s>,
}

pub struct RedisStream {}

pub type PubSubMessage<T> = crate::consumer::AcknowledgeableMessage<AcknowledgeToken, T>;

impl crate::consumer::Consumer for RedisStream {
    type AckToken = AcknowledgeToken;
    type Error = RedisStreamError;
    type Stream = Self;

    fn stream(self) -> Self::Stream {
        self
    }
}

impl stream::Stream for RedisStream {
    type Item =
        Result<AcknowledgeableMessage<AcknowledgeToken, ValidatedMessage>, RedisStreamError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        todo!()
    }
}

/// Errors encountered while streaming messages
#[derive(Debug, thiserror::Error)]
pub enum RedisStreamError {
    /// An error from the underlying stream
    #[error(transparent)]
    Stream(#[from] redis::RedisError),
}

#[derive(Debug)]
pub struct AcknowledgeToken;

#[async_trait::async_trait]
impl crate::consumer::AcknowledgeToken for AcknowledgeToken {
    type AckError = RedisError; // TODO
    type NackError = RedisError; // TODO
    type ModifyError = RedisError; // TODO

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
