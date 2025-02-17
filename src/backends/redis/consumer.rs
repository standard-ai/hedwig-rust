#![allow(unused_variables)]
#![allow(unused_imports)]

use crate::{Headers, ValidatedMessage};
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

// TODO
pub type AcknowledgeToken = ();

pub type PubSubMessage<T> = crate::consumer::AcknowledgeableMessage<AcknowledgeToken, T>;
