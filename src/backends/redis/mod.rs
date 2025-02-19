use std::borrow::Cow;

mod consumer;
mod publisher;

pub use consumer::*;
pub use publisher::*;

#[derive(Debug, thiserror::Error)]
pub enum RedisError {
    #[error("data store disconnected")]
    ClientError(#[from] redis::RedisError),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TopicName<'s>(Cow<'s, str>);

impl<'s> TopicName<'s> {
    pub fn new(name: impl Into<Cow<'s, str>>) -> Self {
        Self(name.into())
    }
}

pub struct ClientBuilderConfig {
    pub endpoint: String,
}

pub struct ClientBuilder {
    config: ClientBuilderConfig,
}

impl ClientBuilder {
    pub async fn new(config: ClientBuilderConfig) -> Result<Self, RedisError> {
        Ok(ClientBuilder { config })
    }
}

impl ClientBuilder {
    pub async fn build_consumer(
        &self,
        queue: impl Into<String>,
    ) -> Result<ConsumerClient, RedisError> {
        let client = redis::Client::open(self.config.endpoint.as_str())?;
        Ok(ConsumerClient::from_client(client, queue.into()))
    }

    pub async fn build_publisher(
        &self,
        // TODO SW-19526 Use publisher_id for full subscription name
        _publisher_id: impl Into<String>,
    ) -> Result<PublisherClient, RedisError> {
        let client = redis::Client::open(self.config.endpoint.as_str())?;
        Ok(PublisherClient::from_client(client))
    }
}
