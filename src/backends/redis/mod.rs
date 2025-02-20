use std::borrow::Cow;

mod consumer;
mod publisher;

pub use consumer::*;
pub use publisher::*;

const PAYLOAD_KEY: &str = "hedwig_payload";
const ENCODING_ATTRIBUTE: (&str, &str) = ("hedwig_encoding", "base64");

#[derive(Debug, thiserror::Error)]
pub enum RedisError {
    #[error("data store disconnected")]
    ClientError(#[from] redis::RedisError),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StreamName<'s>(Cow<'s, str>);

impl<'s> StreamName<'s> {
    pub fn new(name: impl Into<Cow<'s, str>>) -> Self {
        Self(name.into())
    }
}

#[allow(clippy::needless_lifetimes)]
impl<'s> From<hedwig_core::Topic> for StreamName<'s> {
    fn from(topic: hedwig_core::Topic) -> Self {
        StreamName(format!("hedwig:{topic}").into())
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
