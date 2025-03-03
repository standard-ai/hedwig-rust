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
    #[error("deadline exceeded")]
    DeadlineExceeded,
    #[error(transparent)]
    GenericError(Box<dyn std::error::Error + Send + Sync>),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StreamName(String);

impl StreamName {
    pub fn from_topic(topic: impl std::fmt::Display) -> Self {
        StreamName(format!("hedwig:{topic}"))
    }
}

impl From<hedwig_core::Topic> for StreamName {
    fn from(topic: hedwig_core::Topic) -> Self {
        StreamName(format!("hedwig:{topic}"))
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
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
        _queue: impl Into<String>,
    ) -> Result<ConsumerClient, RedisError> {
        let client = redis::Client::open(self.config.endpoint.as_str())?;
        Ok(ConsumerClient::from_client(client))
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
