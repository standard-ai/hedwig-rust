mod consumer;
mod publisher;

use std::time::Duration;

pub use consumer::*;
pub use publisher::*;
use redis::aio::ConnectionManagerConfig;

use hedwig_core::Topic;

const ID_KEY: &str = "hedwig_id";
const PAYLOAD_KEY: &str = "hedwig_payload";
const SCHEMA_KEY: &str = "hedwig_schema";
const MESSAGE_TIMESTAMP_KEY: &str = "hedwig_message_timestamp";
const PUBLISHER_KEY: &str = "hedwig_publisher";

const ENCODING_ATTR: (&str, &str) = ("hedwig_encoding", "base64");
const FORMAT_VERSION_ATTR: (&str, &str) = ("hedwig_format_version", "1.0");

const BACKOFF_MAX_DELAY: Duration = Duration::from_secs(300);

fn connection_manager_config() -> ConnectionManagerConfig {
    ConnectionManagerConfig::new()
        // Note: despite ConnectionManagerConfig documentations says that a factor of 1000 means 1 sec, it is wrong.
        // This is how the delay is muliplied at each retry, starting from 1s.
        .set_factor(2)
        // This is really millis, not secs
        .set_max_delay(BACKOFF_MAX_DELAY.as_millis() as u64)
}

/// Error type for Redis backend
#[derive(Debug, thiserror::Error)]
pub enum RedisError {
    /// Unexpected disconnection
    #[error("data store disconnected")]
    ClientError(#[from] redis::RedisError),
    /// Deadline exceeded
    #[error("deadline exceeded")]
    DeadlineExceeded,
    /// Generic error
    #[error(transparent)]
    GenericError(Box<dyn std::error::Error + Send + Sync>),
}

/// Stream name
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StreamName(String);

impl StreamName {
    /// Create a new stream name from a topic
    pub fn from_topic(topic: impl std::fmt::Display) -> Self {
        StreamName(format!("hedwig:{topic}"))
    }

    /// Get the topic from the stream name
    pub fn as_topic(&self) -> &str {
        &self.0.as_str()[7..]
    }
}

impl From<hedwig_core::Topic> for StreamName {
    fn from(topic: hedwig_core::Topic) -> Self {
        StreamName(format!("hedwig:{topic}"))
    }
}

/// Configuration for the Redis client
#[derive(Debug, Clone)]
pub struct ClientBuilderConfig {
    /// Redis endpoint
    pub endpoint: String,
}

/// Builder for the Redis client
pub struct ClientBuilder {
    config: ClientBuilderConfig,
}

/// Redis client builder
impl ClientBuilder {
    /// Create a new client builder
    pub async fn new(config: ClientBuilderConfig) -> Result<Self, RedisError> {
        Ok(ClientBuilder { config })
    }
}

impl ClientBuilder {
    /// Build a new consumer client
    pub async fn build_consumer(
        &self,
        // TODO
        _queue: impl Into<String>,
    ) -> Result<ConsumerClient, RedisError> {
        let client = redis::Client::open(self.config.endpoint.as_str())?;
        Ok(ConsumerClient::from_client(client))
    }

    /// Build a new publisher client
    pub async fn build_publisher(
        &self,
        // TODO
        _publisher_id: impl Into<String>,
    ) -> Result<PublisherClient, RedisError> {
        let client = redis::Client::open(self.config.endpoint.as_str())?;
        Ok(PublisherClient::from_client(client))
    }
}

struct EncodedMessage {
    id: String,
    topic: Topic,
    schema: std::borrow::Cow<'static, str>,
    b64_data: String,
}
