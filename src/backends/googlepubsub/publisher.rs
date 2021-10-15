use crate::{Topic, ValidatedMessage};
use futures_util::{
    ready,
    sink::{Sink, SinkExt},
};
use pin_project::pin_project;
use std::{
    collections::BTreeMap,
    pin::Pin,
    task::{Context, Poll},
    time::SystemTime,
};
use ya_gcp::pubsub;

use super::{
    retry_policy, BoxError, Connect, DefaultConnector, MakeConnection, PubSubError, SinkError,
    StatusCodeSet, TopicName, Uri,
};

/// A client through which PubSub publishing operations can be performed.
///
/// This includes managing topics and writing data to topics. Created using
/// [`build_publisher`](super::ClientBuilder::build_publisher)
#[derive(Debug, Clone)]
pub struct PublisherClient<C = DefaultConnector> {
    client: pubsub::PublisherClient<C>,
    project: String,
    identifier: String,
}

impl<C> PublisherClient<C> {
    pub(super) fn new(
        client: pubsub::PublisherClient<C>,
        project: String,
        identifier: String,
    ) -> Self {
        PublisherClient {
            client,
            project,
            identifier,
        }
    }

    fn project(&self) -> &str {
        &self.project
    }

    fn identifier(&self) -> &str {
        &self.identifier
    }
}

/// Errors which can occur while publishing a message
#[derive(Debug, thiserror::Error)]
pub enum PublishError {
    /// An error from publishing
    // TODO(pre-5.0) returned errors include the api::PubsubMessage; this could be translated back (with
    // some storage and lookup) to a M: EncodableMessage
    #[error(transparent)]
    Publish(#[from] SinkError),

    /// An error from encountering an invalid user message
    // TODO(pre-5.0) include the invalid message
    #[error("{reason}")]
    InvalidMessage {
        /// The reason the message was invalid
        reason: String,
    },
}

impl<C> PublisherClient<C>
where
    C: MakeConnection<Uri> + ya_gcp::Connect + Clone + Send + Sync + 'static,
    C::Connection: Unpin + Send + 'static,
    C::Future: Send + 'static,
    BoxError: From<C::Error>,
{
    /// Create a new PubSub topic.
    ///
    /// See the GCP documentation on topics [here](https://cloud.google.com/pubsub/docs/admin)
    pub async fn create_topic(&mut self, topic: TopicConfig<'_>) -> Result<(), PubSubError> {
        let topic = topic.into_topic(self);
        self.client.create_topic(topic).await?;

        Ok(())
    }

    /// Delete an existing PubSub topic.
    ///
    /// See the GCP documentation on topics [here](https://cloud.google.com/pubsub/docs/admin)
    pub async fn delete_topic(&mut self, topic: TopicName<'_>) -> Result<(), PubSubError> {
        let topic = topic.into_project_topic_name(self.project()).into();

        self.client
            .delete_topic(pubsub::api::DeleteTopicRequest { topic })
            .await?;

        Ok(())
    }

    /// Createa a new [`Publisher`] instance for publishing messages.
    ///
    /// Multiple publishers can be created using the same client, for example to use different
    /// validators. They may share some underlying resources for greater efficiency than creating
    /// multiple clients.
    // TODO(pre-5.0) accept a sink for messages after successful publishing
    pub fn publisher(&self) -> Publisher<C> {
        Publisher {
            client: self.clone(),
            retry_policy: retry_policy::ExponentialBackoff::new(
                pubsub::DEFAULT_RETRY_CODES,
                retry_policy::exponential_backoff::Config::default(),
            ),
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
pub struct Publisher<C, R = retry_policy::ExponentialBackoff<StatusCodeSet>> {
    client: PublisherClient<C>,
    retry_policy: R,
}

impl<C, OldR> Publisher<C, OldR> {
    /// Set the retry policy for this `Publisher`.
    ///
    /// If a publishing operation encounters an error, the given retry policy will be consulted to
    /// possibly retry the operation, or otherwise propagate the error to the caller.
    pub fn with_retry_policy<R>(self, retry_policy: R) -> Publisher<C, R>
    where
        // TODO(pre-5.0) this could instead be a retry policy on M: EncodableMessage (which the
        // caller actually knows about) instead of api::PubsubMessage. That requires some
        // storage/lookup though, so left for post-mvp
        R: retry_policy::RetryPolicy<pubsub::api::PublishRequest, PubSubError> + Clone,
    {
        Publisher {
            retry_policy,
            client: self.client,
        }
    }
}

impl<M, C, R> crate::publisher::Publisher<M> for Publisher<C, R>
where
    M: crate::publisher::EncodableMessage,
    C: Connect + Clone + Send + Sync + 'static,
    R: retry_policy::RetryPolicy<pubsub::api::PublishRequest, PubSubError> + Clone,
    pubsub::PublishTopicSink<C, R>: Sink<pubsub::api::PubsubMessage, Error = SinkError>,
{
    type PublishError = PublishError;
    type PublishSink = crate::publisher::ValidatorSink<M, M::Validator, PublishSink<C, R>>;

    fn publish_sink(self, validator: M::Validator) -> Self::PublishSink {
        crate::publisher::validator_sink(
            validator,
            PublishSink {
                topic_sinks: BTreeMap::new(),
                buffer: None,
                client: self.client,
                retry_policy: self.retry_policy,
            },
        )
    }
}

match_fields! {
    pubsub::api::Topic =>

    /// Configuration describing a PubSub topic.
    #[derive(Debug, Clone)]
    pub struct TopicConfig<'s> {
        pub name: TopicName<'s>,
        pub labels: std::collections::HashMap<String, String>,
        pub message_storage_policy: Option<pubsub::api::MessageStoragePolicy>,
        pub kms_key_name: String,
        pub message_retention_duration: Option<pubsub::api::Duration>,

        @except:
            schema_settings,
            satisfies_pzs,
    }
}

impl<'s> TopicConfig<'s> {
    fn into_topic<C>(self, client: &PublisherClient<C>) -> pubsub::api::Topic {
        pubsub::api::Topic {
            name: self.name.into_project_topic_name(client.project()).into(),
            labels: self.labels,
            message_storage_policy: self.message_storage_policy,
            kms_key_name: self.kms_key_name,
            message_retention_duration: self.message_retention_duration,

            schema_settings: None, // documented as experimental, and hedwig enforces schemas anyway
            satisfies_pzs: false,  // documented as reserved (currently unused)
        }
    }
}

impl<'s> Default for TopicConfig<'s> {
    fn default() -> Self {
        Self {
            name: TopicName::new(String::new()),
            labels: std::collections::HashMap::new(),
            message_storage_policy: None,
            kms_key_name: String::new(),
            message_retention_duration: None,
        }
    }
}

/// A sink for publishing messages.
///
/// Created by [`publish_sink`](Publisher#method.publish_sink)
#[pin_project]
pub struct PublishSink<C, R = retry_policy::ExponentialBackoff<StatusCodeSet>> {
    // The underlying sinks operate on a single topic. The incoming messages could have varying
    // topics, so this map holds a lazily initialized set of underlying sinks
    topic_sinks: BTreeMap<Topic, Pin<Box<pubsub::PublishTopicSink<C, R>>>>,

    // In order to know which sink to check in `poll_ready`, we need a message's topic; but we
    // won't know the topic until looking at the element in `start_send`, which contractually must
    // always be preceded by a `poll_ready`.
    //
    // Work around this chicken-egg problem by deferring readiness checking by 1 message.
    // The first `poll_ready` will always be Ready, and the first value will be seeded in this
    // buffer. Subsequent `poll_ready`s will check the *previous* message in the buffer, and try to
    // send it to its corresponding underlying sink
    buffer: Option<(Topic, ValidatedMessage)>,

    // Because the sinks will be generated lazily, we need a client and retry policy to create
    // sinks
    client: PublisherClient<C>,
    retry_policy: R,
}

impl<C, R> Sink<(Topic, ValidatedMessage)> for PublishSink<C, R>
where
    C: Connect + Clone + Send + Sync + 'static,
    R: retry_policy::RetryPolicy<pubsub::api::PublishRequest, PubSubError> + Clone,
    pubsub::PublishTopicSink<C, R>: Sink<pubsub::api::PubsubMessage, Error = SinkError>,
{
    type Error = PublishError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        let this = self.project();
        let topic_sinks = this.topic_sinks;
        let retry_policy = this.retry_policy;
        let client = this.client;

        // Given the buffered (topic, message), find the sink corresponding to the topic
        match this.buffer.as_ref() {
            Some((topic, _msg)) => {
                // look up the sink by topic. If a sink doesn't exist, initialize one
                let sink = topic_sinks.entry(*topic).or_insert_with(|| {
                    Box::pin(
                        client
                            .client
                            .publish_topic_sink(
                                TopicName::new(topic.as_ref())
                                    .into_project_topic_name(client.project()),
                            )
                            .with_retry_policy(retry_policy.clone()),
                    )
                });

                // poll the sink to see if it's ready
                ready!(sink.poll_ready_unpin(cx))?;

                // only take out of the buffer when we know the sink is ready
                let (_topic, msg) = this.buffer.take().expect("already check Some");

                // now send the message to the sink
                sink.start_send_unpin(hedwig_to_pubsub(msg, client.identifier())?)?;

                Poll::Ready(Ok(()))
            }

            // The buffer could be empty on the first ever poll_ready or after explicit flushes.
            // In that case the sink is immediately ready for an element
            None => Poll::Ready(Ok(())),
        }
    }

    fn start_send(
        self: Pin<&mut Self>,
        item: (Topic, ValidatedMessage),
    ) -> Result<(), Self::Error> {
        // try to put the item into the buffer.
        // If an item is already in the buffer, the user must not have called `poll_ready`
        if self.project().buffer.replace(item).is_some() {
            panic!("each `start_send` must be preceded by a successful call to `poll_ready`")
        }

        Ok(())
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        // first send any element in the buffer by checking readiness
        ready!(self.as_mut().poll_ready(cx))?;

        // then flush all of the underlying sinks
        let mut all_ready = true;
        for sink in self.project().topic_sinks.values_mut() {
            all_ready &= sink.poll_flush_unpin(cx)?.is_ready();
        }

        if all_ready {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        // first initiate a flush as required by the Sink contract
        ready!(self.as_mut().poll_flush(cx))?;

        // then close all of the underlying sinks
        let mut all_ready = true;
        for sink in self.project().topic_sinks.values_mut() {
            all_ready &= sink.poll_close_unpin(cx)?.is_ready();
        }

        if all_ready {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
}

/// convert a hedwig message into a pubsub message
fn hedwig_to_pubsub(
    mut msg: ValidatedMessage,
    publisher_id: &str,
) -> Result<pubsub::api::PubsubMessage, PublishError> {
    let mut attributes = std::mem::take(msg.headers_mut());

    if let Some(invalid_key) = attributes.keys().find(|key| key.starts_with("hedwig_")) {
        return Err(PublishError::InvalidMessage {
            reason: format!(
                "keys starting with \"hedwig_\" are reserved: {}",
                invalid_key
            ),
        });
    }

    attributes.insert(crate::HEDWIG_ID.into(), msg.uuid().to_string());
    attributes.insert(
        crate::HEDWIG_MESSAGE_TIMESTAMP.into(),
        msg.timestamp()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|_| PublishError::InvalidMessage {
                reason: format!(
                    "timestamp should be after UNIX epoch: {:?}",
                    msg.timestamp()
                ),
            })?
            .as_millis()
            .to_string(),
    );
    attributes.insert(crate::HEDWIG_SCHEMA.into(), msg.schema().into());
    attributes.insert(crate::HEDWIG_PUBLISHER.into(), publisher_id.into());
    attributes.insert(crate::HEDWIG_FORMAT_VERSION.into(), "1.0".into());

    Ok(pubsub::api::PubsubMessage {
        data: msg.into_data(),
        attributes,
        ..pubsub::api::PubsubMessage::default()
    })
}
