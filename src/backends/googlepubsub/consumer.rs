//! A [`Consumer`](crate::Consumer) implementation for Google's [PubSub][0] service
//!
//! [0]: https://cloud.google.com/pubsub/

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
use ya_gcp::pubsub;

use super::{
    retry_policy, AcknowledgeError, BoxError, Connect, DefaultConnector, MakeConnection,
    ModifyAcknowledgeError, PubSubError, StreamSubscriptionConfig, TopicName, Uri,
};

/// A PubSub subscription name.
///
/// This will be used to internally construct the expected
/// `projects/{project}/subscriptions/hedwig-{queue}-{subscription_name}` format for API calls
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SubscriptionName<'s>(Cow<'s, str>);

impl<'s> SubscriptionName<'s> {
    /// Create a new `SubscriptionName`
    pub fn new(name: impl Into<Cow<'s, str>>) -> Self {
        Self(name.into())
    }

    /// Construct a full project and subscription name with this name
    fn into_project_subscription_name(
        self,
        project_name: impl Display,
        queue_name: impl Display,
    ) -> pubsub::ProjectSubscriptionName {
        pubsub::ProjectSubscriptionName::new(
            project_name,
            std::format_args!(
                "hedwig-{queue}-{subscription}",
                queue = queue_name,
                subscription = self.0
            ),
        )
    }
}

/// A client through which PubSub consuming operations can be performed.
///
/// This includes managing subscriptions and reading data from subscriptions. Created using
/// [`build_consumer`](super::ClientBuilder::build_consumer)
#[derive(Debug, Clone)]
pub struct ConsumerClient<C = DefaultConnector> {
    client: pubsub::SubscriberClient<C>,
    project: String,
    queue: String,
}

impl<C> ConsumerClient<C> {
    pub(super) fn new(client: pubsub::SubscriberClient<C>, project: String, queue: String) -> Self {
        ConsumerClient {
            client,
            project,
            queue,
        }
    }

    fn project(&self) -> &str {
        &self.project
    }

    fn queue(&self) -> &str {
        &self.queue
    }
}

impl<C> ConsumerClient<C>
where
    C: MakeConnection<Uri> + Connect + Clone + Send + Sync + 'static,
    C::Connection: Unpin + Send + 'static,
    C::Future: Send + 'static,
    BoxError: From<C::Error>,
{
    /// Create a new PubSub subscription
    ///
    /// See the GCP documentation on subscriptions [here](https://cloud.google.com/pubsub/docs/subscriber)
    pub async fn create_subscription(
        &mut self,
        config: SubscriptionConfig<'_>,
    ) -> Result<(), PubSubError> {
        let subscription = SubscriptionConfig::into_subscription(config, &*self);

        self.client.create_subscription(subscription).await?;

        Ok(())
    }

    /// Delete an existing PubSub subscription.
    ///
    /// See the GCP documentation on subscriptions [here](https://cloud.google.com/pubsub/docs/subscriber)
    pub async fn delete_subscription(
        &mut self,
        subscription: SubscriptionName<'_>,
    ) -> Result<(), PubSubError> {
        let subscription = subscription
            .into_project_subscription_name(self.project(), self.queue())
            .into();

        self.client
            .delete_subscription(pubsub::api::DeleteSubscriptionRequest { subscription })
            .await?;

        Ok(())
    }

    /// Connect to PubSub and start streaming messages from the given subscription
    pub fn stream_subscription(
        &mut self,
        subscription: SubscriptionName<'_>,
        stream_config: StreamSubscriptionConfig,
    ) -> PubSubStream<C> {
        let subscription =
            subscription.into_project_subscription_name(self.project(), self.queue());

        PubSubStream(self.client.stream_subscription(subscription, stream_config))
    }

    /// Seeks messages from the given timestamp.
    /// It marks as acknowledged all the messages prior to the timestamp, and as
    /// not acknowledged the messages after the timestamp.
    pub async fn seek(
        &mut self,
        subscription: SubscriptionName<'_>,
        timestamp: pubsub::api::Timestamp,
    ) -> Result<(), PubSubError> {
        let request = pubsub::api::SeekRequest {
            subscription: subscription
                .into_project_subscription_name(self.project(), self.queue())
                .into(),
            target: Some(pubsub::api::seek_request::Target::Time(timestamp)),
        };
        self.client.seek(request).await?;
        Ok(())
    }

    // TODO list_subscriptions (paginated, nontrivial)
    // TODO update_subscriptions (field mask necessary?)
    // TODO get_subscription (impl From<api::Subscription> for SubscriptionConfig)
    // TODO snapshots?
}

match_fields! {
    pubsub::api::Subscription =>

    /// Configuration describing a PubSub subscription.
    // TODO incorporate standard_config
    #[derive(Debug, Clone)]
    pub struct SubscriptionConfig<'s> {
        pub name: SubscriptionName<'s>,
        pub topic: TopicName<'s>,
        pub ack_deadline_seconds: u16,
        pub retain_acked_messages: bool,
        pub message_retention_duration: Option<pubsub::api::Duration>,
        pub labels: std::collections::HashMap<String, String>,
        pub enable_message_ordering: bool,
        pub expiration_policy: Option<pubsub::api::ExpirationPolicy>,
        pub filter: String,
        pub dead_letter_policy: Option<pubsub::api::DeadLetterPolicy>,
        pub retry_policy: Option<pubsub::api::RetryPolicy>,

        @except:
            push_config,
            detached,
            topic_message_retention_duration,
    }
}

impl<'s> SubscriptionConfig<'s> {
    fn into_subscription<C>(self, client: &ConsumerClient<C>) -> pubsub::api::Subscription {
        pubsub::api::Subscription {
            name: self
                .name
                .into_project_subscription_name(client.project(), client.queue())
                .into(),
            topic: self.topic.into_project_topic_name(client.project()).into(),
            ack_deadline_seconds: self.ack_deadline_seconds.into(),
            retain_acked_messages: self.retain_acked_messages,
            message_retention_duration: self.message_retention_duration,
            labels: self.labels,
            enable_message_ordering: self.enable_message_ordering,
            expiration_policy: self.expiration_policy,
            filter: self.filter,
            dead_letter_policy: self.dead_letter_policy,
            retry_policy: self.retry_policy,
            push_config: None, // push delivery isn't used, it's streaming pull
            detached: false,   // set by the server on gets/listing
            topic_message_retention_duration: None, // Output only, set by the server
        }
    }
}

// TODO replace with a builder?
impl<'s> Default for SubscriptionConfig<'s> {
    fn default() -> Self {
        Self {
            name: SubscriptionName::new(String::new()),
            topic: TopicName::new(String::new()),
            ack_deadline_seconds: 0,
            retain_acked_messages: false,
            message_retention_duration: None,
            labels: std::collections::HashMap::default(),
            enable_message_ordering: false,
            expiration_policy: None,
            filter: "".into(),
            dead_letter_policy: None,
            retry_policy: None,
        }
    }
}

// TODO match_fields! on ExpirationPolicy, DeadLetterPolicy, RetryPolicy

/// A message received from PubSub.
///
/// This includes the message itself, and an [`AcknowledgeToken`](crate::AcknowledgeToken) used to
/// inform the message service when this message has been processed.
#[cfg_attr(docsrs, doc(cfg(feature = "google")))]
pub type PubSubMessage<T> = crate::consumer::AcknowledgeableMessage<pubsub::AcknowledgeToken, T>;

/// Errors encountered while streaming messages from PubSub
#[derive(Debug, thiserror::Error)]
#[cfg_attr(docsrs, doc(cfg(feature = "google")))]
pub enum PubSubStreamError {
    /// An error from the underlying stream
    #[error(transparent)]
    Stream(#[from] PubSubError),

    /// An error from a missing hedwig attribute
    #[error("missing expected attribute: {key}")]
    MissingAttribute {
        /// the missing attribute
        key: &'static str,
    },

    /// An error from a hedwig attribute with an invalid value
    #[error("invalid attribute value for {key}: {invalid_value}")]
    InvalidAttribute {
        /// the invalid attribute
        key: &'static str,
        /// the invalid value
        invalid_value: String,
        /// the error describing the invalidity
        #[source]
        source: BoxError,
    },
}

#[async_trait]
impl crate::consumer::AcknowledgeToken for pubsub::AcknowledgeToken {
    type AckError = AcknowledgeError;
    type ModifyError = ModifyAcknowledgeError;
    type NackError = AcknowledgeError;

    async fn ack(self) -> Result<(), Self::AckError> {
        self.ack().await
    }

    async fn nack(self) -> Result<(), Self::NackError> {
        self.nack().await
    }

    async fn modify_deadline(&mut self, seconds: u32) -> Result<(), Self::ModifyError> {
        self.modify_deadline(seconds).await
    }
}

/// A stream of messages from a subscription in PubSub.
///
/// Created by [`ConsumerClient::stream_subscription`]
#[pin_project]
#[cfg_attr(docsrs, doc(cfg(feature = "google")))]
pub struct PubSubStream<
    C = DefaultConnector,
    R = retry_policy::ExponentialBackoff<pubsub::PubSubRetryCheck>,
>(#[pin] pubsub::StreamSubscription<C, R>);

impl<C, OldR> PubSubStream<C, OldR> {
    /// Set the [`RetryPolicy`](retry_policy::RetryPolicy) to use for this streaming subscription.
    ///
    /// The stream will be reconnected if the policy indicates that an encountered error should be
    /// retried
    // Because `poll_next` requires `Pin<&mut Self>`, this function cannot be called after the
    // stream has started because it moves `self`. That means that the retry policy can only be
    // changed before the polling starts, and is fixed from that point on
    pub fn with_retry_policy<R>(self, retry_policy: R) -> PubSubStream<C, R>
    where
        R: retry_policy::RetryPolicy<(), PubSubError>,
    {
        PubSubStream(self.0.with_retry_policy(retry_policy))
    }
}

impl<C> stream::Stream for PubSubStream<C>
where
    C: MakeConnection<Uri> + Connect + Clone + Send + Sync + 'static,
    C::Connection: Unpin + Send + 'static,
    C::Future: Send + 'static,
    BoxError: From<C::Error>,
{
    type Item = Result<PubSubMessage<ValidatedMessage>, PubSubStreamError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        self.project().0.poll_next(cx).map(|opt| {
            opt.map(|res| {
                let (ack_token, message) = res?;
                Ok(PubSubMessage {
                    ack_token,
                    message: pubsub_to_hedwig(message)?,
                })
            })
        })
    }
}

impl<C> crate::consumer::Consumer for PubSubStream<C>
where
    C: MakeConnection<Uri> + Connect + Clone + Send + Sync + 'static,
    C::Connection: Unpin + Send + 'static,
    C::Future: Send + 'static,
    BoxError: From<C::Error>,
{
    type AckToken = pubsub::AcknowledgeToken;
    type Error = PubSubStreamError;
    type Stream = PubSubStream<C>;

    fn stream(self) -> Self::Stream {
        self
    }
}

/// The namespace of all the hedwig-internal attributes applied to messages
// the backtick '`' is one greater than the underscore '_' in ascii, which makes it the next
// greatest for Ord. Having this as the excluded upper bound makes the range contain every
// string prefixed by "hedwig_".
//
// This uses explicit Bounds instead of the Range syntax because impl RangeBounds<T> for Range<&T>
// requires T: Sized for some reason
const HEDWIG_NAME_RANGE: (Bound<&str>, Bound<&str>) =
    (Bound::Included("hedwig_"), Bound::Excluded("hedwig`"));

/// convert a pubsub message into a hedwig message
fn pubsub_to_hedwig(
    msg: pubsub::api::PubsubMessage,
) -> Result<ValidatedMessage, PubSubStreamError> {
    let mut headers = msg.attributes;

    // extract the hedwig attributes from the attribute map.
    // any remaining attributes were ones inserted by the user
    fn take_attr<F, T>(
        map: &mut Headers,
        key: &'static str,
        parse: F,
    ) -> Result<T, PubSubStreamError>
    where
        F: FnOnce(String) -> Result<T, (String, BoxError)>,
    {
        let value = map
            .remove(key)
            .ok_or(PubSubStreamError::MissingAttribute { key })?;

        parse(value).map_err(
            |(invalid_value, source)| PubSubStreamError::InvalidAttribute {
                key,
                invalid_value,
                source,
            },
        )
    }

    let id = take_attr(&mut headers, crate::HEDWIG_ID, |string| {
        Uuid::from_str(&string).map_err(|e| (string, BoxError::from(e)))
    })?;

    let timestamp = take_attr(&mut headers, crate::HEDWIG_MESSAGE_TIMESTAMP, |string| {
        // match instead of map_err to keep ownership of string
        let millis_since_epoch = match u64::from_str(&string) {
            Err(err) => return Err((string, BoxError::from(err))),
            Ok(t) => t,
        };
        SystemTime::UNIX_EPOCH
            .checked_add(Duration::from_millis(millis_since_epoch))
            .ok_or_else(|| {
                (
                    string,
                    BoxError::from(format!(
                        "time stamp {} is too large for SystemTime",
                        millis_since_epoch
                    )),
                )
            })
    })?;
    let schema = take_attr(&mut headers, crate::HEDWIG_SCHEMA, Ok::<String, _>)?;

    // these attributes we don't actually use, but we check for their existence as defensive
    // validation, and remove them so that the user doesn't see them among the headers
    take_attr(&mut headers, crate::HEDWIG_PUBLISHER, |_| Ok(()))?;
    take_attr(&mut headers, crate::HEDWIG_FORMAT_VERSION, |_| Ok(()))?;

    // for forwards compatibility with future hedwig formats, remove any other "hedwig_*"
    // attributes that might exist, so that the user doesn't witness them.
    headers
        .range::<str, _>(HEDWIG_NAME_RANGE)
        .map(|(k, _v)| k.clone()) // clone b/c there isn't a remove_range, and we can't borrow + remove
        .collect::<Vec<_>>()
        .into_iter()
        .for_each(|k| {
            debug!(message = "removing unknown hedwig attribute", key = &k[..]);
            headers.remove(&k);
        });

    Ok(ValidatedMessage::new(
        id, timestamp, schema, headers, msg.data,
    ))
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        HEDWIG_FORMAT_VERSION, HEDWIG_ID, HEDWIG_MESSAGE_TIMESTAMP, HEDWIG_PUBLISHER, HEDWIG_SCHEMA,
    };
    use pubsub::api::PubsubMessage;
    use std::collections::BTreeMap;

    #[derive(Debug, Clone)]
    struct EqValidatedMessage(ValidatedMessage);

    impl std::ops::Deref for EqValidatedMessage {
        type Target = ValidatedMessage;

        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }

    impl PartialEq<ValidatedMessage> for EqValidatedMessage {
        fn eq(&self, other: &ValidatedMessage) -> bool {
            self.uuid() == other.uuid()
                && self.timestamp() == other.timestamp()
                && self.schema() == other.schema()
                && self.headers() == other.headers()
                && self.data() == other.data()
        }
    }

    macro_rules! string_btree {
        ($($key:expr => $val:expr),* $(,)?) => {
            {
                #[allow(unused_mut)]
                let mut map = BTreeMap::new();
                $(
                    map.insert(($key).to_string(), ($val).to_string());
                )*
                map
            }
        }
    }

    /// Check that the data in headers is deserialized appropriately
    #[test]
    fn headers_parsed() {
        let user_attrs = string_btree! {
            "aaa" => "aaa_value",
            "zzz" => "zzz_value",
            "some_longer_string" => "the value for the longer string",
        };

        let hedwig_attrs = string_btree! {
            HEDWIG_ID => Uuid::nil(),
            HEDWIG_MESSAGE_TIMESTAMP => 1000,
            HEDWIG_SCHEMA => "my-test-schema",
            HEDWIG_PUBLISHER => "my-test-publisher",
            HEDWIG_FORMAT_VERSION => "1",
        };

        let data = "foobar";

        let mut attributes = user_attrs.clone();
        attributes.extend(hedwig_attrs);

        let message = PubsubMessage {
            data: data.into(),
            attributes,
            message_id: String::from("some_unique_id"),
            publish_time: Some(pubsub::api::Timestamp {
                seconds: 15,
                nanos: 42,
            }),
            ordering_key: String::new(),
        };

        let validated_message = pubsub_to_hedwig(message).unwrap();

        assert_eq!(
            EqValidatedMessage(ValidatedMessage::new(
                Uuid::nil(),
                SystemTime::UNIX_EPOCH + Duration::from_millis(1000),
                "my-test-schema",
                user_attrs,
                data
            )),
            validated_message
        );
    }

    /// Check that parsing headers fails if a hedwig attribute is missing
    #[test]
    fn headers_error_on_missing() {
        let full_hedwig_attrs = string_btree! {
            HEDWIG_ID => Uuid::nil(),
            HEDWIG_MESSAGE_TIMESTAMP => 1000,
            HEDWIG_SCHEMA => "my-test-schema",
            HEDWIG_PUBLISHER => "my-test-publisher",
            HEDWIG_FORMAT_VERSION => "1",
        };

        for &missing_header in [
            HEDWIG_ID,
            HEDWIG_MESSAGE_TIMESTAMP,
            HEDWIG_SCHEMA,
            HEDWIG_PUBLISHER,
            HEDWIG_FORMAT_VERSION,
        ]
        .iter()
        {
            let mut attributes = full_hedwig_attrs.clone();
            attributes.remove(missing_header);

            let res = pubsub_to_hedwig(PubsubMessage {
                attributes,
                ..PubsubMessage::default()
            });

            match res {
                Err(PubSubStreamError::MissingAttribute { key }) => assert_eq!(key, missing_header),
                _ => panic!(
                    "result did not fail on missing attribute {}: {:?}",
                    missing_header, res
                ),
            }
        }
    }

    /// Check that unknown hedwig headers are removed from the user-visible message, under the
    /// assumption that they are from some hedwig format change
    #[test]
    fn forward_compat_headers_removed() {
        let hedwig_attrs = string_btree! {
            HEDWIG_ID => Uuid::nil(),
            HEDWIG_MESSAGE_TIMESTAMP => 1000,
            HEDWIG_SCHEMA => "my-test-schema",
            HEDWIG_PUBLISHER => "my-test-publisher",
            HEDWIG_FORMAT_VERSION => "1",
            "hedwig_some_new_flag" => "boom!",
            "hedwig_another_change_from_the_future" => "kablam!",
        };

        let user_attrs = string_btree! {
            "abc" => "123",
            "foo" => "bar",
            "aaaaaaaaaaaaaaaaaaaaaaaaa" => "bbbbbbbbbbbbbbbbbbbb",
            // hedwig attributes are restricted to the "hedwig_" prefix by producers. It should
            // then be valid for a user to have the word "hedwig" prefixed for their own keys
            "hedwig-key-but-with-hyphens" => "assumes the restricted format always uses underscores",
            "hedwigAsAPrefixToSomeString" => "camelCase",
        };

        let mut attributes = user_attrs.clone();
        attributes.extend(hedwig_attrs);

        let validated_message = pubsub_to_hedwig(PubsubMessage {
            attributes,
            ..PubsubMessage::default()
        })
        .unwrap();

        assert_eq!(&user_attrs, validated_message.headers());
    }
}
