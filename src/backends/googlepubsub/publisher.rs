#![allow(clippy::result_large_err)]

use crate::{EncodableMessage, Topic, ValidatedMessage};
use futures_util::{
    ready,
    sink::{Sink, SinkExt},
};
use pin_project::pin_project;
use std::{
    collections::{BTreeMap, VecDeque},
    fmt,
    pin::Pin,
    task::{Context, Poll},
    time::SystemTime,
};
use ya_gcp::{
    grpc::{Body, BoxBody, Bytes, DefaultGrpcImpl, GrpcService, StdError},
    pubsub,
};

use super::{
    retry_policy::{
        exponential_backoff::Config as ExponentialBackoffConfig, ExponentialBackoff,
        RetryOperation, RetryPolicy,
    },
    PubSubError, TopicName,
};

use message_translate::{TopicSink, TopicSinkError};

/// A thread-safe analog to Rc<RefCell<T>>
///
/// There are a few components in the publishing sink which are shared between layers and
/// exclusively borrowed, but not in a way the compiler can recognize. These can't use references
/// because the layers need ownership (some are passed to other libs like into gcp). In principle
/// they could use raw pointers, aided by Pin preventing moves; but the unsafety is unnerving, so
/// checked sharing is used instead.
///
/// Note the element is never actually borrowed across threads, or even across `await` points; all
/// calls happen in a single call stack of `poll_*` functions. Send + Sync are required to ensure
/// the containing top-level sink can be held across awaits (or actually sent) without an unsafe
/// Send+Sync declaration
#[derive(Debug)]
struct Shared<T>(std::sync::Arc<parking_lot::Mutex<T>>);

impl<T> Shared<T> {
    fn new(t: T) -> Self {
        Self(std::sync::Arc::new(parking_lot::Mutex::new(t)))
    }

    fn borrow_mut(&self) -> impl std::ops::DerefMut<Target = T> + '_ {
        self.0
            .try_lock()
            .unwrap_or_else(|| panic!("unexpected overlapping borrow of shared state"))
    }
}

impl<T> Clone for Shared<T> {
    fn clone(&self) -> Self {
        Self(std::sync::Arc::clone(&self.0))
    }
}

/// A client through which PubSub publishing operations can be performed.
///
/// This includes managing topics and writing data to topics. Created using
/// [`build_publisher`](super::ClientBuilder::build_publisher)
#[derive(Debug, Clone)]
pub struct PublisherClient<C = DefaultGrpcImpl> {
    client: pubsub::PublisherClient<C>,
    project: String,
    identifier: String,
}

impl<C> PublisherClient<C> {
    /// Create a new publisher from an existing pubsub client.
    ///
    /// This function is useful for client customization; most callers should typically use the
    /// defaults provided by [`build_publisher`](super::ClientBuilder::build_publisher)
    pub fn from_client(
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

    /// Construct a fully formatted project and topic name for the given topic
    pub fn format_topic(&self, topic: TopicName<'_>) -> pubsub::ProjectTopicName {
        topic.into_project_topic_name(self.project())
    }

    /// Get a reference to the underlying pubsub client
    pub fn inner(&self) -> &pubsub::PublisherClient<C> {
        &self.client
    }

    /// Get a mutable reference to the underlying pubsub client
    pub fn inner_mut(&mut self) -> &mut pubsub::PublisherClient<C> {
        &mut self.client
    }
}

/// Errors which can occur while publishing a message
#[derive(Debug)]
pub enum PublishError<M: EncodableMessage, E> {
    /// An error from publishing
    Publish {
        /// The cause of the error
        cause: PubSubError,

        /// The batch of messages which failed to be published
        messages: Vec<M>,
    },

    /// An error from submitting a successfully published message to the user-provided response
    /// sink
    Response(E),

    /// An error from validating the given message
    InvalidMessage {
        /// The cause of the error
        cause: M::Error,

        /// The message which failed to be validated
        message: M,
    },
}

impl<M: EncodableMessage, E> fmt::Display for PublishError<M, E>
where
    M::Error: fmt::Display,
    E: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            PublishError::Publish { messages, .. } => f.write_fmt(format_args!(
                "could not publish {} messages",
                messages.len()
            )),
            PublishError::Response(..) => f.write_str(
                "could not forward response for a successfully published message to the sink",
            ),
            PublishError::InvalidMessage { .. } => f.write_str("could not validate message"),
        }
    }
}

impl<M: EncodableMessage, E> std::error::Error for PublishError<M, E>
where
    M: fmt::Debug,
    M::Error: std::error::Error + 'static,
    E: std::error::Error + 'static,
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            PublishError::Publish { cause, .. } => Some(cause as &_),
            PublishError::Response(cause) => Some(cause as &_),
            PublishError::InvalidMessage { cause, .. } => Some(cause as &_),
        }
    }
}

impl<M: EncodableMessage, E> From<TopicSinkError<M, E>> for PublishError<M, E> {
    fn from(from: TopicSinkError<M, E>) -> Self {
        match from {
            TopicSinkError::Publish(cause, messages) => PublishError::Publish { cause, messages },
            TopicSinkError::Response(err) => PublishError::Response(err),
        }
    }
}

impl<C> PublisherClient<C>
where
    C: GrpcService<BoxBody>,
    C::Error: Into<StdError>,
    C::ResponseBody: Body<Data = Bytes> + Send + 'static,
    <C::ResponseBody as Body>::Error: Into<StdError> + Send,
{
    /// Create a new PubSub topic.
    ///
    /// See the GCP documentation on topics [here](https://cloud.google.com/pubsub/docs/admin)
    pub async fn create_topic(&mut self, topic: TopicConfig<'_>) -> Result<(), PubSubError> {
        let topic = topic.into_topic(self);
        self.client.raw_api_mut().create_topic(topic).await?;

        Ok(())
    }

    /// Delete an existing PubSub topic.
    ///
    /// See the GCP documentation on topics [here](https://cloud.google.com/pubsub/docs/admin)
    pub async fn delete_topic(&mut self, topic: TopicName<'_>) -> Result<(), PubSubError> {
        let topic = topic.into_project_topic_name(self.project()).into();

        self.client
            .raw_api_mut()
            .delete_topic({
                let mut r = pubsub::api::DeleteTopicRequest::default();
                r.topic = topic;
                r
            })
            .await?;

        Ok(())
    }

    /// Create a a new [`Publisher`] instance for publishing messages.
    ///
    /// Multiple publishers can be created using the same client, for example to use different
    /// validators. They may share some underlying resources for greater efficiency than creating
    /// multiple clients.
    pub fn publisher(&self) -> Publisher<C>
    where
        C: Clone,
    {
        Publisher {
            client: self.clone(),
            retry_policy: ExponentialBackoff::new(
                pubsub::PubSubRetryCheck::default(),
                ExponentialBackoffConfig::default(),
            ),
            publish_config: pubsub::PublishConfig::default(),
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
pub struct Publisher<C = DefaultGrpcImpl, R = ExponentialBackoff<pubsub::PubSubRetryCheck>> {
    client: PublisherClient<C>,
    retry_policy: R,
    publish_config: pubsub::PublishConfig,
}

impl<C, OldR> Publisher<C, OldR> {
    /// Set the retry policy for this `Publisher`.
    ///
    /// If a publishing operation encounters an error, the given retry policy will be consulted to
    /// possibly retry the operation, or otherwise propagate the error to the caller.
    pub fn with_retry_policy<R, M>(self, retry_policy: R) -> Publisher<C, R>
    where
        R: RetryPolicy<[M], PubSubError> + Clone,
        M: EncodableMessage,
    {
        Publisher {
            retry_policy,
            client: self.client,
            publish_config: self.publish_config,
        }
    }

    /// Set the publishing configuration for this `Publisher`.
    pub fn with_config(self, publish_config: pubsub::PublishConfig) -> Self {
        Self {
            publish_config,
            ..self
        }
    }
}

impl<C, M, S, R> crate::publisher::Publisher<M, S> for Publisher<C, R>
where
    C: GrpcService<BoxBody> + Clone + Send + 'static,
    C::Future: Send + 'static,
    C::Error: Into<StdError>,
    C::ResponseBody: Body<Data = Bytes> + Send + 'static,
    <C::ResponseBody as Body>::Error: Into<StdError> + Send,
    M: EncodableMessage + Send + 'static,
    S: Sink<M> + Send + 'static,
    R: RetryPolicy<[M], PubSubError> + Clone + 'static,
    R::RetryOp: Send + 'static,
    <R::RetryOp as RetryOperation<[M], PubSubError>>::Sleep: Send + 'static,
{
    type PublishError = PublishError<M, S::Error>;
    type PublishSink = PublishSink<C, M, S, R>;

    fn publish_sink_with_responses(
        self,
        validator: M::Validator,
        response_sink: S,
    ) -> Self::PublishSink {
        PublishSink {
            topic_sinks: BTreeMap::new(),
            validator,
            buffer: None,
            client: self.client,
            retry_policy: self.retry_policy,
            response_sink: Shared::new(Box::pin(response_sink)),
            publish_config: self.publish_config,
            _p: std::marker::PhantomPinned,
        }
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

impl TopicConfig<'_> {
    fn into_topic<C>(self, client: &PublisherClient<C>) -> pubsub::api::Topic {
        let mut t = pubsub::api::Topic::default();
        t.name = self.name.into_project_topic_name(client.project()).into();
        t.labels = self.labels;
        t.message_storage_policy = self.message_storage_policy;
        t.kms_key_name = self.kms_key_name;
        t.message_retention_duration = self.message_retention_duration;
        t
    }
}

impl Default for TopicConfig<'_> {
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

/// A sink for publishing messages to pubsub topics.
///
/// Created by [`Publisher::publish_sink`](crate::Publisher::publish_sink)
#[pin_project]
pub struct PublishSink<C, M: EncodableMessage, S: Sink<M>, R> {
    // The underlying sinks operate on a single topic. The incoming messages could have varying
    // topics, so this map holds a lazily initialized set of underlying sinks
    #[allow(clippy::type_complexity)] // mostly from Pin+Box
    topic_sinks: BTreeMap<Topic, Pin<Box<TopicSink<C, M, S, R>>>>,

    // The validator for the messages
    validator: M::Validator,

    // In order to know which sink to check in `poll_ready`, we need a message's topic; but we
    // won't know the topic until looking at the element in `start_send`, which contractually must
    // always be preceded by a `poll_ready`.
    //
    // Work around this chicken-egg problem by deferring readiness checking by 1 message.
    // The first `poll_ready` will always be Ready, and the first value will be seeded in this
    // buffer. Subsequent `poll_ready`s will check the *previous* message in the buffer, and try to
    // send it to its corresponding underlying sink
    buffer: Option<M>,

    // Because the sinks will be generated lazily, we need a client, retry policy, and
    // destination sink to create new per-topic sinks
    client: PublisherClient<C>,
    retry_policy: R,

    // The sink where user messages are sent once published, to inform the user that the message
    // was successfully sent.
    //
    // Boxing this sink isn't strictly necessary because it's already in an Arc which does half the
    // job of preventing moves by putting it on the heap; unfortunately there's no pin projection
    // through mutexes, so we can't mark it pinned without some unsafe shenanigans. If we go
    // unsafe, we should ditch the Arc sharing altogether and pass pointers, which should be mostly
    // fine due to the outer pinning
    response_sink: Shared<Pin<Box<S>>>,

    publish_config: pubsub::PublishConfig,

    // enable future !Unpin without breaking changes
    _p: std::marker::PhantomPinned,
}

impl<C, M, S, R> Sink<M> for PublishSink<C, M, S, R>
where
    C: GrpcService<BoxBody> + Clone + Send + 'static,
    C::Future: Send + 'static,
    C::Error: Into<StdError>,
    C::ResponseBody: Body<Data = Bytes> + Send + 'static,
    <C::ResponseBody as Body>::Error: Into<StdError> + Send,
    M: EncodableMessage + Send + 'static,
    S: Sink<M> + Send + 'static,
    R: RetryPolicy<[M], PubSubError> + Clone + 'static,
    R::RetryOp: Send + 'static,
    <R::RetryOp as RetryOperation<[M], PubSubError>>::Sleep: Send + 'static,
{
    type Error = PublishError<M, S::Error>;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        let this = self.project();
        let client = this.client;

        // Given the buffered (topic, message), find the sink corresponding to the topic
        match this.buffer.as_ref() {
            Some(msg) => {
                let topic = msg.topic();
                // look up the sink by topic. If a sink doesn't exist, initialize one
                let sink = {
                    let retry_policy = this.retry_policy;
                    let response_sink = this.response_sink;

                    // avoid cloning the topic if the key exists
                    match this.topic_sinks.get_mut(&topic) {
                        Some(existing) => existing,
                        None => this.topic_sinks.entry(topic.clone()).or_insert(Box::pin(
                            TopicSink::new(
                                client.client.publish_topic_sink(
                                    TopicName::new(topic.as_ref())
                                        .into_project_topic_name(client.project()),
                                    *this.publish_config,
                                ),
                                retry_policy.clone(),
                                Shared::clone(response_sink),
                            ),
                        )),
                    }
                };

                // poll the sink to see if it's ready
                ready!(sink.poll_ready_unpin(cx))?;

                // only take out of the buffer when we know the sink is ready
                let message = this.buffer.take().expect("already check Some");

                // validate the message with the validator
                let validated = match message.encode(this.validator) {
                    Ok(validated_msg) => validated_msg,
                    Err(err) => {
                        return Poll::Ready(Err(PublishError::InvalidMessage {
                            cause: err,
                            message,
                        }))
                    }
                };

                // convert the validated message to pubsub's message type
                let api_message = match hedwig_to_pubsub(validated, client.identifier()) {
                    Ok(api_message) => api_message,
                    Err(err) => {
                        return Poll::Ready(Err(PublishError::Publish {
                            cause: err,
                            messages: vec![message],
                        }))
                    }
                };

                // now send the message to the sink
                sink.start_send_unpin((message, api_message))?;
                Poll::Ready(Ok(()))
            }

            // The buffer could be empty on the first ever poll_ready or after explicit flushes.
            // In that case the sink is immediately ready for an element
            None => Poll::Ready(Ok(())),
        }
    }

    fn start_send(self: Pin<&mut Self>, item: M) -> Result<(), Self::Error> {
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
        for sink in self.topic_sinks.values_mut() {
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
        for sink in self.topic_sinks.values_mut() {
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
) -> Result<pubsub::api::PubsubMessage, PubSubError> {
    let mut attributes = std::mem::take(msg.headers_mut());

    if let Some(invalid_key) = attributes.keys().find(|key| key.starts_with("hedwig_")) {
        return Err(PubSubError::invalid_argument(format!(
            "keys starting with \"hedwig_\" are reserved: {}",
            invalid_key
        )));
    }

    attributes.insert(crate::HEDWIG_ID.into(), msg.uuid().to_string());
    attributes.insert(
        crate::HEDWIG_MESSAGE_TIMESTAMP.into(),
        msg.timestamp()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|_| {
                PubSubError::invalid_argument(format!(
                    "timestamp should be after UNIX epoch: {:?}",
                    msg.timestamp()
                ))
            })?
            .as_millis()
            .to_string(),
    );
    attributes.insert(crate::HEDWIG_SCHEMA.into(), msg.schema().into());
    attributes.insert(crate::HEDWIG_PUBLISHER.into(), publisher_id.into());
    attributes.insert(crate::HEDWIG_FORMAT_VERSION.into(), "1.0".into());

    let mut m = pubsub::api::PubsubMessage::default();
    m.data = msg.into_data();
    m.attributes = attributes;

    Ok(m)
}

/// Translation mechanisms for converting between user messages and api messages.
///
/// While the user submits messages of arbitrary type `M` to the publisher, that information is
/// transformed (first by the generic validator, then a pubsub-specific conversion) into a concrete
/// type (`pubsub::api::PubsubMessage`) to actually communicate with the remote service. Some
/// operations then require user input based on messages in the api type (for example, checking
/// whether a retry is necessary) but the api type is meaningless to the user, they only understand
/// `M`.
///
/// This module provides several means of translating from the api type back into the type `M`
/// (without explicit de-transformation).
mod message_translate {
    use super::*;

    /// A buffer which will hold un-encoded user messages while the encoded version of the message is
    /// published. After publishing (or on encountering an error) the encoded version is mapped back to
    /// this user message so that success (or errors) can be reported in terms of the user's familiar
    /// type, rather than an opaque encoded/serialized version.
    ///
    /// The actual mapping mechanism is ordering-based synchronization. This buffer will
    /// maintain a queue of `M` which is implicitly in the same order as the pubsub library's
    /// internal buffer; one `M` will be pushed here for every corresponding api message pushed to
    /// the lib's buffer, and conversely popped when the corresponding api messages are published
    /// in order. This relies on the pubsub lib's documented preservation of FIFO order.
    ///
    /// This ordering is also preserved after errors. The pubsub sink will report errors along with
    /// the affected messages; this buffer will remove user messages for each error-carried message
    /// to relay back to the user.
    struct TranslateBuffer<M> {
        buf: VecDeque<M>,
    }

    impl<M> TranslateBuffer<M> {
        /// The maximum number of messages that could be inserted before a publisher flushes.
        ///
        /// This is defined by the pubsub service
        const PUBLISH_BUFFER_SIZE: usize = 1000;

        fn new() -> Self {
            Self {
                buf: VecDeque::with_capacity(Self::PUBLISH_BUFFER_SIZE),
            }
        }

        fn add_message(&mut self, user_message: M) {
            self.buf.push_back(user_message)
        }

        fn remove_success(&mut self, _api_message: pubsub::api::PubsubMessage) -> M {
            self.buf
                .pop_front()
                .expect("translate buffer should be in sync with publish buffer")
        }

        fn remove_errors(
            &mut self,
            error: pubsub::PublishError,
        ) -> (PubSubError, impl Iterator<Item = M> + '_) {
            (error.source, self.buf.drain(0..error.messages.len()))
        }

        fn view_messages(&mut self, api_messages: &[pubsub::api::PubsubMessage]) -> &[M] {
            // When a publishing request fails, a retry may be attempted; that retry policy will
            // check on the request payload and the user may choose to retry or not. That payload
            // needs to be translated back into user messages for retry assessment.
            //
            // Ideally we could return a subrange of the vecdeque, but the retry policy API
            // provides the user with `&T` of the failed request, so we can only return a reference
            // and not an instantiated struct. We _can_ get slices of the underlying queue,
            // but a vecdeque might be split into two segments so it wouldn't be a single reference.
            //
            // This call moves elements within the queue such that it all exists in a contiguous
            // segment (while preserving order); then we can return just a single slice. This only
            // happens on publishing errors, so all the moves aren't in the common path and
            // probably won't be a big problem in practice.
            //
            // There is a crate https://crates.io/crates/slice-deque that can create a slice
            // without this data movement (by using clever virtual memory tricks). That's an ideal
            // candidate for this use case (long-lived buffer, ideally contiguous) but its
            // (un)safety makes me nervous, whereas std's vecdeque has more eyes on it
            &self.buf.make_contiguous()[0..api_messages.len()]
        }
    }

    /// A wrapper over the pubsub sink which holds the user message buffer and provides message
    /// translation for the response sink and retry policy
    #[pin_project]
    pub(super) struct TopicSink<C, M, S: Sink<M>, R> {
        user_messages: Shared<TranslateBuffer<M>>,
        #[pin]
        pubsub_sink: pubsub::PublishTopicSink<C, TranslateRetryPolicy<M, R>, TranslateSink<M, S>>,
    }

    pub(super) enum TopicSinkError<M, E> {
        Publish(PubSubError, Vec<M>),
        Response(E),
    }

    impl<C, M, S: Sink<M>, R> TopicSink<C, M, S, R>
    where
        S: Sink<M>,
        R: RetryPolicy<[M], PubSubError>,
    {
        pub(super) fn new(
            pubsub_sink: pubsub::PublishTopicSink<C>,
            retry_policy: R,
            response_sink: Shared<Pin<Box<S>>>,
        ) -> Self {
            let user_messages = Shared::new(TranslateBuffer::new());
            Self {
                user_messages: Shared::clone(&user_messages),
                pubsub_sink: pubsub_sink
                    .with_retry_policy(TranslateRetryPolicy {
                        user_messages: Shared::clone(&user_messages),
                        user_retry: retry_policy,
                    })
                    .with_response_sink(TranslateSink {
                        user_messages,
                        user_sink: response_sink,
                    }),
            }
        }

        /// Translate the error type of a poll_x function into one holding user messages instead of
        /// api messages
        fn translate_poll_fn<F>(
            self: Pin<&mut Self>,
            poll_fn: F,
            cx: &mut Context,
        ) -> Poll<Result<(), TopicSinkError<M, S::Error>>>
        where
            F: FnOnce(
                Pin<
                    &mut pubsub::PublishTopicSink<
                        C,
                        TranslateRetryPolicy<M, R>,
                        TranslateSink<M, S>,
                    >,
                >,
                &mut Context,
            ) -> Poll<Result<(), pubsub::SinkError<S::Error>>>,
        {
            let this = self.project();
            let user_messages = this.user_messages;

            poll_fn(this.pubsub_sink, cx).map_err(|err| match err {
                pubsub::SinkError::Publish(publish_error) => {
                    let mut user_messages = user_messages.borrow_mut();
                    let (source, messages) = user_messages.remove_errors(publish_error);
                    TopicSinkError::Publish(source, messages.collect())
                }
                pubsub::SinkError::Response(response_error) => {
                    TopicSinkError::Response(response_error)
                }
            })
        }
    }

    impl<C, M, S: Sink<M>, R> Sink<(M, pubsub::api::PubsubMessage)> for TopicSink<C, M, S, R>
    where
        C: GrpcService<BoxBody> + Clone + Send + 'static,
        C::Future: Send + 'static,
        C::Error: Into<StdError>,
        C::ResponseBody: Body<Data = Bytes> + Send + 'static,
        <C::ResponseBody as Body>::Error: Into<StdError> + Send,
        R: RetryPolicy<[M], PubSubError> + 'static,
        R::RetryOp: Send + 'static,
        <R::RetryOp as RetryOperation<[M], PubSubError>>::Sleep: Send + 'static,
        S: Sink<M> + Send + 'static,
        M: EncodableMessage + Send + 'static,
    {
        type Error = TopicSinkError<M, S::Error>;

        fn poll_ready(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
            self.translate_poll_fn(pubsub::PublishTopicSink::poll_ready, cx)
        }

        fn start_send(
            self: Pin<&mut Self>,
            (user_message, api_message): (M, pubsub::api::PubsubMessage),
        ) -> Result<(), Self::Error> {
            let this = self.project();

            // try to send the api message to the sink. Only if successful will it be added to the
            // buffer; if it fails some argument check, the buffer does not need to be popped for
            // translation
            match this.pubsub_sink.start_send(api_message) {
                Ok(()) => {
                    this.user_messages.borrow_mut().add_message(user_message);
                    Ok(())
                }
                Err(err) => Err(match err {
                    pubsub::SinkError::Publish(publish_error) => {
                        assert_eq!(publish_error.messages.len(), 1);
                        TopicSinkError::Publish(publish_error.source, vec![user_message])
                    }
                    pubsub::SinkError::Response(_) => {
                        unreachable!("response sink should not be used in start_send")
                    }
                }),
            }
        }

        fn poll_flush(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
            self.translate_poll_fn(pubsub::PublishTopicSink::poll_flush, cx)
        }
        fn poll_close(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
            self.translate_poll_fn(pubsub::PublishTopicSink::poll_close, cx)
        }
    }

    /// A retry policy which can be used by pubsub to retry api messages, but will provide the user
    /// with user messages to assess retry-worthyness
    struct TranslateRetryPolicy<M, R> {
        user_messages: Shared<TranslateBuffer<M>>,
        user_retry: R,
    }

    impl<M, R> RetryPolicy<pubsub::api::PublishRequest, PubSubError> for TranslateRetryPolicy<M, R>
    where
        R: RetryPolicy<[M], PubSubError>,
    {
        type RetryOp = TranslateRetryOp<M, R::RetryOp>;

        fn new_operation(&mut self) -> Self::RetryOp {
            TranslateRetryOp {
                user_messages: Shared::clone(&self.user_messages),
                user_retry_op: self.user_retry.new_operation(),
            }
        }
    }

    struct TranslateRetryOp<M, O> {
        user_messages: Shared<TranslateBuffer<M>>,
        user_retry_op: O,
    }

    impl<M, O> RetryOperation<pubsub::api::PublishRequest, PubSubError> for TranslateRetryOp<M, O>
    where
        O: RetryOperation<[M], PubSubError>,
    {
        type Sleep = O::Sleep;

        fn check_retry(
            &mut self,
            failed_value: &pubsub::api::PublishRequest,
            error: &PubSubError,
        ) -> Option<Self::Sleep> {
            // Given a failed request with api messages, translate it into user messages
            let mut user_messages = self.user_messages.borrow_mut();
            let failed_messages = user_messages.view_messages(&failed_value.messages);

            self.user_retry_op.check_retry(failed_messages, error)
        }
    }

    /// A sink used to translate successful publishing responses from api messages back to user
    /// messages for consumption by the user's response sink
    struct TranslateSink<M, S: Sink<M>> {
        user_messages: Shared<TranslateBuffer<M>>,
        user_sink: Shared<Pin<Box<S>>>,
    }

    impl<M, S> Sink<pubsub::api::PubsubMessage> for TranslateSink<M, S>
    where
        S: Sink<M>,
    {
        type Error = S::Error;

        fn poll_ready(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
            self.user_sink.borrow_mut().poll_ready_unpin(cx)
        }
        fn start_send(
            self: Pin<&mut Self>,
            api_message: pubsub::api::PubsubMessage,
        ) -> Result<(), Self::Error> {
            let user_message = self.user_messages.borrow_mut().remove_success(api_message);
            self.user_sink.borrow_mut().start_send_unpin(user_message)
        }
        fn poll_flush(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
            self.user_sink.borrow_mut().poll_flush_unpin(cx)
        }
        fn poll_close(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Self::Error>> {
            self.user_sink.borrow_mut().poll_close_unpin(cx)
        }
    }
}
