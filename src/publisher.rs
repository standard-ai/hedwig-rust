//! Types, traits, and functions necessary to publish messages using hedwig

use crate::{Topic, ValidatedMessage};
use futures_util::sink;
use std::{
    pin::Pin,
    task::{Context, Poll},
};

/// Message publishers.
///
/// Message publishers validate, encode, and deliver messages to an endpoint, possibly a remote
/// one. Message publishers may also additionally validate a message for publisher-specific
/// requirements (e.g.  size).
pub trait Publisher<M: EncodableMessage, S: sink::Sink<M> = Drain<M>> {
    /// The error type that may be encountered when publishing a message
    type PublishError;
    /// The [`Sink`](futures_util::sink::Sink) type provided by the publisher to accept messages,
    /// validate them, then publish them to the destination.
    type PublishSink: sink::Sink<M, Error = Self::PublishError>;

    /// Create a new sink to accept messages.
    ///
    /// The sink will use the given validator to validate and/or encode messages, possibly batch
    /// them together, then publish them to their destination. The details of the internal encoding
    /// and batching may vary by `Publisher` implementation.
    fn publish_sink(self, validator: M::Validator) -> Self::PublishSink
    where
        Self: Sized,
        S: Default,
    {
        self.publish_sink_with_responses(validator, S::default())
    }

    /// Create a new sink to accept messages.
    ///
    /// This creates a sink like [`publish_sink`](Publisher::publish_sink) while additionally
    /// listening for successful responses; after a message has been successfully published, it
    /// will be passed to the given response sink to complete any necessary work (e.g.
    /// acknowledging success or collecting metrics)
    fn publish_sink_with_responses(
        self,
        validator: M::Validator,
        response_sink: S,
    ) -> Self::PublishSink;
}

/// Types that can be encoded and published.
pub trait EncodableMessage {
    /// The errors that can occur when calling the [`EncodableMessage::encode`] method.
    ///
    /// Will typically match the errors returned by the [`EncodableMessage::Validator`].
    type Error;

    /// The validator to use for this message.
    type Validator;

    /// Topic into which this message shall be published.
    fn topic(&self) -> Topic;

    /// Encode the message payload.
    fn encode(&self, validator: &Self::Validator) -> Result<ValidatedMessage, Self::Error>;
}

/// Like [`futures_util::sink::Drain`] but implements `Default`
#[derive(Debug)]
pub struct Drain<T>(std::marker::PhantomData<T>);

impl<T> Default for Drain<T> {
    fn default() -> Self {
        Self(std::marker::PhantomData)
    }
}

impl<T> sink::Sink<T> for Drain<T> {
    type Error = futures_util::never::Never;

    fn poll_ready(self: Pin<&mut Self>, _: &mut Context) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn start_send(self: Pin<&mut Self>, _: T) -> Result<(), Self::Error> {
        Ok(())
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn poll_close(self: Pin<&mut Self>, _: &mut Context) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}
