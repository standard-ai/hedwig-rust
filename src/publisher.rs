//! Types, traits, and functions necessary to publish messages using hedwig

use crate::{Topic, ValidatedMessage};
use either::Either;
use futures_util::sink;
use pin_project::pin_project;
use std::{
    pin::Pin,
    task::{Context, Poll},
};

/// Message publishers.
///
/// Message publishers validate, encode, and deliver messages to an endpoint, possibly a remote
/// one. Message publishers may also additionally validate a message for publisher-specific
/// requirements (e.g.  size).
pub trait Publisher<M: EncodableMessage> {
    /// The error type that may be encountered when publishing a message
    type PublishError;
    /// The [`Sink`](futures_util::sink::Sink) type provided by the publisher to accept messages,
    /// validate them, then publish them to the destination.
    type PublishSink: sink::Sink<M, Error = Either<M::Error, Self::PublishError>>;

    /// Create a new sink to accept messages.
    ///
    /// The sink will use the given validator to validate and/or encode messages, possibly batch
    /// them together, then publish them to their destination. The details of the internal encoding
    /// and batching may vary by `Publisher` implementation.
    fn publish_sink(self, validator: M::Validator) -> Self::PublishSink;
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
    fn encode(self, validator: &Self::Validator) -> Result<ValidatedMessage, Self::Error>;
}

/// A sink which ingests messages, validates them with the given validator, then forwards them to
/// the given destination sink.
pub fn validator_sink<M, S>(
    validator: M::Validator,
    destination_sink: S,
) -> ValidatorSink<M, M::Validator, S>
where
    M: EncodableMessage,
    S: sink::Sink<(Topic, ValidatedMessage)>,
{
    ValidatorSink {
        _message_type: std::marker::PhantomData,
        validator,
        sink: destination_sink,
    }
}

/// The sink returned by the [`validator_sink`](validator_sink) function
#[pin_project]
pub struct ValidatorSink<M, V, S> {
    _message_type: std::marker::PhantomData<M>,
    validator: V,
    #[pin]
    sink: S,
}

impl<M, S> sink::Sink<M> for ValidatorSink<M, M::Validator, S>
where
    M: EncodableMessage,
    S: sink::Sink<(Topic, ValidatedMessage)>,
{
    type Error = Either<M::Error, S::Error>;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project()
            .sink
            .poll_ready(cx)
            .map(|res| res.map_err(Either::Right))
    }

    fn start_send(self: Pin<&mut Self>, message: M) -> Result<(), Self::Error> {
        let this = self.project();

        let topic = message.topic();
        let validated_message = message.encode(this.validator).map_err(Either::Left)?;

        this.sink
            .start_send((topic, validated_message))
            .map_err(Either::Right)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project()
            .sink
            .poll_flush(cx)
            .map(|res| res.map_err(Either::Right))
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project()
            .sink
            .poll_close(cx)
            .map(|res| res.map_err(Either::Right))
    }
}
