//! Types, traits, and functions necessary to consume messages using hedwig
//!
//! See the [`Consumer`] trait.

use crate::ValidatedMessage;
use async_trait::async_trait;
use either::Either;
use futures_util::stream;
use pin_project::pin_project;
use std::{
    pin::Pin,
    task::{Context, Poll},
};

/// Message consumers ingest messages from a queue service and present them to the user application
/// as a [`Stream`](futures_util::stream::Stream).
///
/// ## Acknowledging Messages
/// Typically message services deliver messages with a particular delivery time window, during
/// which this message won't be sent to other consumers. In AWS SQS this is called the [visibility
/// timeout][AWS], and in GCP PubSub this is the [ack deadline][GCP].
///
/// If a message is successfully acknowledged within this time, it will be considered processed and
/// not delivered to other consumers (and possibly deleted depending on the service's
/// configuration). A message can conversely be negatively-acknowledged, to indicate e.g.
/// processing has failed and the message should be delivered again to some consumer. This time
/// window can also be modified for each message, to allow for longer or shorter message processing
/// than the default configured time window.
///
/// Implementations of this trait do not ack/nack/modify messages themselves, and instead present
/// this functionality to users with the [`AcknowledgeableMessage`] type. Message processors are
/// responsible for handling message acknowledgement, including extensions for processing time as
/// necessary.
// If we had async drop, sending nacks on drop would be nice. Alas, rust isn't there yet
///
/// Bear in mind that message delivery and acknowledgement are all best-effort in distributed
/// message services. An acknowledged or extended message may still be re-delivered for any number
/// of reasons, and applications should be made resilient to such events.
///
/// [AWS]: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html
/// [GCP]: https://cloud.google.com/pubsub/docs/subscriber
pub trait Consumer {
    /// The type of acknowledgement tokens produced by the underlying service implementation
    type AckToken: AcknowledgeToken;
    /// Errors encountered while streaming messages
    type Error;
    /// The stream returned by [`stream`]
    type Stream: stream::Stream<
        Item = Result<AcknowledgeableMessage<Self::AckToken, ValidatedMessage>, Self::Error>,
    >;

    /// Begin pulling messages from the backing message service.
    ///
    /// The messages produced by this stream have not been decoded yet. Users should typically call
    /// [`consume`](Consumer::consume) instead, to produce decoded messages.
    fn stream(self) -> Self::Stream;

    /// Create a stream of decoded messages from this consumer, using a validator for the given
    /// [decodable](DecodableMessage) message type.
    fn consume<M>(self, validator: M::Validator) -> MessageStream<Self::Stream, M::Validator, M>
        where Self: Sized,
              M: DecodableMessage,
    {
        MessageStream {
            stream: self.stream(),
            validator,
            _message_type: std::marker::PhantomData,
        }
    }
}

/// Messages which can be decoded from a [`ValidatedMessage`] stream.
pub trait DecodableMessage {
    /// The error returned when a message fails to decode
    type Error;

    /// The validator used to decode a validated message
    type Validator;

    /// Decode the given message, using the given validator, into its structured type
    fn decode(msg: ValidatedMessage, validator: &Self::Validator) -> Result<Self, Self::Error>
    where
        Self: Sized;
}

/// A received message which can be acknowledged to prevent re-delivery by the backing message
/// service.
///
/// See the documentation for acknowledging messages on [`Consumer`]
#[must_use = "Messages should be ack'ed to prevent repeated delivery, or nack'ed to improve responsiveness"]
pub struct AcknowledgeableMessage<A, M> {
    /// The acknowledgement token which executes the ack/nack/modify operations
    pub ack_token: A,

    /// The underlying message
    pub message: M,
}

impl<A, M> AcknowledgeableMessage<A, M>
where
    A: AcknowledgeToken,
{
    /// Acknowledge this message, declaring that processing was successful and the message should
    /// not be re-delivered to consumers.
    pub async fn ack(self) -> Result<M, A::AckError> {
        self.ack_token.ack().await?;
        Ok(self.message)
    }

    /// Negatively acknowledge this message, declaring that processing was unsuccessful and the
    /// message should be re-delivered to consumers.
    pub async fn nack(self) -> Result<M, A::NackError> {
        self.ack_token.nack().await?;
        Ok(self.message)
    }

    /// Modify the acknowledgement deadline for this message to the given number of seconds.
    ///
    /// The new deadline will typically be this number of seconds after the service receives this
    /// modification requesst, though users should check their implementation's documented
    /// behavior.
    pub async fn modify_deadline(&mut self, seconds: u32) -> Result<(), A::ModifyError> {
        self.ack_token.modify_deadline(seconds).await
    }
}

impl<A, M> std::ops::Deref for AcknowledgeableMessage<A, M> {
    type Target = M;

    fn deref(&self) -> &M {
        &self.message
    }
}

impl<A, M> std::ops::DerefMut for AcknowledgeableMessage<A, M> {
    fn deref_mut(&mut self) -> &mut M {
        &mut self.message
    }
}

/// A token associated with some message received from a message service, used to issue an
/// ack/nack/modify request
///
/// See the documentation for acknowledging messages on [`Consumer`]
#[async_trait]
#[must_use = "Messages should be ack'ed to prevent repeated delivery, or nack'ed to improve responsiveness"]
pub trait AcknowledgeToken {
    /// Errors returned by [`ack`](AcknowledgeToken::ack)
    type AckError;
    /// Errors returned by [`nack`](AcknowledgeToken::nack)
    type NackError;
    /// Errors returned by [`modify_deadline`](AcknowledgeToken::modify_deadline)
    type ModifyError;

    /// Acknowledge the associated message
    async fn ack(self) -> Result<(), Self::AckError>;

    /// Negatively acknowledge the associated message
    async fn nack(self) -> Result<(), Self::NackError>;

    /// Change the associated message's acknowledge deadline to the given number of seconds
    // uses u32 seconds instead of e.g. Duration because SQS and PubSub both have second
    // granularity; Duration::from_millis(999) would truncate to 0, which might be surprising
    async fn modify_deadline(&mut self, seconds: u32) -> Result<(), Self::ModifyError>;
}

/// The stream returned by the [`consume`](Consumer::consume) function
#[pin_project]
pub struct MessageStream<S, V, M> {
    #[pin]
    stream: S,
    validator: V,
    _message_type: std::marker::PhantomData<M>,
}

impl<S, V, M, AckToken, StreamError> stream::Stream for MessageStream<S, V, M>
where
    S: stream::Stream<
        Item = Result<AcknowledgeableMessage<AckToken, ValidatedMessage>, StreamError>,
    >,
    M: DecodableMessage<Validator = V>,
{
    #[allow(clippy::type_complexity)] // it is what it is, aliases would all be generic anyway
    type Item = Result<AcknowledgeableMessage<AckToken, M>, Either<StreamError, M::Error>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let validator = this.validator;
        this.stream.poll_next(cx).map(|opt| {
            opt.map(|res| {
                res.map_err(Either::Left).and_then(
                    |AcknowledgeableMessage { ack_token, message }| {
                        Ok(AcknowledgeableMessage {
                            ack_token,
                            message: M::decode(message, validator).map_err(Either::Right)?,
                        })
                    },
                )
            })
        })
    }
}
