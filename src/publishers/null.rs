use crate::{Publisher, ValidatedMessage};
use futures_util::stream::Stream;
use std::{pin::Pin, task};

/// A blackhole publisher that doesn't publish messages anywhere.
///
/// Great for conditionally disabling publishing.
///
/// # Examples
///
/// ```
/// use hedwig::publishers::NullPublisher;
/// let publisher = NullPublisher::default();
/// ```
#[derive(Debug, Default, Clone, Copy)]
pub struct NullPublisher;

impl Publisher for NullPublisher {
    type MessageId = ();
    type MessageError = std::convert::Infallible;
    type PublishStream = NullPublishStream;

    fn publish<'a, I>(&self, _: &'static str, messages: I) -> Self::PublishStream
    where
        I: Iterator<Item = &'a ValidatedMessage> + ExactSizeIterator,
    {
        NullPublishStream(0..messages.len())
    }
}

/// Stream for `NullPublisher::publish`.
pub struct NullPublishStream(std::ops::Range<usize>);

impl Stream for NullPublishStream {
    type Item = Result<(), std::convert::Infallible>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        _: &mut task::Context<'_>,
    ) -> task::Poll<Option<Self::Item>> {
        task::Poll::Ready(self.0.next().map(|_| Ok(())))
    }
}
