use std::{future::Future, pin::Pin, task};

use crate::{Publisher, PublisherResult, ValidatedMessage};

/// A blackhole publisher that doesn't publish messages
///
/// Great for conditionally enabling publishing.
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
    type PublishFuture = NullPublishFuture;

    fn publish(&self, _: &'static str, messages: Vec<ValidatedMessage>) -> Self::PublishFuture {
        NullPublishFuture(messages.len())
    }
}

/// Future for `NullPublisher::publish`.
pub struct NullPublishFuture(usize);

impl Future for NullPublishFuture {
    type Output = PublisherResult<()>;

    fn poll(self: Pin<&mut Self>, _: &mut task::Context<'_>) -> task::Poll<Self::Output> {
        task::Poll::Ready(PublisherResult::Success(vec![(); self.0]))
    }
}
