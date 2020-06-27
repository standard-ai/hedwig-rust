use crate::{Publisher, PublisherResult, ValidatedMessage};

use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task,
};
use uuid::Uuid;

/// A mock publisher that stores messages in-memory for later verification
///
/// This is useful primarily in tests.
///
/// # Examples
///
/// ```
/// use hedwig::publishers::MockPublisher;
/// let publisher = MockPublisher::default();
/// let publisher_view = publisher.clone();
/// ```
#[derive(Debug, Default, Clone)]
pub struct MockPublisher(Arc<Mutex<Vec<(&'static str, ValidatedMessage)>>>);

impl MockPublisher {
    /// Verify that a message was published. This method asserts that the message you expected to
    /// be published, was indeed published
    ///
    /// Panics if the message was not published.
    pub fn assert_message_published(&self, topic: &'static str, uuid: Uuid) {
        {
            let lock = self.0.lock().expect("this mutex cannot get poisoned");
            for (mt, msg) in &lock[..] {
                if mt == &topic && msg.id == uuid {
                    return;
                }
            }
        }
        panic!(
            "Message with uuid {} was not published to topic {}",
            uuid, topic
        );
    }
}

impl Publisher for MockPublisher {
    type MessageId = Uuid;
    type PublishFuture = MockPublishFuture;

    fn publish(&self, topic: &'static str, messages: Vec<ValidatedMessage>) -> Self::PublishFuture {
        let mut lock = self.0.lock().expect("this mutex cannot get poisoned");
        let num_messages = messages.len();
        let mut ids = Vec::with_capacity(num_messages);
        for message in messages {
            ids.push(message.id);
            lock.push((topic, message));
        }
        MockPublishFuture(ids)
    }
}

pub struct MockPublishFuture(Vec<Uuid>);

impl Future for MockPublishFuture {
    type Output = PublisherResult<Uuid>;

    fn poll(mut self: Pin<&mut Self>, _: &mut task::Context<'_>) -> task::Poll<Self::Output> {
        task::Poll::Ready(PublisherResult::Success(std::mem::replace(
            &mut self.0,
            Vec::new(),
        )))
    }
}
