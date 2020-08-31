use crate::{Publisher, ValidatedMessage};

use std::{
    pin::Pin,
    sync::{Arc, Mutex},
    task,
};

use futures_util::stream::Stream;
use uuid::Uuid;

/// A mock publisher that stores messages in-memory for later verification.
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
    /// Create a new mock publisher.
    pub fn new() -> Self {
        Default::default()
    }

    /// Number of messages published into this publisher.
    pub fn len(&self) -> usize {
        let lock = self.0.lock().expect("this mutex cannot get poisoned");
        lock.len()
    }

    /// Number of messages published into this publisher.
    pub fn is_empty(&self) -> bool {
        let lock = self.0.lock().expect("this mutex cannot get poisoned");
        lock.is_empty()
    }

    /// Verify that a message was published. This method asserts that the message you expected to
    /// be published, was indeed published
    ///
    /// Panics if the message was not published.
    pub fn assert_message_published(&self, topic: &'static str, uuid: &Uuid) {
        {
            let lock = self.0.lock().expect("this mutex cannot get poisoned");
            for (mt, msg) in &lock[..] {
                if mt == &topic && &msg.id == uuid {
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
    type MessageError = std::convert::Infallible;
    type PublishStream = MockPublishStream;

    fn publish<'a, I>(&self, topic: &'static str, messages: I) -> Self::PublishStream
    where
        I: Iterator<Item = &'a ValidatedMessage> + ExactSizeIterator,
    {
        let data = self.0.clone();
        let messages: Vec<_> = messages.cloned().collect();
        MockPublishStream(Box::new(messages.into_iter().map(move |msg| {
            let id = msg.id;
            data.lock()
                .expect("this mutex cannot get poisoned")
                .push((topic, msg));
            id
        })))
    }
}

/// Stream of mock publisher results.
pub struct MockPublishStream(Box<dyn Iterator<Item = Uuid> + Send + Sync>);

impl Stream for MockPublishStream {
    type Item = Result<Uuid, std::convert::Infallible>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        _: &mut task::Context<'_>,
    ) -> task::Poll<Option<Self::Item>> {
        task::Poll::Ready(self.0.next().map(Ok))
    }
}
