//! Provides a `Sink` interface for publishing to hedwig.

use crate::{
    publish::{EncodableMessage, PublishBatch, Publisher},
    Topic, ValidatedMessage,
};
use either::Either;
use futures_util::{ready, sink::Sink, stream::Stream};
use pin_project::pin_project;
use std::{
    pin::Pin,
    task::{Context, Poll},
};

/// A sink which ingests messages, validates them with the given validator, then forwards them to
/// the given destination sink.
pub fn validator_sink<M, S>(
    validator: M::Validator,
    destination_sink: S,
) -> ValidatorSink<M, M::Validator, S>
where
    M: EncodableMessage,
    S: Sink<(Topic, ValidatedMessage)>,
{
    ValidatorSink {
        _message_type: std::marker::PhantomData,
        validator,
        sink: destination_sink,
    }
}

/// A sink which ingests validated messages and publishes them to the given publisher.
///
/// This sink internally batches elements to publish multiple messages at once. The `batch_size`
/// argument can be adjusted to control the number of elements stored in these batches.
/// `poll_ready` will check whether inserting an additional element would exceed this size limit,
/// and trigger a flush before returning `Ready` if so. Users may call `poll_flush` to empty this
/// batch at any time.
///
/// Unlike some sinks, this sink's polling functions can be resumed after encountering an error, so
/// long as the underlying publisher's errors are not terminal. Transient errors, for example, can
/// be ignored and polling can be resumed to continue publishing.
///
/// The sink can accept new elements while a flush is in progress, so long as the internal batch
/// has additional capacity -- i.e. `poll_ready` may return `Ready` while `poll_flush` returns
/// `Pending`. Together with the resume-on-error support mentioned above and the data in the
/// [`FailedMessage`](FailedMessage) error type, this behavior can be used to retry failed messages
/// by re-submitting them to this same sink.
// TODO actually implement such a retry Sink adapter layer with some backoff
pub fn publisher_sink<P>(publisher: P, batch_size: usize) -> PublisherSink<P, P::PublishStream>
where
    P: Publisher,
{
    PublisherSink {
        publisher,
        batch_capacity: usize::max(1, batch_size),
        batch: PublishBatch::new(),
        flush_state: FlushState::NotFlushing,
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

impl<M, S> Sink<M> for ValidatorSink<M, M::Validator, S>
where
    M: EncodableMessage,
    S: Sink<(Topic, ValidatedMessage)>,
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

/// An error encountered when a message fails to be published
#[derive(Debug, Clone, thiserror::Error)]
#[cfg_attr(test, derive(PartialEq, Eq))]
#[error("Failed to publish a message")]
pub struct FailedMessage<E: std::error::Error + 'static> {
    /// The underlying error source
    #[source]
    pub error: E,

    /// The topic to which this publish attempt was made
    pub topic: Topic,

    /// The message which failed to be published
    pub message: ValidatedMessage,
}

/// The sink returned by the [`publisher_sink`](publisher_sink) function
#[pin_project]
pub struct PublisherSink<P, S> {
    publisher: P,
    batch_capacity: usize,
    batch: PublishBatch,
    #[pin]
    flush_state: FlushState<S>,
}

/// The sink is either flushing or not flushing. Closing and ready-checking are indirectly flushes
#[pin_project(project=FlushProjection)]
#[derive(Debug)]
enum FlushState<S> {
    NotFlushing,
    Flushing(#[pin] crate::publish::PublishBatchStream<S>),
}

impl<P> Sink<(Topic, ValidatedMessage)> for PublisherSink<P, P::PublishStream>
where
    P: Publisher,
    P::PublishStream: Unpin,
{
    type Error = FailedMessage<P::MessageError>;

    // The sink works by maintaining a PublishBatch to collect entries, and publishing that batch
    // as soon as the configured capacity is met, or an explicit flush or close is called.
    //
    // Once a flush starts, the batch creates a stream which will submit messages to the publisher.
    // This stream is driven by the `poll_flush` method of the sink; once the stream is complete,
    // `poll_flush` will return Ready.
    //
    // Because the publishing stream may treat errors as transient -- and thus may allow the stream
    // to be polled again after an error -- the sink also allows polling the flush after
    // encountering an error. Thanks to this non-terminal erroring, the sink can support retries by
    // having the caller submit failed messages back to the sink. The actual retry logic is left to
    // a layer above this sink, but the sink itself provides the mechanisms to support it by having
    // the batch collect elements while a flush is in progress.

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.batch.len() < self.batch_capacity {
            Poll::Ready(Ok(()))
        } else {
            self.poll_flush(cx)
        }
    }

    fn start_send(
        mut self: Pin<&mut Self>,
        (topic, message): (Topic, ValidatedMessage),
    ) -> Result<(), Self::Error> {
        assert!(
            self.batch.len() < self.batch_capacity,
            "start_send must be preceded by a successful poll_ready"
        );
        self.batch.push(topic, message);
        Ok(())
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        use FlushProjection::{Flushing, NotFlushing};

        let mut this = self.as_mut().project();

        // Check whether the sink is in the flushing state already or not.
        //
        // State transitions are done using this loop + match. The state variable may be updated by
        // a match arm, after which the caller will continue on the loop
        'check_state: loop {
            return match this.flush_state.as_mut().project() {
                NotFlushing => {
                    if this.batch.is_empty() {
                        // if the sink isn't flushing, and the batch is empty, there's nothing to do!
                        Poll::Ready(Ok(()))
                    } else {
                        // the sink isn't yet flushing, but there are elements to flush.
                        // take them out of the batch and start the publishing stream
                        let batch = std::mem::replace(this.batch, PublishBatch::new());
                        let publish = batch.publish(this.publisher);
                        this.flush_state.set(FlushState::Flushing(publish));

                        // re-enter the match with the new state, where the publish stream will be
                        // polled
                        continue 'check_state;
                    }
                }
                Flushing(mut flush_stream) => {
                    // if the sink is flushing, the publish stream needs to be polled until its
                    // completion
                    'poll_publish: loop {
                        break match ready!(flush_stream.as_mut().poll_next(cx)) {
                            None => {
                                // done flushing. switch to NotFlushing and check if any new
                                // elements have been added to the publish buffer
                                this.flush_state.set(FlushState::NotFlushing);

                                //re-enter the match with the new state
                                continue 'check_state;
                            }
                            // if an error occurs in publishing, pause flushing and propogate the
                            // error. The caller may continue to poll after this, and the publish
                            // attempt will resume
                            Some((Err(error), topic, message)) => Poll::Ready(Err(FailedMessage {
                                error,
                                topic,
                                message,
                            })),
                            Some((Ok(_msg_id), _topic, _message)) => {
                                // successful returns from the publishing stream simply have
                                // nothing to do. keep driving the publishing to completion
                                continue 'poll_publish;
                            }
                        };
                    }
                }
            };
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.poll_flush(cx)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn test_validated_message(data: impl Into<Vec<u8>>) -> ValidatedMessage {
        ValidatedMessage {
            id: uuid::Uuid::nil(),
            timestamp: std::time::SystemTime::UNIX_EPOCH,
            schema: "test_schema",
            headers: crate::Headers::default(),
            data: data.into(),
        }
    }

    struct TestValidator;

    #[derive(Debug, Eq, PartialEq)]
    struct TestMessage(&'static str);

    impl EncodableMessage for TestMessage {
        type Error = std::convert::Infallible;
        type Validator = TestValidator;

        fn topic(&self) -> Topic {
            "test_topic"
        }

        fn encode(self, _: &Self::Validator) -> Result<ValidatedMessage, Self::Error> {
            Ok(test_validated_message(self.0))
        }
    }

    mod validator_sink {
        use super::*;

        struct CountingSink<T> {
            poll_ready_called: u32,
            start_send_called: u32,
            poll_flush_called: u32,
            poll_close_called: u32,
            elements: Vec<T>,
        }

        impl<T> CountingSink<T> {
            fn new() -> Self {
                Self {
                    poll_ready_called: 0,
                    start_send_called: 0,
                    poll_flush_called: 0,
                    poll_close_called: 0,
                    elements: Vec::new(),
                }
            }
        }

        // not auto-impl'ed because of T maybe?
        impl<T> Unpin for CountingSink<T> {}

        impl<T> Sink<T> for CountingSink<T> {
            type Error = std::convert::Infallible;

            fn poll_ready(
                mut self: Pin<&mut Self>,
                _: &mut Context<'_>,
            ) -> Poll<Result<(), Self::Error>> {
                self.poll_ready_called += 1;
                Poll::Ready(Ok(()))
            }

            fn start_send(mut self: Pin<&mut Self>, item: T) -> Result<(), Self::Error> {
                self.start_send_called += 1;
                self.elements.push(item);
                Ok(())
            }

            fn poll_flush(
                mut self: Pin<&mut Self>,
                _: &mut Context<'_>,
            ) -> Poll<Result<(), Self::Error>> {
                self.poll_flush_called += 1;
                Poll::Ready(Ok(()))
            }

            fn poll_close(
                mut self: Pin<&mut Self>,
                _: &mut Context<'_>,
            ) -> Poll<Result<(), Self::Error>> {
                self.poll_close_called += 1;
                Poll::Ready(Ok(()))
            }
        }

        #[test]
        fn methods_delegated() {
            let mut sink = validator_sink::<TestMessage, _>(TestValidator, CountingSink::new());

            let mut cx = Context::from_waker(futures_util::task::noop_waker_ref());

            // sanity check
            assert_eq!(0, sink.sink.poll_ready_called);
            assert_eq!(0, sink.sink.start_send_called);
            assert_eq!(0, sink.sink.poll_flush_called);
            assert_eq!(0, sink.sink.poll_close_called);
            assert_eq!(Vec::<(Topic, ValidatedMessage)>::new(), sink.sink.elements);

            assert_eq!(Poll::Ready(Ok(())), Pin::new(&mut sink).poll_ready(&mut cx));

            assert_eq!(1, sink.sink.poll_ready_called);
            assert_eq!(0, sink.sink.start_send_called);
            assert_eq!(0, sink.sink.poll_flush_called);
            assert_eq!(0, sink.sink.poll_close_called);

            assert_eq!(Poll::Ready(Ok(())), Pin::new(&mut sink).poll_flush(&mut cx));

            assert_eq!(1, sink.sink.poll_ready_called);
            assert_eq!(0, sink.sink.start_send_called);
            assert_eq!(1, sink.sink.poll_flush_called);
            assert_eq!(0, sink.sink.poll_close_called);

            assert_eq!(Poll::Ready(Ok(())), Pin::new(&mut sink).poll_close(&mut cx));

            assert_eq!(1, sink.sink.poll_ready_called);
            assert_eq!(0, sink.sink.start_send_called);
            assert_eq!(1, sink.sink.poll_flush_called);
            assert_eq!(1, sink.sink.poll_close_called);

            assert_eq!(Ok(()), Pin::new(&mut sink).start_send(TestMessage("foo")));

            assert_eq!(1, sink.sink.poll_ready_called);
            assert_eq!(1, sink.sink.start_send_called);
            assert_eq!(1, sink.sink.poll_flush_called);
            assert_eq!(1, sink.sink.poll_close_called);
            assert_eq!(
                vec![(
                    "test_topic",
                    TestMessage("foo").encode(&TestValidator).unwrap()
                )],
                sink.sink.elements
            );
        }
    }

    mod publisher_sink {
        use super::*;
        use crate::publish::publishers::MockPublisher;
        use futures_util::pin_mut;
        use std::{cell::RefCell, rc::Rc};

        #[derive(Debug, Eq, PartialEq, thiserror::Error)]
        #[error("test error")]
        struct TestError(String);

        /// what should the publish stream do next
        #[derive(Debug)]
        enum PublishCommand {
            /// return Poll::Pending
            Pending,

            /// advance the stream
            Next,

            /// discard the next value and return the given error
            Error(TestError),
        }

        struct ControlledPublishStream {
            command: Rc<RefCell<PublishCommand>>,
            mock: MockPublisher,
            messages: std::vec::IntoIter<(Topic, ValidatedMessage)>,
        }

        impl Stream for ControlledPublishStream {
            type Item = Result<(), TestError>;

            fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
                use PublishCommand::{Error, Next, Pending};

                // reset to Pending every time so that advancing has to be done affirmatively
                let command = std::mem::replace(&mut *self.command.borrow_mut(), Pending);

                match command {
                    Pending => {
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    }
                    Next => {
                        // publish the next element to the mock
                        let (topic, message) = match self.messages.next() {
                            None => return Poll::Ready(None),
                            Some(t) => t,
                        };

                        let stream = self.mock.publish(topic, std::iter::once(&message));
                        pin_mut!(stream);

                        // mock publishing shouldn't fail
                        assert_eq!(
                            Poll::Ready(Some(Ok(*message.uuid()))),
                            stream.as_mut().poll_next(cx)
                        );
                        assert_eq!(Poll::Ready(None), stream.as_mut().poll_next(cx));

                        Poll::Ready(Some(Ok(())))
                    }
                    Error(err) => {
                        // drop the publish stream's actual element, as if the publish had failed
                        std::mem::drop(self.messages.next());

                        // replace it with this error
                        Poll::Ready(Some(Err(err)))
                    }
                }
            }
        }

        #[derive(Debug)]
        struct ControlledPublisher {
            command: Rc<RefCell<PublishCommand>>,
            mock: MockPublisher,
        }

        impl ControlledPublisher {
            fn new() -> (Self, Rc<RefCell<PublishCommand>>) {
                let command = Rc::new(RefCell::new(PublishCommand::Pending));
                (
                    ControlledPublisher {
                        command: Rc::clone(&command),
                        mock: MockPublisher::new(),
                    },
                    command,
                )
            }
        }

        impl Publisher for ControlledPublisher {
            type MessageId = ();
            type MessageError = TestError;
            type PublishStream = ControlledPublishStream;

            fn publish<'a, I>(&self, topic: Topic, messages: I) -> Self::PublishStream
            where
                I: Iterator<Item = &'a ValidatedMessage> + ExactSizeIterator + DoubleEndedIterator,
            {
                ControlledPublishStream {
                    command: Rc::clone(&self.command),
                    mock: self.mock.clone(),
                    messages: messages
                        .cloned()
                        .map(|msg| (topic, msg))
                        .collect::<Vec<_>>()
                        .into_iter(),
                }
            }
        }

        /// The publisher should check that a batch size of zero doesn't crash and instead uses
        /// something like a max(1, size)
        #[test]
        fn batch_size_zero() {
            let batch_size = 0;
            let publisher = MockPublisher::new();
            let sink = publisher_sink(publisher, batch_size);
            pin_mut!(sink);
            let mut cx = Context::from_waker(futures_util::task::noop_waker_ref());

            assert_eq!(Poll::Ready(Ok(())), sink.as_mut().poll_ready(&mut cx));
            assert_eq!(
                Ok(()),
                sink.as_mut()
                    .start_send(("test_topic", test_validated_message("foo")))
            );
        }

        /// The publisher should start flushing when the batch size has been exceeded
        #[test]
        fn batching_batches() {
            let topic = "test_topic";
            let batch_size = 3;
            let publisher = MockPublisher::new();
            let sink = publisher_sink(publisher, batch_size);
            pin_mut!(sink);

            // sanity check
            assert!(sink.publisher.is_empty());

            let mut cx = Context::from_waker(futures_util::task::noop_waker_ref());

            // insert 3 messages to reach the buffer limit
            for &data in ["foo", "bar", "baz"].iter() {
                assert_eq!(Poll::Ready(Ok(())), sink.as_mut().poll_ready(&mut cx));
                assert_eq!(
                    Ok(()),
                    sink.as_mut()
                        .start_send((topic, test_validated_message(data)))
                );
            }

            // the flush should not have been triggered yet
            assert!(sink.publisher.is_empty());

            // the next poll should find that the buffer is full; it will trigger a flush before
            // returning `Ready` (it won't return Pending because the MockPublisher isn't async)
            assert_eq!(Poll::Ready(Ok(())), sink.as_mut().poll_ready(&mut cx));

            assert_eq!(
                vec![
                    &(topic, test_validated_message("foo")),
                    &(topic, test_validated_message("bar")),
                    &(topic, test_validated_message("baz"))
                ],
                sink.publisher.messages().iter().collect::<Vec<_>>()
            );
        }

        /// The publisher should flush buffered elements when asked to close
        #[test]
        fn close_flushes_batch() {
            let topic = "test_topic";
            let batch_size = 3;
            let publisher = MockPublisher::new();
            let sink = publisher_sink(publisher, batch_size);
            pin_mut!(sink);

            // sanity check
            assert!(sink.publisher.is_empty());

            let mut cx = Context::from_waker(futures_util::task::noop_waker_ref());

            // insert 1 messages to reach the buffer limit
            assert_eq!(Poll::Ready(Ok(())), sink.as_mut().poll_ready(&mut cx));
            assert_eq!(
                Ok(()),
                sink.as_mut()
                    .start_send((topic, test_validated_message("foo")))
            );

            // no flush has been triggered yet
            assert!(sink.publisher.is_empty());

            // closing should trigger a flush
            assert_eq!(Poll::Ready(Ok(())), sink.as_mut().poll_close(&mut cx));

            assert_eq!(
                vec![&(topic, test_validated_message("foo")),],
                sink.publisher.messages().iter().collect::<Vec<_>>()
            );
        }

        /// The publisher should flush buffered elements when asked to flush
        #[test]
        fn flush_incomplete_batch() {
            let topic = "test_topic";
            let batch_size = 3;
            let publisher = MockPublisher::new();
            let sink = publisher_sink(publisher, batch_size);
            pin_mut!(sink);

            // sanity check
            assert!(sink.publisher.is_empty());

            let mut cx = Context::from_waker(futures_util::task::noop_waker_ref());

            // insert 1 messages to reach the buffer limit
            assert_eq!(Poll::Ready(Ok(())), sink.as_mut().poll_ready(&mut cx));
            assert_eq!(
                Ok(()),
                sink.as_mut()
                    .start_send((topic, test_validated_message("foo")))
            );

            // no flush has been triggered yet
            assert!(sink.publisher.is_empty());

            // trigger a flush
            assert_eq!(Poll::Ready(Ok(())), sink.as_mut().poll_flush(&mut cx));

            assert_eq!(
                vec![&(topic, test_validated_message("foo")),],
                sink.publisher.messages().iter().collect::<Vec<_>>()
            );
        }

        /// `start_send` should panic if the buffer is full, because the user should have checked
        /// `poll_ready`
        #[test]
        #[should_panic]
        fn panic_at_buffer_full_without_ready_check() {
            let topic = "test_topic";
            let batch_size = 1;
            let publisher = MockPublisher::new();
            let sink = publisher_sink(publisher, batch_size);
            pin_mut!(sink);

            assert_eq!(
                Ok(()),
                sink.as_mut()
                    .start_send((topic, test_validated_message("foo")))
            );

            // should panic here
            let _ = sink
                .as_mut()
                .start_send((topic, test_validated_message("bar")));
        }

        /// Step through flushing a non-full batch and see that yield points are respected
        #[test]
        fn partial_flushing_check() {
            let topic = "test_topic";
            let batch_size = 3;
            let (publisher, command) = ControlledPublisher::new();
            let sink = publisher_sink(publisher, batch_size);
            pin_mut!(sink);

            let mut cx = Context::from_waker(futures_util::task::noop_waker_ref());

            // insert 2 elements into the sink before flushing

            for &data in ["foo", "bar"].iter() {
                assert_eq!(Poll::Ready(Ok(())), sink.as_mut().poll_ready(&mut cx));
                assert_eq!(
                    Ok(()),
                    sink.as_mut()
                        .start_send((topic, test_validated_message(data)))
                );
            }

            // the elements should have been inserted into the batch, but not yet published
            assert_eq!(2, sink.batch.len());
            assert!(sink.publisher.mock.is_empty());

            // set the publisher to return Pending on its next iteration
            *command.borrow_mut() = PublishCommand::Pending;
            assert_eq!(Poll::Pending, sink.as_mut().poll_flush(&mut cx));

            // the start of flushing should empty the batch. Though the publisher should have
            // yielded, so the elements are in flight and not yet published
            assert_eq!(0, sink.batch.len());
            assert!(sink.publisher.mock.is_empty());

            // stepping the publish stream once should publish the first element
            *command.borrow_mut() = PublishCommand::Next;
            assert_eq!(Poll::Pending, sink.as_mut().poll_flush(&mut cx));

            assert_eq!(
                vec![&(topic, test_validated_message("foo"))],
                sink.publisher.mock.messages().iter().collect::<Vec<_>>()
            );

            // the publisher might be pending for a while, the flushing will propogate this
            assert_eq!(Poll::Pending, sink.as_mut().poll_flush(&mut cx));
            assert_eq!(Poll::Pending, sink.as_mut().poll_flush(&mut cx));
            assert_eq!(Poll::Pending, sink.as_mut().poll_flush(&mut cx));

            // advance to the next element
            *command.borrow_mut() = PublishCommand::Next;
            assert_eq!(Poll::Pending, sink.as_mut().poll_flush(&mut cx));
            assert_eq!(
                vec![
                    &(topic, test_validated_message("foo")),
                    &(topic, test_validated_message("bar"))
                ],
                sink.publisher.mock.messages().iter().collect::<Vec<_>>()
            );

            // the publish stream isn't done because it hasn't yielded None yet
            assert_eq!(Poll::Pending, sink.as_mut().poll_flush(&mut cx));

            // one last advance to None should finish the flush
            *command.borrow_mut() = PublishCommand::Next;
            assert_eq!(Poll::Ready(Ok(())), sink.as_mut().poll_flush(&mut cx));
        }

        /// A failed message can be re-sent to the sink and eventually succeed
        #[test]
        fn flushing_error_retry() {
            let topic = "test_topic";
            let batch_size = 5;
            let (publisher, command) = ControlledPublisher::new();
            let sink = publisher_sink(publisher, batch_size);
            pin_mut!(sink);

            let mut cx = Context::from_waker(futures_util::task::noop_waker_ref());

            // insert 3 elements. the middle one will encounter an error later
            for &data in ["a", "b", "c"].iter() {
                assert_eq!(Poll::Ready(Ok(())), sink.as_mut().poll_ready(&mut cx));
                assert_eq!(
                    Ok(()),
                    sink.as_mut()
                        .start_send((topic, test_validated_message(data)))
                );
            }

            //sanity check
            assert!(sink.publisher.mock.is_empty());

            // start flushing
            *command.borrow_mut() = PublishCommand::Next;
            assert_eq!(Poll::Pending, sink.as_mut().poll_flush(&mut cx));
            assert_eq!(
                vec![&(topic, test_validated_message("a"))],
                sink.publisher.mock.messages().iter().collect::<Vec<_>>()
            );

            // encounter an error on the second element
            *command.borrow_mut() = PublishCommand::Error(TestError("boom!".into()));
            let failed_message = match sink.as_mut().poll_flush(&mut cx) {
                Poll::Ready(Err(err)) => err,
                _ => panic!("expected ready error"),
            };
            assert_eq!(
                &FailedMessage {
                    error: TestError("boom!".into()),
                    topic,
                    message: test_validated_message("b")
                },
                &failed_message
            );

            //re-submit the failed message to the sink
            assert_eq!(Poll::Ready(Ok(())), sink.as_mut().poll_ready(&mut cx));
            assert_eq!(
                Ok(()),
                sink.as_mut()
                    .start_send((failed_message.topic, failed_message.message))
            );

            // the retried element is now waiting in the sink's next batch
            assert_eq!(1, sink.batch.len());
            // the publisher is still in the same state of having the first element
            assert_eq!(
                vec![&(topic, test_validated_message("a"))],
                sink.publisher.mock.messages().iter().collect::<Vec<_>>()
            );

            //flushing can continue after an error
            assert_eq!(Poll::Pending, sink.as_mut().poll_flush(&mut cx)); // just a pending pass-through

            // push the next publish from the first batch, as if the error were transient
            *command.borrow_mut() = PublishCommand::Next;
            assert_eq!(Poll::Pending, sink.as_mut().poll_flush(&mut cx));

            // the re-submit pushed the retry to essentially the back of the line, so the flush
            // resumes from where the publish stream left off with the 3rd element
            assert_eq!(
                vec![
                    &(topic, test_validated_message("a")),
                    &(topic, test_validated_message("c"))
                ],
                sink.publisher.mock.messages().iter().collect::<Vec<_>>()
            );

            // advance the stream, but this is to the ending None so the retry won't be
            // published yet. The next batch will be pulled from however
            *command.borrow_mut() = PublishCommand::Next;
            assert_eq!(Poll::Pending, sink.as_mut().poll_flush(&mut cx));
            assert!(sink.batch.is_empty());
            assert_eq!(
                vec![
                    &(topic, test_validated_message("a")),
                    &(topic, test_validated_message("c"))
                ],
                sink.publisher.mock.messages().iter().collect::<Vec<_>>()
            );

            // finally the retry element can be published
            *command.borrow_mut() = PublishCommand::Next;
            assert_eq!(Poll::Pending, sink.as_mut().poll_flush(&mut cx));
            assert_eq!(
                vec![
                    &(topic, test_validated_message("a")),
                    &(topic, test_validated_message("c")),
                    &(topic, test_validated_message("b"))
                ],
                sink.publisher.mock.messages().iter().collect::<Vec<_>>()
            );

            // advance the stream to terminate and finish the flush
            *command.borrow_mut() = PublishCommand::Next;
            assert_eq!(Poll::Ready(Ok(())), sink.as_mut().poll_flush(&mut cx));
        }
    }
}
