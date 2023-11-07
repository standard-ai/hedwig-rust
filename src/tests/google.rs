#![cfg(all(feature = "google", feature = "protobuf"))]

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;
use crate::{
    googlepubsub::{
        retry_policy::{RetryOperation, RetryPolicy},
        AuthFlow, ClientBuilder, ClientBuilderConfig, PubSubConfig, PubSubError, PublishError,
        StreamSubscriptionConfig, SubscriptionConfig, SubscriptionName, TopicConfig, TopicName,
    },
    message,
    validators::{
        prost::{ExactSchemaMatcher, SchemaMismatchError},
        ProstDecodeError, ProstDecoder, ProstValidator, ProstValidatorError,
    },
    Consumer, DecodableMessage, EncodableMessage, Headers, Publisher, Topic, ValidatedMessage,
};
use futures_util::{pin_mut, SinkExt, StreamExt, TryFutureExt, TryStreamExt};
use std::{
    sync::mpsc,
    task::{Context, Poll},
};
use ya_gcp::pubsub::emulator::EmulatorClient;

const SCHEMA: &str = "test-schema";
const TOPIC: &str = "test-topic";

#[derive(Clone, PartialEq, Eq, prost::Message)]
struct TestMessage {
    #[prost(string, tag = "1")]
    payload: String,
}

impl EncodableMessage for TestMessage {
    type Error = ProstValidatorError;
    type Validator = ProstValidator;

    fn topic(&self) -> Topic {
        TOPIC.into()
    }

    fn encode(&self, validator: &Self::Validator) -> Result<ValidatedMessage, Self::Error> {
        validator.validate(
            uuid::Uuid::nil(),
            std::time::SystemTime::UNIX_EPOCH,
            SCHEMA,
            Headers::from([(String::from("key"), String::from("value"))]),
            self,
        )
    }
}

impl DecodableMessage for TestMessage {
    type Decoder = ProstDecoder<ExactSchemaMatcher<TestMessage>>;
    type Error = ProstDecodeError<SchemaMismatchError>;

    fn decode(msg: ValidatedMessage, validator: &Self::Decoder) -> Result<Self, Self::Error> {
        validator.decode(msg)
    }
}

#[test]
fn decode_with_headers() -> Result<(), BoxError> {
    let orig_message = TestMessage {
        payload: "foobar".into(),
    };

    let encoded = orig_message.encode(&ProstValidator::new())?;

    let decoded = message::ValidatedMessage::<TestMessage>::decode(
        encoded,
        &ProstDecoder::new(ExactSchemaMatcher::new(SCHEMA)),
    )?;

    let headers = Headers::from([(String::from("key"), String::from("value"))]);

    assert_eq!(decoded.headers(), &headers);

    Ok(())
}

#[tokio::test]
#[ignore = "pubsub emulator is finicky, run this test manually"]
async fn roundtrip_protobuf() -> Result<(), BoxError> {
    let project_name = "test-project";
    let topic_name = TopicName::new(TOPIC);
    let subscription_name = SubscriptionName::new("test-subscription");

    let emulator = EmulatorClient::with_project(project_name).await?;

    let client_builder = ClientBuilder::new(
        ClientBuilderConfig::new().auth_flow(AuthFlow::NoAuth),
        PubSubConfig::new().endpoint(emulator.endpoint()),
    )
    .await?;

    let mut publisher_client = client_builder
        .build_publisher(project_name, "test_publisher")
        .await?;

    publisher_client
        .create_topic(TopicConfig {
            name: topic_name.clone(),
            ..TopicConfig::default()
        })
        .await?;

    let mut consumer_client = client_builder
        .build_consumer(project_name, "test_queue")
        .await?;

    consumer_client
        .create_subscription(SubscriptionConfig {
            name: subscription_name.clone(),
            topic: topic_name.clone(),
            ..SubscriptionConfig::default()
        })
        .await?;

    let mut publisher =
        Publisher::<TestMessage>::publish_sink(publisher_client.publisher(), ProstValidator::new());

    publisher
        .send(TestMessage {
            payload: "foobar".into(),
        })
        .await?;

    let consumer = consumer_client
        .stream_subscription(subscription_name, StreamSubscriptionConfig::default())
        .consume::<TestMessage>(ProstDecoder::new(ExactSchemaMatcher::new(SCHEMA)));

    pin_mut!(consumer);

    assert_eq!(
        TestMessage {
            payload: "foobar".into()
        },
        Option::unwrap(consumer.next().await)?.ack().await?
    );
    Ok(())
}

/// Test that the publisher-side response sink receives elements when the publisher publishes
#[tokio::test]
#[ignore = "pubsub emulator is finicky, run this test manually"]
async fn response_sink_responses() -> Result<(), BoxError> {
    let project_name = "test-project";
    let topic_name = TopicName::new(TOPIC);
    let subscription_name = SubscriptionName::new("test-subscription");

    let emulator = EmulatorClient::with_project(project_name).await?;

    let client_builder = ClientBuilder::new(
        ClientBuilderConfig::new().auth_flow(AuthFlow::NoAuth),
        PubSubConfig::new().endpoint(emulator.endpoint()),
    )
    .await?;

    let mut publisher_client = client_builder
        .build_publisher(project_name, "test_publisher")
        .await?;

    publisher_client
        .create_topic(TopicConfig {
            name: topic_name.clone(),
            ..TopicConfig::default()
        })
        .await?;

    let mut consumer_client = client_builder
        .build_consumer(project_name, "test_queue")
        .await?;

    consumer_client
        .create_subscription(SubscriptionConfig {
            name: subscription_name.clone(),
            topic: topic_name.clone(),
            ..SubscriptionConfig::default()
        })
        .await?;

    let (response_sink, mut responses) = futures_channel::mpsc::unbounded();
    let mut cx = Context::from_waker(futures_util::task::noop_waker_ref());

    let mut publisher = Publisher::<TestMessage, _>::publish_sink_with_responses(
        publisher_client.publisher(),
        ProstValidator::new(),
        response_sink,
    );

    let consumer = consumer_client
        .stream_subscription(subscription_name, StreamSubscriptionConfig::default())
        .consume::<TestMessage>(ProstDecoder::new(ExactSchemaMatcher::new(SCHEMA)));

    pin_mut!(consumer);

    {
        let message = TestMessage {
            payload: "foobar".into(),
        };

        publisher.feed(message.clone()).await?;

        // the response sink should not be populated until a flush
        assert_eq!(Poll::Pending, responses.poll_next_unpin(&mut cx));
        publisher.flush().await?;
        assert_eq!(
            Poll::Ready(Some(message.clone())),
            responses.poll_next_unpin(&mut cx)
        );

        assert_eq!(message, Option::unwrap(consumer.next().await)?.ack().await?);
    }

    {
        let message1 = TestMessage {
            payload: "one".into(),
        };
        let message2 = TestMessage {
            payload: "two".into(),
        };
        let message3 = TestMessage {
            payload: "three".into(),
        };
        // create a message that will exceed the message limits (~10MB) and therefore error
        let invalid_message4 = TestMessage {
            payload: "4".repeat(10 * 1_000_000 + 1),
        };
        let message5 = TestMessage {
            payload: "five".into(),
        };

        publisher.feed(message1.clone()).await?;
        publisher.feed(message2.clone()).await?;
        publisher.feed(message3.clone()).await?;

        // buffering the invalid message (via feed) actually works, its validity is checked later
        // when submitted to the underlying sink with the next poll_ready
        publisher.feed(invalid_message4.clone()).await?;
        match publisher.poll_ready_unpin(&mut cx) {
            Poll::Ready(Err(PublishError::Publish { cause, messages })) => {
                assert_eq!(vec![invalid_message4], messages);
                assert_eq!(tonic::Code::InvalidArgument, cause.code());
            }
            other => panic!("expected invalid arg error, was {:?}", other),
        }

        publisher.feed(message5.clone()).await?;

        // no responses are sent yet
        assert_eq!(Poll::Pending, responses.poll_next_unpin(&mut cx));

        // the flush can still happen despite the error and the non-error values should come through
        publisher.flush().await?;
        assert_eq!(
            vec![
                message1.clone(),
                message2.clone(),
                message3.clone(),
                message5.clone()
            ],
            responses.by_ref().take(4).collect::<Vec<_>>().await
        );

        assert_eq!(
            vec![
                message1.clone(),
                message2.clone(),
                message3.clone(),
                message5.clone()
            ],
            consumer
                .by_ref()
                .take(4)
                .map_err(BoxError::from)
                .and_then(|msg| msg.ack().map_err(BoxError::from))
                .try_collect::<Vec<_>>()
                .await?
        );
    }

    {
        let message6 = TestMessage {
            payload: "six".into(),
        };
        let message7 = TestMessage {
            payload: "seven".into(),
        };
        // create a message that will *not* exceed the message limits, but will exceed the total
        // request limits even when it's the only message in a request. This induces an error later
        // in the process, at the time of flush instead of insertion
        let invalid_message8 = TestMessage {
            payload: "8".repeat(10 * 1_000_000 - 6),
        };
        let message9 = TestMessage {
            payload: "nine".into(),
        };

        publisher.feed(message6.clone()).await?;
        publisher.feed(message7.clone()).await?;

        publisher.feed(invalid_message8.clone()).await?;
        // the error doesn't happen here because the invalid message was only just submitted to the
        // sub-sink by this ready check. The buffer will first note that it's over capacity, and
        // induce a flush.
        assert!(matches!(
            publisher.poll_ready_unpin(&mut cx),
            Poll::Ready(Ok(()))
        ));
        // to actually poll that flush, we need a new element in the hedwig buffer to forward
        // readiness checks to the pubsub sink (we're avoiding a manual `flush` call to test the
        // path where flushes happen unprompted)
        publisher.start_send_unpin(message9.clone())?;

        // now readiness checking will drive the flush, and eventually find the invalid message and
        // return an error
        match futures_util::future::poll_fn(|cx| publisher.poll_ready_unpin(cx)).await {
            Err(PublishError::Publish { cause, messages }) => {
                assert_eq!(vec![invalid_message8], messages);
                assert_eq!(tonic::Code::InvalidArgument, cause.code());
            }
            other => panic!("expected invalid arg error, was {:?}", other),
        }

        // flushing did allow two messages through before the error
        assert_eq!(
            vec![message6.clone(), message7.clone()],
            responses.by_ref().take(2).collect::<Vec<_>>().await
        );

        // then a manual flush can send the last message submitted after the invalid message
        publisher.flush().await?;
        assert_eq!(
            vec![message9.clone()],
            responses.by_ref().take(1).collect::<Vec<_>>().await
        );

        // all the sent messages eventually arrive to the consumer
        assert_eq!(
            vec![message6.clone(), message7.clone(), message9.clone()],
            consumer
                .by_ref()
                .take(3)
                .map_err(BoxError::from)
                .and_then(|msg| msg.ack().map_err(BoxError::from))
                .try_collect::<Vec<_>>()
                .await?
        );
    }
    Ok(())
}

/// Check to see that the retry policy will translate from api messages to user messages
#[tokio::test]
#[ignore = "pubsub emulator is finicky, run this test manually"]
async fn retry_message_translate() -> Result<(), BoxError> {
    let project_name = "roundtrip-test-project";
    let topic_name = TopicName::new(TOPIC);

    let emulator = EmulatorClient::with_project(project_name).await?;

    let client_builder = ClientBuilder::new(
        ClientBuilderConfig::new().auth_flow(AuthFlow::NoAuth),
        PubSubConfig::new().endpoint(emulator.endpoint()),
    )
    .await?;

    let mut publisher_client = client_builder
        .build_publisher(project_name, "roundtrip_test_publisher")
        .await?;

    publisher_client
        .create_topic(TopicConfig {
            name: topic_name.clone(),
            ..TopicConfig::default()
        })
        .await?;

    // Create a retry policy which will send the failure values to a channel (for manual
    // inspection) then fail the operation without retrying
    #[derive(Clone)]
    struct TestRetryPolicy {
        sender: mpsc::Sender<Vec<TestMessage>>,
    }

    struct TestRetryOperation {
        sender: mpsc::Sender<Vec<TestMessage>>,
    }

    impl RetryPolicy<[TestMessage], PubSubError> for TestRetryPolicy {
        type RetryOp = TestRetryOperation;

        fn new_operation(&mut self) -> Self::RetryOp {
            TestRetryOperation {
                sender: self.sender.clone(),
            }
        }
    }

    impl RetryOperation<[TestMessage], PubSubError> for TestRetryOperation {
        type Sleep = futures_util::future::Ready<()>;

        fn check_retry(
            &mut self,
            failed_value: &[TestMessage],
            _error: &PubSubError,
        ) -> Option<Self::Sleep> {
            self.sender
                .send(failed_value.to_owned())
                .expect("receiver should not be dropped while senders in use");
            None
        }
    }

    // construct messages such that the first two will buffer and the third will force a flush of
    // the first two. The request limit is 10MB, so 2+2MB start the buffer and an additional 8MB
    // will trigger a flush
    let message1 = TestMessage {
        payload: "1".repeat(2 * 1_000_000),
    };
    let message2 = TestMessage {
        payload: "2".repeat(2 * 1_000_000),
    };
    let message3 = TestMessage {
        payload: "3".repeat(8 * 1_000_000),
    };
    let message4 = TestMessage {
        payload: "4".into(),
    };

    let (retry_tx, retry_rx) = mpsc::channel();
    let mut publisher = Publisher::<TestMessage>::publish_sink(
        publisher_client
            .publisher()
            .with_retry_policy(TestRetryPolicy { sender: retry_tx }),
        ProstValidator::new(),
    );

    publisher.feed(message1.clone()).await?;
    publisher.feed(message2.clone()).await?;
    publisher.feed(message3.clone()).await?;
    publisher.feed(message4.clone()).await?;

    // flushing (and thus errors/retries) should not have been triggered yet
    assert_eq!(Err(mpsc::TryRecvError::Empty), retry_rx.try_recv());

    // drop the emulator to kill the process and trigger errors on publishing
    std::mem::drop(emulator);

    // flushing still hasn't happened
    assert_eq!(Err(mpsc::TryRecvError::Empty), retry_rx.try_recv());

    // check readiness to trigger the capacity flush (less than a full flush though, only enough to
    // make room for a new request)
    match futures_util::future::poll_fn(|cx| publisher.poll_ready_unpin(cx)).await {
        Err(PublishError::Publish { cause: _, messages }) => {
            assert_eq!(vec![message1.clone(), message2.clone()], messages);
        }
        other => panic!("expected publish error, was {:?}", other),
    }

    //now the retry attempts of the first flush should be visible
    assert_eq!(Ok(vec![message1, message2]), retry_rx.try_recv());
    // nothing else has attempted flushing though
    assert_eq!(Err(mpsc::TryRecvError::Empty), retry_rx.try_recv());

    // flush the rest
    match publisher.flush().await {
        Err(PublishError::Publish { cause: _, messages }) => {
            assert_eq!(vec![message3.clone(), message4.clone()], messages);
        }
        other => panic!("expected publish error, was {:?}", other),
    }

    // witness the retries are of everything left
    assert_eq!(Ok(vec![message3, message4]), retry_rx.try_recv());

    Ok(())
}
