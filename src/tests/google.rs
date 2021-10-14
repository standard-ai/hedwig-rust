#![cfg(feature = "google")]

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;
use crate::{
    googlepubsub::{
        AuthFlow, ClientBuilder, ClientBuilderConfig, PubSubConfig, StreamSubscriptionConfig,
        SubscriptionConfig, SubscriptionName, TopicConfig, TopicName,
    },
    validators::{
        prost::{ExactSchemaMatcher, SchemaMismatchError},
        ProstDecodeError, ProstDecoder, ProstValidator, ProstValidatorError,
    },
    Consumer, DecodableMessage, EncodableMessage, Headers, Publisher, Topic, ValidatedMessage,
};
use futures_util::{pin_mut, SinkExt, StreamExt};
use ya_gcp::pubsub::emulator::EmulatorClient;

const SCHEMA: &str = "roundtrip-test-schema";
const TOPIC: &str = "roundtrip-test-topic";

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

    fn encode(self, validator: &Self::Validator) -> Result<ValidatedMessage, Self::Error> {
        validator.validate(
            uuid::Uuid::nil(),
            std::time::SystemTime::UNIX_EPOCH,
            SCHEMA,
            Headers::default(),
            &self,
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

#[tokio::test]
#[ignore = "pubsub emulator is finicky, run this test manually"]
async fn roundtrip_protobuf() -> Result<(), BoxError> {
    let project_name = "roundtrip-test-project";
    let topic_name = TopicName::new(TOPIC);
    let subscription_name = SubscriptionName::new("roundtrip-test-subscription");

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

    let mut consumer_client = client_builder
        .build_consumer(project_name, "roundrip_test_queue")
        .await?;

    consumer_client
        .create_subscription(SubscriptionConfig {
            name: subscription_name.clone(),
            topic: topic_name.clone(),
            ..SubscriptionConfig::default()
        })
        .await?;

    let mut publisher = publisher_client
        .publisher()
        .publish_sink(ProstValidator::new());

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
