use futures_util::{SinkExt, StreamExt};
use hedwig::{
    googlepubsub::{
        AuthFlow, ClientBuilder, ClientBuilderConfig, PubSubConfig, ServiceAccountAuth,
        StreamSubscriptionConfig, SubscriptionConfig, SubscriptionName, TopicConfig, TopicName,
    },
    validators, Consumer, DecodableMessage, EncodableMessage, Headers, Publisher,
};
use std::time::SystemTime;
use structopt::StructOpt;

#[derive(Clone, PartialEq, Eq, prost::Message)]
struct UserCreatedMessage {
    #[prost(string, tag = "1")]
    payload: String,
}

impl<'a> EncodableMessage for UserCreatedMessage {
    type Error = validators::ProstValidatorError;
    type Validator = validators::ProstValidator;
    fn topic(&self) -> hedwig::Topic {
        "user.created".into()
    }
    fn encode(self, validator: &Self::Validator) -> Result<hedwig::ValidatedMessage, Self::Error> {
        Ok(validator.validate(
            uuid::Uuid::new_v4(),
            SystemTime::now(),
            "user.created/1.0",
            Headers::new(),
            &self,
        )?)
    }
}

impl DecodableMessage for UserCreatedMessage {
    type Error = validators::ProstDecodeError<validators::prost::SchemaMismatchError>;
    type Decoder =
        validators::ProstDecoder<validators::prost::ExactSchemaMatcher<UserCreatedMessage>>;

    fn decode(msg: hedwig::ValidatedMessage, decoder: &Self::Decoder) -> Result<Self, Self::Error> {
        decoder.decode(msg)
    }
}

#[derive(Debug, StructOpt)]
struct Args {
    /// The name of the pubsub project
    #[structopt(long)]
    project_name: String,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error + 'static>> {
    let args = Args::from_args();

    println!("Building PubSub clients");

    let builder = ClientBuilder::new(
        ClientBuilderConfig::new().auth_flow(AuthFlow::ServiceAccount(ServiceAccountAuth::EnvVar)),
        PubSubConfig::default(),
    )
    .await?;

    let topic_name = TopicName::new("pubsub_example_topic");
    let subscription_name = SubscriptionName::new("pubsub_example_subscription");

    let mut publisher_client = builder
        .build_publisher(&args.project_name, "myapp_publisher")
        .await?;
    let mut consumer_client = builder
        .build_consumer(&args.project_name, "myapp_consumer")
        .await?;

    println!("Creating topic {:?}", &topic_name);

    publisher_client
        .create_topic(TopicConfig {
            name: topic_name.clone(),
            ..TopicConfig::default()
        })
        .await?;

    println!("Creating subscription {:?}", &subscription_name);

    consumer_client
        .create_subscription(SubscriptionConfig {
            topic: topic_name.clone(),
            name: subscription_name.clone(),
            ..SubscriptionConfig::default()
        })
        .await?;

    println!("Publishing message to topic");

    let validator = hedwig::validators::ProstValidator::new();
    let mut publisher = publisher_client.publisher().publish_sink(validator);

    for i in 1..=10 {
        let message = UserCreatedMessage {
            payload: format!("this is message #{}", i),
        };

        publisher.send(message).await?;
    }

    println!("Reading back published message");

    let mut read_stream = consumer_client
        .stream_subscription(
            subscription_name.clone(),
            StreamSubscriptionConfig::default(),
        )
        .consume::<UserCreatedMessage>(hedwig::validators::ProstDecoder::new(
            hedwig::validators::prost::ExactSchemaMatcher::new("user.created/1.0"),
        ));

    for i in 1..=10 {
        let message = read_stream
            .next()
            .await
            .ok_or("unexpected end of stream")??
            .ack()
            .await?;

        assert_eq!(
            message,
            UserCreatedMessage {
                payload: format!("this is message #{}", i)
            }
        );
    }

    println!("All messages matched!");

    println!("Deleting subscription {:?}", &subscription_name);

    consumer_client
        .delete_subscription(subscription_name)
        .await?;

    println!("Deleting topic {:?}", &topic_name);

    publisher_client.delete_topic(topic_name).await?;

    println!("Done");

    Ok(())
}
