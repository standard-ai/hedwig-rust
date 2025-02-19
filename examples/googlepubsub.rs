//! An example of ingesting messages from a PubSub subscription, applying a
//! transformation, then submitting those transformations to another PubSub topic.

use futures_util::{SinkExt, StreamExt, TryFutureExt};
use hedwig::{
    googlepubsub::{
        AuthFlow, ClientBuilder, ClientBuilderConfig, PubSubConfig, PubSubMessage, PublishError,
        ServiceAccountAuth, StreamSubscriptionConfig, SubscriptionConfig, SubscriptionName,
        TopicConfig, TopicName,
    },
    validators, Consumer, DecodableMessage, EncodableMessage, Headers, Publisher,
};
use std::{error::Error as StdError, time::SystemTime};
use structopt::StructOpt;

const USER_CREATED_TOPIC: &str = "user.created";
const USER_UPDATED_TOPIC: &str = "user.updated";

/// The input data, representing some user being created with the given name
#[derive(PartialEq, Eq, prost::Message)]
struct UserCreatedMessage {
    #[prost(string, tag = "1")]
    name: String,
}

impl EncodableMessage for UserCreatedMessage {
    type Error = validators::ProstValidatorError;
    type Validator = validators::ProstValidator;
    fn topic(&self) -> hedwig::Topic {
        USER_CREATED_TOPIC.into()
    }
    fn encode(&self, validator: &Self::Validator) -> Result<hedwig::ValidatedMessage, Self::Error> {
        validator.validate(
            uuid::Uuid::new_v4(),
            SystemTime::now(),
            "user.created/1.0",
            Headers::new(),
            self,
        )
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

/// The output data, where the given user has now been assigned an ID and some metadata
#[derive(PartialEq, Eq, prost::Message)]
struct UserUpdatedMessage {
    #[prost(string, tag = "1")]
    name: String,

    #[prost(int64, tag = "2")]
    id: i64,

    #[prost(string, tag = "3")]
    metadata: String,
}

/// The output message will carry an ack token from the input message, to ack when the output is
/// successfully published, or nack on failure
#[derive(Debug)]
struct TransformedMessage(PubSubMessage<UserUpdatedMessage>);

impl EncodableMessage for TransformedMessage {
    type Error = validators::ProstValidatorError;
    type Validator = validators::ProstValidator;

    fn topic(&self) -> hedwig::Topic {
        USER_UPDATED_TOPIC.into()
    }

    fn encode(&self, validator: &Self::Validator) -> Result<hedwig::ValidatedMessage, Self::Error> {
        validator.validate(
            uuid::Uuid::new_v4(),
            SystemTime::now(),
            "user.updated/1.0",
            Headers::new(),
            &self.0.message,
        )
    }
}

#[derive(Debug, StructOpt)]
struct Args {
    /// The name of the pubsub project
    #[structopt(long)]
    project_name: String,

    /// Load credentials from an authorized user secret, such as the one created when running `gcloud auth
    /// application-default login`
    #[structopt(long)]
    user_account_credentials: Option<std::path::PathBuf>,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn StdError>> {
    tracing_subscriber::fmt::init();

    let args = Args::from_args();

    println!("Building PubSub clients");

    let auth_flow = if let Some(user) = args.user_account_credentials {
        AuthFlow::UserAccount(user)
    } else {
        AuthFlow::ServiceAccount(ServiceAccountAuth::EnvVar)
    };

    let builder = ClientBuilder::new(
        ClientBuilderConfig::new().auth_flow(auth_flow),
        PubSubConfig::default(),
    )
    .await?;

    let input_topic_name = TopicName::new(USER_CREATED_TOPIC);
    let subscription_name = SubscriptionName::new("user-metadata-updaters");

    let output_topic_name = TopicName::new(USER_UPDATED_TOPIC);
    const APP_NAME: &str = "user-metadata-updater";

    let mut publisher_client = builder
        .build_publisher(&args.project_name, APP_NAME)
        .await?;
    let mut consumer_client = builder.build_consumer(&args.project_name, APP_NAME).await?;

    for topic_name in [&input_topic_name, &output_topic_name] {
        println!("Creating topic {:?}", topic_name);

        let _ = publisher_client
            .create_topic(TopicConfig {
                name: topic_name.clone(),
                ..TopicConfig::default()
            })
            .await;
    }

    println!("Creating subscription {:?}", &subscription_name);

    let _ = consumer_client
        .create_subscription(SubscriptionConfig {
            topic: input_topic_name.clone(),
            name: subscription_name.clone(),
            ..SubscriptionConfig::default()
        })
        .await;

    println!(
        "Synthesizing input messages for topic {:?}",
        &input_topic_name
    );

    {
        let validator = validators::ProstValidator::new();
        let mut input_sink =
            Publisher::<UserCreatedMessage>::publish_sink(publisher_client.publisher(), validator);

        for i in 1..=10 {
            let message = UserCreatedMessage {
                name: format!("Example Name #{}", i),
            };

            input_sink.feed(message).await?;
        }
        input_sink.flush().await?;
    }

    println!("Ingesting input messages, applying transformations, and publishing to destination");

    let mut read_stream = consumer_client
        .stream_subscription(
            subscription_name.clone(),
            StreamSubscriptionConfig::default(),
        )
        .consume::<UserCreatedMessage>(hedwig::validators::ProstDecoder::new(
            hedwig::validators::prost::ExactSchemaMatcher::new("user.created/1.0"),
        ));

    let mut output_sink = Publisher::<TransformedMessage, _>::publish_sink_with_responses(
        publisher_client.publisher(),
        validators::ProstValidator::new(),
        futures_util::sink::unfold((), |_, message: TransformedMessage| async move {
            // if the output is successfully sent, ack the input to mark it as processed
            message.0.ack().await.map(|_success| ())
        }),
    );

    for i in 1..=10 {
        let PubSubMessage { ack_token, message } = read_stream
            .next()
            .await
            .expect("stream should have 10 elements")?;

        if message.name != format!("Example Name #{}", i) {
            println!("Unexpected message received: {:?}", &message.name);
        } else {
            println!("Received: {:?}", &message.name);
        }

        let transformed = TransformedMessage(PubSubMessage {
            ack_token,
            message: UserUpdatedMessage {
                name: message.name,
                id: random_id(),
                metadata: "some metadata".into(),
            },
        });

        output_sink
            .feed(transformed)
            .or_else(|publish_error| async move {
                // if publishing fails, nack the failed messages to allow later retries
                Err(match publish_error {
                    PublishError::Publish { cause, messages } => {
                        for failed_transform in messages {
                            failed_transform.0.nack().await?;
                        }
                        Box::<dyn StdError>::from(cause)
                    }
                    err => Box::<dyn StdError>::from(err),
                })
            })
            .await?
    }
    output_sink.flush().await?;

    println!("All messages matched and published successfully!");

    println!("Deleting subscription {:?}", &subscription_name);

    let _ = consumer_client.delete_subscription(subscription_name).await;

    for topic_name in [input_topic_name, output_topic_name] {
        println!("Deleting topic {:?}", &topic_name);

        let _ = publisher_client.delete_topic(topic_name).await;
    }

    println!("Done");

    Ok(())
}

fn random_id() -> i64 {
    4 // chosen by fair dice roll.
      // guaranteed to be random.
}
