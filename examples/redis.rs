use futures_util::{SinkExt, StreamExt, TryFutureExt};
use hedwig::{
    redis::{
        ClientBuilder, ClientBuilderConfig, Group, GroupName, RedisMessage, StreamName, TopicConfig,
    },
    validators, Consumer, DecodableMessage, EncodableMessage, Headers, Publisher,
};
use std::{error::Error as StdError, time::SystemTime};
use structopt::StructOpt;
use tracing::warn;

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
struct TransformedMessage(RedisMessage<UserUpdatedMessage>);

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
    #[structopt(long)]
    endpoint: String,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn StdError>> {
    tracing_subscriber::fmt::init();

    let args = Args::from_args();

    println!("Building PubSub clients");

    let config = ClientBuilderConfig {
        endpoint: args.endpoint,
    };

    let builder = ClientBuilder::new(config).await?;

    let input_topic_name = StreamName::from_topic(USER_CREATED_TOPIC);
    let input_consumer_group = Group::new(GroupName::new(APP_NAME), input_topic_name.clone());

    let output_topic_name = StreamName::from_topic(USER_UPDATED_TOPIC);
    const APP_NAME: &str = "user-metadata-updater";

    let mut publisher_client = builder.build_publisher(APP_NAME).await?;
    let mut consumer_client = builder.build_consumer(APP_NAME).await?;

    for topic_name in [&input_topic_name, &output_topic_name] {
        println!("Creating topic {:?}", topic_name);

        publisher_client
            .create_topic(TopicConfig {
                name: topic_name.clone(),
            })
            .await?;
    }

    let _ = consumer_client
        .create_subscription(&input_consumer_group)
        .await
        .inspect_err(|err| {
            warn!(err = err.to_string(), "cannot create consumer group");
        });

    println!(
        "Synthesizing input messages for topic {:?}",
        &input_topic_name
    );

    {
        let validator = validators::ProstValidator::new();
        let mut input_sink = Publisher::<UserCreatedMessage>::publish_sink(
            publisher_client.publisher().await,
            validator,
        );

        for i in 1..=10 {
            let message = UserCreatedMessage {
                name: format!("Example Name #{}", i),
            };

            // TODO googlepubsub error can be used with ? but this is not. Why?
            input_sink.feed(message).await.unwrap();
        }

        // TODO googlepubsub error can be used with ? but this is not. Why?
        input_sink.flush().await.unwrap();
    }

    println!("Ingesting input messages, applying transformations, and publishing to destination");

    let mut read_stream = consumer_client
        .stream_subscription(input_consumer_group.clone())
        .await
        .consume::<UserCreatedMessage>(hedwig::validators::ProstDecoder::new(
            hedwig::validators::prost::ExactSchemaMatcher::new("user.created/1.0"),
        ));

    let mut output_sink = Publisher::<TransformedMessage, _>::publish_sink_with_responses(
        publisher_client.publisher().await,
        validators::ProstValidator::new(),
        futures_util::sink::unfold((), |_, message: TransformedMessage| async move {
            // if the output is successfully sent, ack the input to mark it as processed
            message.0.ack().await.map(|_success| ())
        }),
    );

    for i in 1..=10 {
        let RedisMessage { ack_token, message } = read_stream
            .next()
            .await
            .expect("stream should have 10 elements")?;

        if message.name != format!("Example Name #{}", i) {
            println!("Unexpected message received: {:?}", &message.name);
        } else {
            println!("Received: {:?}", &message.name);
        }

        let transformed = TransformedMessage(RedisMessage {
            ack_token,
            message: UserUpdatedMessage {
                name: message.name,
                id: random_id(),
                metadata: "some metadata".into(),
            },
        });

        let _ = output_sink
            .feed(transformed)
            .inspect_err(|publish_error| {
                println!("Error: {:?}", publish_error);
            })
            .or_else(|publish_error| async move {
                // if publishing fails, nack the failed messages to allow later retries
                Err(match publish_error {
                    hedwig::redis::PublishError::Publish { cause: _, messages } => {
                        for failed_transform in messages {
                            failed_transform.0.nack().await?;
                        }
                        Box::<dyn StdError>::from("Cannot publish message")
                    }
                    err => Box::<dyn StdError>::from(err),
                })
            })
            .await;
    }

    // TODO SW-19526 googlepubsub example differs here
    if (output_sink.flush().await).is_err() {
        panic!()
    }

    println!("All messages matched and published successfully!");

    println!("Deleting subscription {:?}", &input_consumer_group);

    consumer_client
        .delete_subscription(input_consumer_group)
        .await?;

    for topic_name in [input_topic_name, output_topic_name] {
        println!("Deleting topic {:?}", &topic_name);

        publisher_client.delete_topic(topic_name).await?;
    }

    println!("Done");

    Ok(())
}

fn random_id() -> i64 {
    4 // chosen by fair dice roll.
      // guaranteed to be random.
}
