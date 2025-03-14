use futures_util::{SinkExt, StreamExt};
use hedwig::{
    redis::{ClientBuilder, ClientBuilderConfig, Group, GroupName, RedisMessage, StreamName},
    validators, AcknowledgeToken, Consumer, DecodableMessage, EncodableMessage, Headers, Publisher,
};
use std::{
    error::Error as StdError,
    time::{Duration, SystemTime},
};
use structopt::StructOpt;
use tracing::info;

const TEST_MESSAGE: &str = "test_message";
const SCHEMA: &str = "test_message/1.0";

/// The input data, representing some user being created with the given name
#[derive(PartialEq, Eq, prost::Message)]
struct TestMessage {
    #[prost(uint64)]
    id: u64,
}

impl EncodableMessage for TestMessage {
    type Error = validators::ProstValidatorError;
    type Validator = validators::ProstValidator;
    fn topic(&self) -> hedwig::Topic {
        TEST_MESSAGE.into()
    }
    fn encode(&self, validator: &Self::Validator) -> Result<hedwig::ValidatedMessage, Self::Error> {
        validator.validate(
            uuid::Uuid::new_v4(),
            SystemTime::now(),
            SCHEMA,
            Headers::new(),
            self,
        )
    }
}

impl DecodableMessage for TestMessage {
    type Error = validators::ProstDecodeError<validators::prost::SchemaMismatchError>;
    type Decoder = validators::ProstDecoder<validators::prost::ExactSchemaMatcher<TestMessage>>;

    fn decode(msg: hedwig::ValidatedMessage, decoder: &Self::Decoder) -> Result<Self, Self::Error> {
        decoder.decode(msg)
    }
}

#[derive(Debug, StructOpt)]
struct Args {
    #[structopt(long, default_value = "redis://localhost:6379")]
    endpoint: String,

    #[structopt(long, default_value = "1")]
    mode: usize,

    #[structopt(long, default_value = "1")]
    count: u64,

    #[structopt(long, default_value = "0")]
    delay_ms: usize,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn StdError>> {
    tracing_subscriber::fmt::init();

    let args = Args::from_args();

    let config = ClientBuilderConfig {
        endpoint: args.endpoint,
    };

    let builder = ClientBuilder::new(config).await?;

    let input_topic_name = StreamName::from_topic(TEST_MESSAGE);
    let input_consumer_group = Group::new(GroupName::new(APP_NAME), input_topic_name.clone());

    const APP_NAME: &str = "redis-debug";

    let publisher_client = builder.build_publisher(APP_NAME).await?;
    let mut consumer_client = builder.build_consumer(APP_NAME).await?;

    match args.mode {
        1 => {
            let delay = Duration::from_millis(args.delay_ms.try_into().unwrap());
            let validator = validators::ProstValidator::new();
            let mut input_sink = Publisher::<TestMessage>::publish_sink(
                publisher_client.publisher().await,
                validator,
            );

            for id in 1..=args.count {
                let message = TestMessage { id };

                info!("Sending message {:?}", message.id);

                input_sink.feed(message).await.unwrap();
                input_sink.flush().await.unwrap();
                tokio::time::sleep(delay).await;
            }

            info!("Finished. Sent {} messages", args.count);
        }
        2 => {
            let mut read_stream = consumer_client
                .stream_subscription(input_consumer_group.clone())
                .await
                .consume::<TestMessage>(hedwig::validators::ProstDecoder::new(
                    hedwig::validators::prost::ExactSchemaMatcher::new(SCHEMA),
                ));

            let mut count = 0;

            for _ in 1..=args.count {
                match read_stream.next().await {
                    Some(Ok(msg)) => {
                        let RedisMessage { ack_token, message } = msg;
                        let _ = ack_token.ack().await;
                        info!("Received: {:?}", &message.id);

                        count += 1;
                    }
                    Some(Err(err)) => {
                        info!("{:?}", err);
                        break;
                    }
                    None => {
                        panic!();
                        // info!("Received {} messages", count);
                        // tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }

            info!("Finished. Received {} messages", count);
        }
        _ => {
            panic!();
        }
    }

    Ok(())
}
