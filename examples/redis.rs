//! An example of ingesting messages from a Redis Stream, applying a transformation, then submitting those
//! transformations to another Redis Stream.

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
    #[structopt(long)]
    endpoint: String,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn StdError>> {
    let args = Args::from_args();

    let input_topic_name = TopicName::new(USER_CREATED_TOPIC);
    let subscription_name = SubscriptionName::new("user-metadata-updaters");

    let output_topic_name = TopicName::new(USER_UPDATED_TOPIC);
    const APP_NAME: &str = "user-metadata-updater";

    Ok(())
}

