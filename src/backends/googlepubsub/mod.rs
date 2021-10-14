//! Adapters for using GCP's PubSub as a message service for hedwig

#![macro_use]

use std::fmt::Display;

pub use ya_gcp::{
    grpc::StatusCodeSet,
    pubsub::{
        AcknowledgeError, BuildError, Error as PubSubError, MakeConnection, ModifyAcknowledgeError,
        PubSubConfig, SinkError, StreamSubscriptionConfig, Uri, DEFAULT_RETRY_CODES,
    },
    retry_policy, AuthFlow, ClientBuilderConfig, Connect, CreateBuilderError, DefaultConnector,
    ServiceAccountAuth,
};

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Create a new struct with the same fields as another struct, with the annotated exceptions
///
/// This is used to create a narrowed-down API type, with irrelevant fields removed and other fields
/// replaced with richer types.
macro_rules! match_fields {
    (
        $target:path =>

        $(#[$struct_attr:meta])*
        pub struct $struct_name:ident {
            $(
                $(#[$field_attr:meta])*
                pub $field_name:ident : $field_type:ty,
            )*$(,)?

            // fields which exist in the target but not in the struct.
            // used to ensure names are listed exhaustively
            @except:
            $(
                $target_except_field:ident,
            )*$(,)?
        }
    ) => {
        $(#[$struct_attr])*
        // nested cfg_attr prevents older compilers from parsing the new doc = EXPR syntax
        #[cfg_attr(docsrs, cfg_attr(docsrs,
            doc = "", // newline
            doc = concat!("This is a more ergonomic wrapper over [`", stringify!($target), "`]")
        ))]
        #[cfg_attr(not(docsrs), allow(missing_docs))]
        pub struct $struct_name {
            $(
                #[cfg_attr(docsrs, cfg_attr(docsrs, doc = concat!(
                    "See [`", stringify!($field_name), "`]",
                    "(", stringify!($target), "::", stringify!($field_name), ")"
                )))]
                $(#[$field_attr])*
                pub $field_name : $field_type,
            )*
        }

        impl $struct_name {
            const _MATCH_CHECK: () = {
                match None {
                    Some($target {
                        $(
                            $field_name: _,
                        )*
                        $(
                            $target_except_field: _,
                        )*
                    }) => {},
                    None => {}
                };
                ()
            };
        }
    }
}

mod consumer;
mod publisher;

pub use consumer::*;
pub use publisher::*;

/// A PubSub topic name.
///
/// This will be used to internally construct the expected
/// `projects/{project}/topics/hedwig-{topic}` format for API calls
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TopicName(String);

impl TopicName {
    /// Create a new `TopicName`
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    /// Construct a full project and topic name with this name
    fn into_project_topic_name(
        self,
        project_name: impl Display,
    ) -> ya_gcp::pubsub::ProjectTopicName {
        ya_gcp::pubsub::ProjectTopicName::new(
            project_name,
            std::format_args!("hedwig-{topic}", topic = self.0),
        )
    }
}

/// A builder used to create [`ConsumerClient`] and [`PublisherClient`] instances
///
/// Note that the builder is not consumed when creating clients, and many clients can be built
/// using the same builder. This may allow some resource re-use across the clients
pub struct ClientBuilder<C = DefaultConnector> {
    inner: ya_gcp::ClientBuilder<C>,
    pubsub_config: PubSubConfig,
}

impl ClientBuilder {
    /// Create a new client builder using the default HTTPS connector based on the crate's
    /// enabled features
    pub async fn new(
        config: ClientBuilderConfig,
        pubsub_config: PubSubConfig,
    ) -> Result<Self, CreateBuilderError> {
        Ok(ClientBuilder {
            inner: ya_gcp::ClientBuilder::new(config).await?,
            pubsub_config,
        })
    }
}

impl<C> ClientBuilder<C>
where
    C: Connect + Clone + Send + Sync + 'static,
{
    /// Create a new client builder with the given connector.
    pub async fn with_connector(
        config: ClientBuilderConfig,
        pubsub_config: PubSubConfig,
        connector: C,
    ) -> Result<Self, CreateBuilderError> {
        Ok(ClientBuilder {
            inner: ya_gcp::ClientBuilder::with_connector(config, connector).await?,
            pubsub_config,
        })
    }
}

impl<C> ClientBuilder<C>
where
    C: MakeConnection<Uri> + Connect + Clone + Send + Sync + 'static,
    C::Connection: Unpin + Send + 'static,
    C::Future: Send + 'static,
    BoxError: From<C::Error>,
{
    /// Create a new [`ConsumerClient`] for consuming messages from PubSub subscriptions within the
    /// given project, identified by the given queue name.
    pub async fn build_consumer(
        &self,
        project: impl Into<String>,
        queue: impl Into<String>,
    ) -> Result<ConsumerClient<C>, BuildError> {
        Ok(ConsumerClient::new(
            self.inner
                .build_pubsub_subscriber(self.pubsub_config.clone())
                .await?,
            project.into(),
            queue.into(),
        ))
    }

    /// Create a new [`PublisherClient`] for publishing messages to PubSub topics within the given
    /// project.
    ///
    /// Each published message will have an attribute labelling the publisher with the given
    /// identifier.
    pub async fn build_publisher(
        &self,
        project: impl Into<String>,
        publisher_id: impl Into<String>,
    ) -> Result<PublisherClient<C>, BuildError> {
        Ok(PublisherClient::new(
            self.inner
                .build_pubsub_publisher(self.pubsub_config.clone())
                .await?,
            project.into(),
            publisher_id.into(),
        ))
    }
}
