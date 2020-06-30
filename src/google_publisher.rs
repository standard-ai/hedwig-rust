use futures::{TryFutureExt, TryStreamExt};
use std::{borrow::Cow, future::Future, pin::Pin, sync::Arc, task};
use yup_oauth2::authenticator::Authenticator;

use crate::{PublisherResult, ValidatedMessage};

#[derive(Debug, thiserror::Error)]
enum GooglePubsubError {
    #[error("Could not get authentication token")]
    GetAuthToken(#[source] yup_oauth2::Error),
    #[error("Could not POST the request with messages")]
    PostMessages(#[source] hyper::Error),
    #[error("Could not construct the request URI")]
    ConstructRequestUri(#[source] http::uri::InvalidUri),
    #[error("Could not construct the request")]
    ConstructRequest(#[source] http::Error),
    #[error("Could not serialize a publish request")]
    SerializePublishRequest(#[source] serde_json::Error),
    #[error("Publish request failed with status code {1}")]
    ResponseStatus(
        #[source] Option<Box<dyn std::error::Error + Sync + Send>>,
        http::StatusCode,
    ),
    #[error("Could not parse the response body")]
    ResponseParse(#[source] serde_json::Error),
    #[error("Could not receive the response body")]
    ResponseBodyReceive(#[source] hyper::Error),
}

const JSON_METATYPE: &str = "application/json";

/// A publisher that uses the Google Cloud Pub/Sub service as a message transport
///
/// # Examples
///
/// ```no_run
/// async {
///     let google_project =
///         std::env::var("GOOGLE_CLOUD_PROJECT").unwrap();
///     let google_credentials = std::env::var("GOOGLE_APPLICATION_CREDENTIALS").unwrap();
///     let secret = yup_oauth2::read_service_account_key(google_credentials)
///         .await
///         .expect("$GOOGLE_APPLICATION_CREDENTIALS is not a valid service account key");
///     let client = hyper::Client::builder().build(hyper_tls::HttpsConnector::new());
///     let authenticator = std::sync::Arc::new(
///         yup_oauth2::ServiceAccountAuthenticator::builder(secret)
///              .hyper_client(client.clone())
///              .build()
///              .await
///              .expect("could not create an authenticator")
///     );
///     Ok::<_, Box<dyn std::error::Error>>(
///         hedwig::publishers::GooglePubSubPublisher::new(google_project, client, authenticator)
///     )
/// };
/// ```
#[allow(missing_debug_implementations)]
pub struct GooglePubSubPublisher<C> {
    google_cloud_project: Cow<'static, str>,
    client: hyper::Client<C>,
    authenticator: Arc<Authenticator<C>>
}

impl<C> GooglePubSubPublisher<C> {
    /// Create a new Google Cloud Pub/Sub publisher
    pub fn new<P>(
        project: P,
        client: hyper::Client<C>,
        authenticator: Arc<Authenticator<C>>,
    ) -> GooglePubSubPublisher<C>
    where
        P: Into<Cow<'static, str>>,
    {
        GooglePubSubPublisher {
            google_cloud_project: project.into(),
            client,
            authenticator,
        }
    }
}

impl<C> crate::Publisher for GooglePubSubPublisher<C>
where
    C: hyper::client::connect::Connect + Clone + Send + Sync + 'static,
{
    type MessageId = String;
    type PublishFuture = GooglePubSubPublishFuture;

    fn publish(&self, topic: &'static str, messages: Vec<ValidatedMessage>) -> Self::PublishFuture {
        let client = self.client.clone();
        let authenticator = self.authenticator.clone();
        let uri = http::Uri::from_maybe_shared(format!(
            "https://pubsub.googleapis.com/v1/projects/{0}/topics/hedwig-{1}:publish",
            self.google_cloud_project, topic
        ));
        GooglePubSubPublishFuture(Box::pin(async move {
            let result = async {
                let uri = uri.map_err(GooglePubsubError::ConstructRequestUri)?;
                const AUTH_SCOPES: [&str; 1] = ["https://www.googleapis.com/auth/pubsub"];
                let token = authenticator
                    .token(&AUTH_SCOPES)
                    .await
                    .map_err(GooglePubsubError::GetAuthToken)?;
                let data = serde_json::to_vec(&PubsubPublishRequestSchema {
                    messages: &messages,
                })
                .map_err(GooglePubsubError::SerializePublishRequest)?;
                let request = http::Request::post(uri)
                    .header(
                        http::header::AUTHORIZATION,
                        format!("Bearer {}", token.as_str()),
                    )
                    .header(http::header::ACCEPT, JSON_METATYPE)
                    .header(http::header::CONTENT_TYPE, JSON_METATYPE)
                    .body(hyper::Body::from(data))
                    .map_err(GooglePubsubError::ConstructRequest)?;
                let response = client
                    .request(request)
                    .map_err(GooglePubsubError::PostMessages)
                    .await?;
                let (parts, body) = response.into_parts();
                let body_data = body
                    .map_ok(|v| v.to_vec())
                    .try_concat()
                    .map_err(GooglePubsubError::ResponseBodyReceive)
                    .await?;
                if !parts.status.is_success() {
                    let src = serde_json::from_slice(&body_data)
                        .ok()
                        .map(|v: PubsubPublishFailResponseSchema| v.error.message.into());
                    return Err(GooglePubsubError::ResponseStatus(src, parts.status));
                }
                let rsp: PubsubPublishResponseSchema =
                    serde_json::from_slice(&body_data).map_err(GooglePubsubError::ResponseParse)?;
                Ok(rsp)
            }
            .await;
            match result {
                Ok(PubsubPublishResponseSchema { message_ids }) => {
                    PublisherResult::Success(message_ids)
                }
                Err(e) => PublisherResult::OneError(e.into(), messages),
            }
        }))
    }
}

/// The `GooglePubSubPublisher::publish` future
pub struct GooglePubSubPublishFuture(
    Pin<Box<dyn Future<Output = PublisherResult<String>> + Send + 'static>>,
);

impl Future for GooglePubSubPublishFuture {
    type Output = PublisherResult<String>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> task::Poll<Self::Output> {
        Pin::new(&mut self.0).poll(cx)
    }
}

/// Schema for the Google PubsubMessage REST API type
#[derive(serde::Serialize)]
struct PubsubMessageSchema<'a> {
    data: &'a str,
    attributes: &'a crate::Headers,
}

/// Schema for the Google PubsubRequest REST API type
#[derive(serde::Serialize)]
struct PubsubPublishRequestSchema<'a> {
    #[serde(serialize_with = "serialize_validated_messages")]
    messages: &'a [ValidatedMessage],
}

fn serialize_validated_messages<S: serde::Serializer>(
    msgs: &[ValidatedMessage],
    serializer: S,
) -> Result<S::Ok, S::Error> {
    let mut seq = serializer.serialize_seq(Some(msgs.len()))?;
    for element in msgs {
        let raw_message = base64::encode(
            // Would be better with `S::to_string(&element)` if it was a thing. Then we could
            // properly propagate the error here... That said serde_json probably cannot fail.
            // Hopefully.
            &serde_json::to_vec(&element).expect("could not serialize message contents"),
        );
        serde::ser::SerializeSeq::serialize_element(
            &mut seq,
            &PubsubMessageSchema {
                data: &raw_message,
                attributes: &element.metadata.headers,
            },
        )?;
    }
    serde::ser::SerializeSeq::end(seq)
}

/// Schema for the Google PubsubResponse REST API type
#[derive(serde::Deserialize)]
struct PubsubPublishResponseSchema {
    #[serde(rename = "messageIds")]
    message_ids: Vec<String>,
}

#[derive(serde::Deserialize)]
struct PubsubPublishFailResponseSchema {
    error: PubsubPublishErrorSchema,
}

#[derive(serde::Deserialize)]
struct PubsubPublishErrorSchema {
    message: String,
}

#[cfg(test)]
mod tests {
    #[test]
    fn assert_send_sync() {
        fn assert_markers<T: Send + Sync>() {}
        assert_markers::<super::GooglePubSubPublisher<()>>();
    }
}
