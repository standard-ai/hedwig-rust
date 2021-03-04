use futures_util::stream::{Stream, StreamExt, TryStreamExt};
use std::{borrow::Cow, pin::Pin, sync::Arc, task, time::SystemTime};
use yup_oauth2::authenticator::Authenticator;

use crate::{Topic, ValidatedMessage};

const AUTH_SCOPES: [&str; 1] = ["https://www.googleapis.com/auth/pubsub"];
const JSON_METATYPE: &str = "application/json";
const API_BODY_PREFIX: &[u8] = br##"{"messages":["##;
const API_BODY_SUFFIX: &[u8] = br##"]}"##;

// https://cloud.google.com/pubsub/quotas#resource_limits
const API_DATA_LENGTH_LIMIT: usize = 10 * 1024 * 1024; // 10MiB
const API_MSG_COUNT_LIMIT: usize = 1000;
// Not actually important, because submitting through API already has the same limit (and there's
// some overhead in the API call), so even if we do check this, we would still be prone to hitting
// the `API_DATA_LENGTH_LIMIT` limit. So just check the `API_DATA_LENGTH_LIMIT`.
//
// const API_MSG_LENGTH_LIMIT: usize = 10 * 1024 * 1024; // 10MiB
const API_MSG_ATTRIBUTE_COUNT_LIMIT: usize = 100;
const API_MSG_ATTRIBUTE_KEY_SIZE_LIMIT: usize = 256; // 256 bytes
const API_MSG_ATTRIBUTE_VAL_SIZE_LIMIT: usize = 1024; // 1024 bytes

/// Error messages that occur when publishing to Google PubSub.
#[derive(Debug, Clone, thiserror::Error)]
#[non_exhaustive]
#[cfg_attr(docsrs, doc(cfg(feature = "google")))]
pub enum GooglePubSubError {
    /// Could not get authentication token.
    #[error("could not get authentication token")]
    GetAuthToken(#[source] Arc<yup_oauth2::Error>),
    /// Could not POST the request with messages.
    #[error("could not POST the request with messages")]
    PostMessages(#[source] Arc<hyper::Error>),
    /// Could not construct the request URI.
    #[error("could not construct the request URI")]
    ConstructRequestUri(#[source] Arc<http::uri::InvalidUri>),
    /// Could not construct the request.
    #[error("could not construct the request")]
    ConstructRequest(#[source] Arc<http::Error>),
    /// Publish request failed with a bad HTTP status code.
    #[error("publish request failed with status code {1}")]
    ResponseStatus(#[source] Option<Arc<PubSubPublishError>>, http::StatusCode),
    /// Could not parse the response body.
    #[error("could not parse the response body")]
    ResponseParse(#[source] Arc<serde_json::Error>),
    /// Could not receive the response body.
    #[error("could not receive the response body")]
    ResponseBodyReceive(#[source] Arc<hyper::Error>),

    // Critical message serialization errors
    /// Message contains too many headers.
    #[error("message contains too many headers")]
    MessageTooManyHeaders,
    /// Message contains a header key that's too large.
    #[error("message contains a header key that's too large")]
    MessageHeaderKeysTooLarge,
    /// Message contains a header with {0} key which is reserved.
    #[error("message contains a header with {0} key which is reserved")]
    MessageHeaderKeysReserved(Arc<str>),
    /// Message contains a header value that's too large.
    #[error("message contains a header value that's too large")]
    MessageHeaderValuesTooLarge,
    /// Encoded message data is too long.
    #[error("encoded message data is too long")]
    MessageDataTooLong,
    /// Message timestamp is too far in the past.
    #[error("message timestamp is too far in the past")]
    MessageTimestampTooOld(#[source] std::time::SystemTimeError),
    /// Could not serialize message data.
    #[error("could not serialize message data")]
    SerializeMessageData(#[source] Arc<serde_json::Error>),
}

/// An error message returned by the API
#[derive(Debug, thiserror::Error)]
#[error("{message}")]
#[cfg_attr(docsrs, doc(cfg(feature = "google")))]
pub struct PubSubPublishError {
    message: String,
}

impl GooglePubSubError {
    /// Can this error be considered recoverable.
    ///
    /// Some examples of transient errors include errors such as failure to make a network request
    /// or authenticate with the API endpoint.
    ///
    /// In these instances there is a good chance that retrying publishing of a message may
    /// succeed.
    ///
    /// # Examples
    ///
    /// This function is useful when deciding whether to re-queue message for publishing.
    ///
    /// ```no_run
    /// # use hedwig::publishers::GooglePubSubPublisher;
    /// use futures_util::stream::StreamExt;
    ///
    /// let publisher: GooglePubSubPublisher<hyper::client::HttpConnector> = unimplemented!();
    /// let mut batch = hedwig::PublishBatch::new();
    /// // add messages
    /// let mut stream = batch.publish(&publisher);
    /// let mut next_batch = hedwig::PublishBatch::new();
    /// async {
    ///     while let Some(result) = stream.next().await {
    ///         match result {
    ///             (Ok(id), _, msg) => {
    ///                 println!("message {} published successfully: {:?}", msg.uuid(), id);
    ///             }
    ///             (Err(e), topic, msg) if e.is_transient() => {
    ///                 // Retry
    ///                 next_batch.push(topic, msg);
    ///             }
    ///             (Err(e), _, msg) => {
    ///                 eprintln!("failed to publish {}: {}", msg.uuid(), e);
    ///             }
    ///         }
    ///     }
    /// };
    /// ```
    pub fn is_transient(&self) -> bool {
        use http::StatusCode;
        use GooglePubSubError::*;
        const GOOGLE_STATUS_CODE_CANCELLED: u16 = 499;

        match self {
            // These will typically encode I/O errors, although they might encode non-I/O stuff
            // too.
            PostMessages(..) => true,
            ResponseBodyReceive(..) => true,
            GetAuthToken(err) if matches!(**err, yup_oauth2::Error::HttpError(_)) => true,
            GetAuthToken(_) => false,

            // Some HTTP response codes are plausibly retry-able.
            //
            // References:
            //
            // https://github.com/googleapis/google-cloud-go/blob/9e64b018255bd8d9b31d60e8f396966251de946b/pubsub/apiv1/publisher_client.go#L86
            // https://cloud.google.com/apis/design/errors#handling_errors
            // https://cloud.google.com/pubsub/docs/reference/error-codes
            ResponseStatus(_, StatusCode::BAD_GATEWAY) => true,
            ResponseStatus(_, StatusCode::SERVICE_UNAVAILABLE) => true,
            ResponseStatus(_, StatusCode::GATEWAY_TIMEOUT) => true,
            ResponseStatus(_, StatusCode::TOO_MANY_REQUESTS) => true,
            ResponseStatus(_, StatusCode::CONFLICT) => true,
            ResponseStatus(_, code) => code.as_u16() == GOOGLE_STATUS_CODE_CANCELLED,

            // Unlikely to ever succeed.
            ConstructRequestUri(..) => false,
            ResponseParse(..) => false,
            ConstructRequest(..) => false,
            MessageTooManyHeaders => false,
            MessageHeaderKeysTooLarge => false,
            MessageHeaderKeysReserved(..) => false,
            MessageHeaderValuesTooLarge => false,
            MessageDataTooLong => false,
            MessageTimestampTooOld(..) => false,
            SerializeMessageData(..) => false,
        }
    }
}

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
///     let publisher = hedwig::publishers::GooglePubSubPublisher::new(
///         "rust_publisher".into(),
///         google_project.into(),
///         client,
///         authenticator
///     );
///     Ok::<_, Box<dyn std::error::Error>>(publisher)
/// };
/// ```
#[allow(missing_debug_implementations)]
#[cfg_attr(docsrs, doc(cfg(feature = "google")))]
pub struct GooglePubSubPublisher<C> {
    identifier: Cow<'static, str>,
    google_cloud_project: Cow<'static, str>,
    client: hyper::Client<C>,
    authenticator: Arc<Authenticator<C>>,
}

impl<C> GooglePubSubPublisher<C> {
    /// Create a new Google Cloud Pub/Sub publisher
    pub fn new(
        identifier: Cow<'static, str>,
        google_cloud_project: Cow<'static, str>,
        client: hyper::Client<C>,
        authenticator: Arc<Authenticator<C>>,
    ) -> GooglePubSubPublisher<C> {
        GooglePubSubPublisher {
            identifier,
            google_cloud_project,
            client,
            authenticator,
        }
    }
}

async fn publish_single_body<C>(
    batch: Result<SegmentationResult, GooglePubSubError>,
    uri: http::uri::Uri,
    authenticator: Arc<Authenticator<C>>,
    client: hyper::Client<C>,
) -> Vec<Result<String, GooglePubSubError>>
where
    C: hyper::client::connect::Connect + Clone + Send + Sync + 'static,
{
    let batch = match batch {
        Ok(b) => b,
        Err(e) => return vec![Err(e)],
    };
    let msg_count = batch.messages_in_body;
    let result = async move {
        let token = authenticator
            .token(&AUTH_SCOPES)
            .await
            .map_err(|e| GooglePubSubError::GetAuthToken(Arc::new(e)))?;
        let request = http::Request::post(uri)
            .header(
                http::header::AUTHORIZATION,
                format!("Bearer {}", token.as_str()),
            )
            .header(http::header::ACCEPT, JSON_METATYPE)
            .header(http::header::CONTENT_TYPE, JSON_METATYPE)
            .body(batch.request_body)
            .map_err(|e| GooglePubSubError::ConstructRequest(Arc::new(e)))?;
        let response = client
            .request(request)
            .await
            .map_err(|e| GooglePubSubError::PostMessages(Arc::new(e)))?;
        let (parts, body) = response.into_parts();
        let response_body_data = body
            .try_fold(vec![], |mut acc, ok| async move {
                acc.extend(ok);
                Ok(acc)
            })
            .await
            .map_err(|e| GooglePubSubError::ResponseBodyReceive(Arc::new(e)))?;
        if !parts.status.is_success() {
            let src = serde_json::from_slice(&response_body_data)
                .ok()
                .map(|v: PubSubPublishFailResponseSchema| Arc::new(v.error.into()));
            return Err(GooglePubSubError::ResponseStatus(src, parts.status));
        }
        let response_json: PubSubPublishResponseSchema =
            serde_json::from_slice(&response_body_data)
                .map_err(|e| GooglePubSubError::ResponseParse(Arc::new(e)))?;
        Ok(response_json.message_ids.into_iter().map(Ok).collect())
    }
    .await;
    match result {
        Ok(msgs) => msgs,
        Err(err) => std::iter::repeat(err).map(Err).take(msg_count).collect(),
    }
}

impl<C> crate::Publisher for GooglePubSubPublisher<C>
where
    C: hyper::client::connect::Connect + Clone + Send + Sync + 'static,
{
    type MessageId = String;
    type MessageError = GooglePubSubError;
    type PublishStream = GooglePubSubPublishStream;

    fn publish<'a, I>(&self, topic: Topic, messages: I) -> Self::PublishStream
    where
        I: Iterator<Item = &'a ValidatedMessage>,
    {
        let Self {
            identifier,
            authenticator,
            client,
            google_cloud_project,
        } = self;
        let uri = http::Uri::from_maybe_shared(format!(
            "https://pubsub.googleapis.com/v1/projects/{0}/topics/hedwig-{1}:publish",
            google_cloud_project, topic
        ))
        .map_err(Arc::new);
        let uri = match uri {
            Ok(uri) => uri,
            Err(e) => {
                let c = messages.count();
                return GooglePubSubPublishStream(Box::pin(futures_util::stream::iter(
                    std::iter::repeat(Err(GooglePubSubError::ConstructRequestUri(e))).take(c),
                )));
            }
        };
        let stream = GoogleMessageSegmenter::new(&*identifier, messages)
            .map(|batch| {
                publish_single_body(batch, uri.clone(), authenticator.clone(), client.clone())
            })
            .collect::<futures_util::stream::FuturesOrdered<_>>()
            .flat_map(futures_util::stream::iter);
        GooglePubSubPublishStream(Box::pin(stream))
    }
}

/// The `GooglePubSubPublisher::publish` stream
pub struct GooglePubSubPublishStream(
    Pin<Box<dyn Stream<Item = Result<String, GooglePubSubError>> + Send + 'static>>,
);

impl Stream for GooglePubSubPublishStream {
    type Item = Result<String, GooglePubSubError>;
    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
    ) -> task::Poll<Option<Self::Item>> {
        Pin::new(&mut self.0).poll_next(cx)
    }
}

/// Encode and segment the messages into API request bodies Google is willing to stomach.
///
/// Here, the logic is somewhat convoluted. We, in a very hacky way, implement a portion
/// of json serialization in order to validate requirements PubSub places on API calls. In
/// particular Google PubSub has the following limits that matter.  Some of those limits are
/// something we can possibly attempt to handle here:
///
/// Single publish API call: < API_DATA_LENGTH_LIMIT
/// Single publish API call: < API_MSG_COUNT_LIMIT
///
/// Some others are irrecoverable and are indicative of malformed messages:
///
/// Message size (the data field) < API_MSG_LENGTH_LIMIT
/// Attributes per message < API_MSG_ATTRIBUTE_COUNT_LIMIT
/// Attribute key size < API_MSG_ATTRIBUTE_KEY_SIZE_LIMIT
/// Attribute value size < API_MSG_ATTRIBUTE_VAL_SIZE_LIMIT
///
/// We will validate for both of these in this code section below.
struct GoogleMessageSegmenter<'a, I> {
    identifier: &'a str,
    messages: I,
    body_data: Vec<u8>,
    messages_in_body: usize,
    enqueued_error: Option<GooglePubSubError>,
}

struct SegmentationResult {
    request_body: hyper::Body,
    messages_in_body: usize,
}

impl<'a, I> GoogleMessageSegmenter<'a, I> {
    fn new(identifier: &'a str, messages: I) -> Self {
        Self {
            identifier,
            messages,
            body_data: Vec::from(API_BODY_PREFIX),
            messages_in_body: 0,
            enqueued_error: None,
        }
    }

    fn take_batch(&mut self) -> Option<SegmentationResult> {
        debug_assert!(self.messages_in_body <= API_MSG_COUNT_LIMIT);
        debug_assert!(self.body_data.len() <= API_DATA_LENGTH_LIMIT);
        if self.messages_in_body == 0 {
            return None;
        }
        let mut body_data = std::mem::replace(&mut self.body_data, Vec::from(API_BODY_PREFIX));
        let messages_in_body = std::mem::replace(&mut self.messages_in_body, 0);
        body_data.extend(API_BODY_SUFFIX);
        Some(SegmentationResult {
            request_body: hyper::Body::from(body_data),
            messages_in_body,
        })
    }

    fn maybe_enqueue_error(
        &mut self,
        error: GooglePubSubError,
    ) -> Result<SegmentationResult, GooglePubSubError> {
        match self.take_batch() {
            Some(batch) => {
                self.enqueued_error = Some(error);
                Ok(batch)
            }
            None => Err(error),
        }
    }

    fn encode_message(&self, message: &ValidatedMessage) -> Result<Vec<u8>, GooglePubSubError> {
        let header_count = message.headers.len() + PubSubAttributesSchema::BUILTIN_ATTRIBUTES;
        if header_count > API_MSG_ATTRIBUTE_COUNT_LIMIT {
            return Err(GooglePubSubError::MessageTooManyHeaders);
        }
        for (key, value) in message.headers.iter() {
            if key.len() >= API_MSG_ATTRIBUTE_KEY_SIZE_LIMIT {
                return Err(GooglePubSubError::MessageHeaderKeysTooLarge);
            }
            if key.starts_with("hedwig_") {
                return Err(GooglePubSubError::MessageHeaderKeysReserved(key[..].into()));
            }
            if value.len() >= API_MSG_ATTRIBUTE_VAL_SIZE_LIMIT {
                return Err(GooglePubSubError::MessageHeaderValuesTooLarge);
            }
        }
        let schema_value_too_large = message.schema.len() >= API_MSG_ATTRIBUTE_VAL_SIZE_LIMIT;
        let identifier_too_large = self.identifier.len() >= API_MSG_ATTRIBUTE_VAL_SIZE_LIMIT;
        if schema_value_too_large || identifier_too_large {
            return Err(GooglePubSubError::MessageHeaderValuesTooLarge);
        }
        let payload = base64::encode(&message.data);
        let timestamp = message
            .timestamp
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(GooglePubSubError::MessageTimestampTooOld)?
            .as_millis()
            .to_string();
        let encoded_api_body = serde_json::to_vec(&PubSubMessageSchema {
            data: &payload,
            attributes: PubSubAttributesSchema {
                hedwig_id: &message.id,
                hedwig_publisher: &*self.identifier,
                hedwig_message_timestamp: &timestamp,
                hedwig_format_version: "1.0",
                hedwig_schema: message.schema,
                headers: &message.headers,
            },
        })
        .map_err(|e| GooglePubSubError::SerializeMessageData(Arc::new(e)))?;
        if API_BODY_PREFIX.len() + API_BODY_SUFFIX.len() + encoded_api_body.len()
            >= API_DATA_LENGTH_LIMIT
        {
            // This message wouldn’t fit even if we created a new batch, it is too large!
            return Err(GooglePubSubError::MessageDataTooLong);
        }
        Ok(encoded_api_body)
    }

    fn append_message_data(&mut self, msg_json: &[u8]) {
        debug_assert!(self.messages_in_body == 0 || self.body_data.last() == Some(&b','));
        self.body_data.extend(msg_json);
        self.messages_in_body += 1;
    }
}

impl<'a, 'v, I: Iterator<Item = &'v ValidatedMessage>> Iterator for GoogleMessageSegmenter<'a, I> {
    type Item = Result<SegmentationResult, GooglePubSubError>;
    fn next(&mut self) -> Option<Self::Item> {
        // This iterator needs to preserve the ordering of messages, so when we encounter an error
        // we must first release the currently built batch before reporting an error. Then, on the
        // next iteration (i.e. this one) we return the error.
        if let Some(err) = self.enqueued_error.take() {
            return Some(Err(err));
        }
        loop {
            let message = match self.messages.next() {
                None => return self.take_batch().map(Ok),
                Some(msg) => msg,
            };
            // Validate if a message is structurally valid.
            let msg_json = match self.encode_message(message) {
                Ok(msg_json) => msg_json,
                Err(e) => return Some(self.maybe_enqueue_error(e)),
            };
            // Append to current batch or create a new one (and return finished batch).
            let need_comma = self.messages_in_body != 0;
            let data_len = self.body_data.len() + need_comma as usize + msg_json.len();
            let data_fits_in_current = data_len + API_BODY_SUFFIX.len() < API_DATA_LENGTH_LIMIT;
            let message_fits_in_current = self.messages_in_body < API_MSG_COUNT_LIMIT;
            if !data_fits_in_current || !message_fits_in_current {
                // We need a new batch.
                self.body_data.extend(API_BODY_SUFFIX);
                let batch = self.take_batch();
                self.append_message_data(&msg_json);
                debug_assert!(batch.is_some());
                return batch.map(Ok);
            } else {
                // We can append the current batch.
                if need_comma {
                    self.body_data.push(b',');
                }
                self.append_message_data(&msg_json);
            }
        }
    }
}

/// Schema for the Google PubSubMessage REST API type
#[derive(serde::Serialize)]
struct PubSubMessageSchema<'a> {
    data: &'a str,
    attributes: PubSubAttributesSchema<'a>,
}

#[derive(serde::Serialize)]
struct PubSubAttributesSchema<'a> {
    hedwig_id: &'a uuid::Uuid,
    hedwig_format_version: &'a str,
    hedwig_message_timestamp: &'a str,
    hedwig_publisher: &'a str,
    hedwig_schema: &'a str,
    #[serde(flatten)]
    headers: &'a crate::Headers,
}

impl<'a> PubSubAttributesSchema<'a> {
    const BUILTIN_ATTRIBUTES: usize = 5;
}

/// Schema for the Google PubSubResponse REST API type
#[derive(serde::Deserialize)]
struct PubSubPublishResponseSchema {
    #[serde(rename = "messageIds")]
    message_ids: Vec<String>,
}

#[derive(serde::Deserialize)]
struct PubSubPublishFailResponseSchema {
    error: PubSubPublishErrorSchema,
}

#[derive(serde::Deserialize)]
struct PubSubPublishErrorSchema {
    message: String,
}

impl From<PubSubPublishErrorSchema> for PubSubPublishError {
    fn from(other: PubSubPublishErrorSchema) -> Self {
        Self {
            message: other.message,
        }
    }
}

#[cfg(test)]
#[allow(unused)]
mod tests {
    use super::{GoogleMessageSegmenter, GooglePubSubError, SegmentationResult};
    use crate::{tests::*, validators, Headers, Message, ValidatedMessage};
    use futures_util::stream::TryStreamExt;
    use hyper::body::HttpBody;
    use std::time::SystemTime;
    use uuid::Uuid;

    #[test]
    fn assert_send_sync() {
        fn assert_markers<T: Send + Sync>() {}
        assert_markers::<super::GooglePubSubPublisher<()>>();
    }

    #[test]
    fn empty_segmenter() {
        let mut segmenter = GoogleMessageSegmenter::new("", [].iter());
        assert!(segmenter.take_batch().is_none());
        assert!(segmenter.next().is_none());
        assert!(segmenter.take_batch().is_none());
        assert!(segmenter.next().is_none());
    }

    async fn test_segmenter(messages: Vec<ValidatedMessage>) {
        let messages_expected = messages.len();
        let mut segmenter = GoogleMessageSegmenter::new("", messages.iter());
        assert!(segmenter.take_batch().is_none());
        let mut segment = match segmenter.next() {
            None => panic!("expected a segment!"),
            Some(Err(e)) => panic!("expected a successful segment! Got {}", e),
            Some(Ok(segment)) => segment,
        };
        assert!(matches!(segmenter.next(), None));
        assert!(segmenter.take_batch().is_none());
        let data = segment
            .request_body
            .data()
            .await
            .expect("body")
            .expect("body");
        let val = serde_json::from_slice::<serde_json::Value>(&data[..])
            .expect("data should be valid json!");
        let messages = val.get("messages").expect("messages key exists");
        for message_idx in 0..messages_expected {
            let message = messages.get(message_idx).expect("mesage exists");
            message.get("data").expect("mesage data exists");
        }
    }

    #[cfg(feature = "json-schema")]
    #[tokio::test]
    async fn single_msg_segmenter() {
        let validator = validators::JsonSchemaValidator::new(SCHEMA).unwrap();
        let msgs = vec![JsonUserCreatedMessage::new_valid("U_123")
            .encode(&validator)
            .unwrap()];
        test_segmenter(msgs).await;
    }

    #[cfg(feature = "json-schema")]
    #[tokio::test]
    async fn multi_msg_segmenter() {
        let validator = validators::JsonSchemaValidator::new(SCHEMA).unwrap();
        let msgs = vec![
            JsonUserCreatedMessage::new_valid("U_123")
                .encode(&validator)
                .unwrap(),
            JsonUserCreatedMessage::new_valid("U_124")
                .encode(&validator)
                .unwrap(),
            JsonUserCreatedMessage::new_valid("U_125")
                .encode(&validator)
                .unwrap(),
            JsonUserCreatedMessage::new_valid("U_126")
                .encode(&validator)
                .unwrap(),
        ];
        test_segmenter(msgs).await;
    }

    #[cfg(feature = "json-schema")]
    #[tokio::test]
    async fn huuuge_single_msg_segmenter() {
        let validator = validators::JsonSchemaValidator::new(SCHEMA).unwrap();
        let msgs = vec![JsonUserCreatedMessage::new_valid(
            // base64 makes strings 4/3 larger than original.
            String::from_utf8(vec![b'a'; (10 * 1024 * 1024 - 512) * 3 / 4]).unwrap(),
        )
        .encode(&validator)
        .unwrap()];
        test_segmenter(msgs).await;
    }

    #[cfg(feature = "json-schema")]
    #[tokio::test]
    async fn less_huge_double_msg_segmenter() {
        let validator = validators::JsonSchemaValidator::new(SCHEMA).unwrap();
        // base64 makes strings 4/3 larger than original.
        let msgs = vec![
            JsonUserCreatedMessage::new_valid(
                String::from_utf8(vec![b'a'; (5 * 1024 * 1024 - 512) * 3 / 4]).unwrap(),
            )
            .encode(&validator)
            .unwrap(),
            JsonUserCreatedMessage::new_valid(
                String::from_utf8(vec![b'a'; (5 * 1024 * 1024 - 512) * 3 / 4]).unwrap(),
            )
            .encode(&validator)
            .unwrap(),
        ];
        test_segmenter(msgs).await;
    }

    #[cfg(feature = "json-schema")]
    #[test]
    fn oversized_single_msg_segmenter() {
        let validator = validators::JsonSchemaValidator::new(SCHEMA).unwrap();
        let small_message = JsonUserCreatedMessage::new_valid("U345");
        let oversized_message = JsonUserCreatedMessage::new_valid(
            String::from_utf8(vec![b'a'; (10 * 1024 * 1024 - 50) * 3 / 4]).unwrap(),
        );
        let msgs = vec![
            small_message.encode(&validator).unwrap(),
            oversized_message.encode(&validator).unwrap(),
            small_message.encode(&validator).unwrap(),
        ];
        let mut segmenter = GoogleMessageSegmenter::new("", msgs.iter());
        assert!(segmenter.take_batch().is_none());
        assert!(matches!(segmenter.next(), Some(Ok(_))));
        assert!(matches!(segmenter.next(), Some(Err(_))));
        assert!(matches!(segmenter.next(), Some(Ok(_))));
        assert!(matches!(segmenter.next(), None));
    }

    #[cfg(feature = "json-schema")]
    #[test]
    fn segmenter_preserves_order_when_splitting() {
        let validator = validators::JsonSchemaValidator::new(SCHEMA).unwrap();
        let small_message = JsonUserCreatedMessage::new_valid("U345");
        let oversized_message = JsonUserCreatedMessage::new_valid(
            String::from_utf8(vec![b'a'; (10 * 1024 * 1024 - 512) * 3 / 4]).unwrap(),
        );

        let msgs = vec![
            small_message.encode(&validator).unwrap(),
            oversized_message.encode(&validator).unwrap(),
            small_message.encode(&validator).unwrap(),
        ];
        let mut segmenter = GoogleMessageSegmenter::new("", msgs.iter());
        assert!(segmenter.take_batch().is_none());
        assert!(matches!(segmenter.next(), Some(Ok(_))));
        assert!(matches!(segmenter.next(), Some(Ok(_))));
        assert!(matches!(segmenter.next(), Some(Ok(_))));
        assert!(matches!(segmenter.next(), None));
    }

    #[test]
    fn errors_send_sync() {
        assert_error::<super::GooglePubSubError>();
    }

    #[test]
    fn publish_stream_is_send() {
        assert_send::<super::GooglePubSubPublishStream>();
    }

    #[cfg(feature = "json-schema")]
    fn test_headers(headers: Headers) -> Option<Result<SegmentationResult, GooglePubSubError>> {
        let validator = validators::JsonSchemaValidator::new(SCHEMA).unwrap();
        let msg = JsonUserCreatedMessage {
            uuid: Uuid::new_v4(),
            schema: "https://hedwig.corp/schema#/schemas/user.created/1.0",
            user_id: String::from("hello"),
            headers,
            time: SystemTime::now(),
        }
        .encode(&validator)
        .expect("validates");
        let mut segmenter = GoogleMessageSegmenter::new("", vec![&msg].into_iter());
        segmenter.next()
    }

    #[cfg(feature = "json-schema")]
    fn test_header_name_value(
        name: String,
        value: String,
    ) -> Option<Result<SegmentationResult, GooglePubSubError>> {
        test_headers(vec![(name, value)].into_iter().collect())
    }

    #[cfg(feature = "json-schema")]
    fn test_header_name(name: String) -> Option<Result<SegmentationResult, GooglePubSubError>> {
        test_header_name_value(name, "value".into())
    }

    #[cfg(feature = "json-schema")]
    fn test_header_value(value: String) -> Option<Result<SegmentationResult, GooglePubSubError>> {
        test_header_name_value("key".into(), value)
    }

    #[cfg(feature = "json-schema")]
    #[test]
    fn reserved_header_names_fail_validation() {
        for &name in &["hedwig_", "hedwig_banana", "hedwig_message_timestamp"] {
            assert!(matches!(
                test_header_name(name.into()),
                Some(Err(GooglePubSubError::MessageHeaderKeysReserved(_)))
            ));
        }
    }

    #[cfg(feature = "json-schema")]
    #[test]
    fn valid_header_names_and_values_pass() {
        for &name in &["hello", "hedwi", "banana"] {
            assert!(matches!(test_header_name(name.into()), Some(Ok(_))));
        }
    }

    #[cfg(feature = "json-schema")]
    #[test]
    fn valid_very_long_header_name() {
        let name = String::from_utf8(vec![b'a'; 255]).unwrap();
        assert!(matches!(test_header_name(name), Some(Ok(_))));
    }

    #[cfg(feature = "json-schema")]
    #[test]
    fn invalid_overlong_header_name() {
        let name = String::from_utf8(vec![b'a'; 256]).unwrap();
        assert!(matches!(
            test_header_name(name),
            Some(Err(GooglePubSubError::MessageHeaderKeysTooLarge))
        ));
    }

    #[cfg(feature = "json-schema")]
    #[test]
    fn valid_very_long_header_value() {
        let name = String::from_utf8(vec![b'a'; 1023]).unwrap();
        assert!(matches!(test_header_value(name), Some(Ok(_))));
    }

    #[cfg(feature = "json-schema")]
    #[test]
    fn invalid_overlong_header_value() {
        let name = String::from_utf8(vec![b'a'; 1024]).unwrap();
        assert!(matches!(
            test_header_value(name),
            Some(Err(GooglePubSubError::MessageHeaderValuesTooLarge))
        ));
    }

    #[cfg(feature = "json-schema")]
    #[tokio::test]
    async fn valid_very_many_headers() {
        let mut headers = Headers::new();
        for i in 0..(super::API_MSG_ATTRIBUTE_COUNT_LIMIT
            - super::PubSubAttributesSchema::BUILTIN_ATTRIBUTES)
        {
            headers.insert(format!("hdr{}", i), String::from("value"));
        }
        let result = test_headers(headers);
        assert!(matches!(result, Some(Ok(_))));
        let result = result.unwrap().unwrap();
        let body = result
            .request_body
            .try_fold(vec![], |mut acc, ok| async move {
                acc.extend(ok);
                Ok(acc)
            })
            .await
            .unwrap();
        let msg: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let actual_attribute_count = msg
            .get("messages")
            .unwrap()
            .get(0)
            .unwrap()
            .get("attributes")
            .unwrap()
            .as_object()
            .unwrap()
            .len();
        assert_eq!(actual_attribute_count, super::API_MSG_ATTRIBUTE_COUNT_LIMIT);
    }

    #[cfg(feature = "json-schema")]
    #[test]
    fn invalid_too_many_headers() {
        let mut headers = Headers::new();
        for i in 0..(100 - super::PubSubAttributesSchema::BUILTIN_ATTRIBUTES + 1) {
            headers.insert(format!("hdr{}", i), String::from("value"));
        }
        assert!(matches!(
            test_headers(headers),
            Some(Err(GooglePubSubError::MessageTooManyHeaders))
        ));
    }
}
