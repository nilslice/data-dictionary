use std::cmp::Ordering;
use std::collections::HashMap;
use std::env;

use crate::error::Error;
use crate::gcp_client::GcpClient;

use chrono::{DateTime, Utc};
use reqwest::{self, Method, StatusCode};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SubscriptionCreatePayload {
    topic: String,
    // TODO: include more fields where applicable:
    // https://cloud.google.com/pubsub/docs/reference/rest/v1/projects.subscriptions/create#request-body
}

fn max_messages_from_env() -> Result<usize, Error> {
    match env::var("DD_TOPIC_MAX_MESSAGES") {
        Ok(v) => v.parse().map_err(|e| Error::Generic(Box::new(e))),
        Err(e) => Err(Error::Generic(Box::new(e))),
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Attributes {
    pub notification_config: String,
    pub event_type: Event,
    pub event_time: DateTime<Utc>,
    pub payload_format: PayloadFormat,
    pub bucket_id: String,
    pub object_id: String,
    pub object_generation: String,
    pub overwritten_by_generation: Option<String>,
    pub overwrote_generation: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PullResponse {
    pub received_messages: Option<Vec<Message>>,
}
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Message {
    pub ack_id: String,
    pub message: PubsubMessage,
}

impl Ord for Message {
    fn cmp(&self, other: &Self) -> Ordering {
        self.message
            .attributes
            .event_time
            .cmp(&other.message.attributes.event_time)
    }
}

impl PartialOrd for Message {
    fn partial_cmp(&self, other: &Message) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Message {
    fn eq(&self, other: &Self) -> bool {
        self.ack_id == other.ack_id
    }
}

impl Eq for Message {}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PubsubMessage {
    pub data: String,
    pub attributes: Attributes,
    pub message_id: String,
    pub publish_time: DateTime<Utc>,
}

pub struct Subscriber<'a> {
    name: String,
    project_id: String,
    topic: String,
    service_endpoint: String,
    max_messages: usize,
    client: &'a GcpClient,
}

impl<'a> Subscriber<'a> {
    /// Creates a pub/sub subscription from the available environment variables. Requires each of
    /// DD_GCP_PROJECT_ID, DD_TOPIC_NAME, DD_SUBSCRIPTION_NAME, PUBSUB_SERVICE to be set.
    pub async fn from_env(client: &'a GcpClient) -> Result<Subscriber<'a>, Error> {
        // TODO: clean this up, it has become disgusting...
        let sub = Subscriber {
            name: env::var("DD_SUBSCRIPTION_NAME")
                .expect("DD_SUBSCRIPTION_NAME environment variable not set"),
            project_id: env::var("DD_GCP_PROJECT_ID")
                .expect("DD_GCP_PROJECT_ID environment variable not set"),
            topic: env::var("DD_TOPIC_NAME").expect("DD_TOPIC_NAME environment variable not set"),
            service_endpoint: env::var("PUBSUB_SERVICE")
                .expect("PUBSUB_SERVICE environment variable not set"),

            max_messages: max_messages_from_env()?,
            client,
        };

        Ok(sub)
    }

    /// Creates a subscription using the "pull" method.
    pub async fn subscribe(&self) -> Result<(), Error> {
        let url = format!("{}/v1/{}", self.service_endpoint, self.name());
        let sub_payload = SubscriptionCreatePayload {
            topic: self.topic(),
        };
        let resp = self
            .client
            .request(Method::PUT, &url)?
            .json(&sub_payload)
            .send()
            .await
            .map_err(|e| {
                Error::Http(format!("failed to make subscription create request: {}", e))
            })?;

        match resp.status() {
            StatusCode::OK | StatusCode::CONFLICT => Ok(()),
            StatusCode::NOT_FOUND => Err(Error::Http(format!(
                "pubsub subscription failed, topic '{}' does not exist",
                self.topic()
            ))),
            _ => Err(Error::Http(format!(
            "pubsub subscription failed, unexpected response with status code: {}, and body: {:?}",
            resp.status(),
            resp.text().await
        ))),
        }
    }

    pub async fn pull(&self) -> Result<PullResponse, Error> {
        // POST https://pubsub.googleapis.com/v1/{subscription}:pull
        let url = format!("{}/v1/{}:pull", self.service_endpoint, self.name());
        let resp = self
            .client
            .request(Method::POST, &url)?
            .json(&serde_json::json!({ "maxMessages": self.max_messages }))
            .send()
            .await
            .map_err(|e| Error::Http(format!("failed to make subscription pull request: {}", e)))?;

        match resp.status() {
            StatusCode::OK => resp.json().await.map_err(|e| Error::Generic(Box::new(e))),
            _ => Err(Error::Http(format!(
                "subscription pull response error code: {}",
                resp.status()
            ))),
        }
    }

    pub async fn ack(&self, ack_id: impl AsRef<str>) -> Result<(), Error> {
        let url = format!("{}/v1/{}:acknowledge", self.service_endpoint, self.name());
        self.client
            .request(Method::POST, &url)?
            .json(&serde_json::json!({ "ackIds": [ack_id.as_ref()] }))
            .send()
            .await
            .map(|_| ())
            .map_err(|e| Error::Http(format!("failed to make ack request: {}", e)))
    }

    pub fn topic(&self) -> String {
        format!("projects/{}/topics/{}", self.project_id, self.topic)
    }

    pub fn name(&self) -> String {
        format!("projects/{}/subscriptions/{}", self.project_id, self.name)
    }
}

#[test]
fn test_subscription_create_payload() {
    let expected = r#"{
  "topic": "{topic}"
}"#;
    let topic = String::from("datadict-test");
    let expected = expected.replace("{topic}", &topic);
    let sub_payload = SubscriptionCreatePayload { topic };
    let payload = serde_json::to_string_pretty(&sub_payload).unwrap();
    assert_eq!(payload.replace("\n", ""), expected.replace("\n", ""));
}
#[derive(Debug, Deserialize)]
pub struct Notification {
    pub attributes: Attributes,
    pub payload: Option<Payload>,
}

#[derive(Debug, Deserialize)]
pub enum Event {
    #[serde(rename = "OBJECT_FINALIZE")]
    ObjectFinalize,
    #[serde(rename = "OBJECT_METADATA_UPDATE")]
    ObjectMetadataUpdate,
    #[serde(rename = "OBJECT_DELETE")]
    ObjectDelete,
    #[serde(rename = "OBJECT_ARCHIVE")]
    ObjectArchive,
}

#[derive(Debug, Deserialize)]
pub enum PayloadFormat {
    #[serde(rename = "JSON_API_V1")]
    JsonApiV1,
    #[serde(rename = "NONE")]
    None,
}
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Payload {
    pub kind: String,
    pub id: String,
    pub self_link: String,
    pub name: String,
    pub bucket: String,
    pub generation: String,
    pub metageneration: String,
    pub content_type: String,
    pub time_created: DateTime<Utc>,
    pub updated: DateTime<Utc>,
    pub time_deleted: Option<DateTime<Utc>>,
    pub temporary_hold: Option<bool>,
    pub event_based_hold: Option<bool>,
    pub retention_expiration_time: Option<DateTime<Utc>>,
    pub storage_class: String,
    pub time_storage_class_updated: DateTime<Utc>,
    pub size: String,
    pub md5_hash: String,
    pub media_link: String,
    pub content_encoding: Option<String>,
    pub content_disposition: Option<String>,
    pub content_language: Option<String>,
    pub cache_control: Option<String>,
    pub metadata: Option<HashMap<String, String>>,
    pub acl: Option<Vec<ObjectAccessControls>>,
    pub owner: Option<ObjectOwner>,
    pub crc32c: Option<String>,
    pub component_count: Option<usize>,
    pub etag: Option<String>,
    pub customer_encryption: Option<CustomerEncryption>,
    pub kms_key_name: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ObjectAccessControls {
    pub kind: String,
    pub id: String,
    pub self_link: String,
    pub bucket: String,
    pub object: String,
    pub generation: String,
    pub entity: String,
    pub role: String,
    pub email: String,
    pub entity_id: String,
    pub domain: String,
    pub project_team: ProjectTeam,
    pub etag: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProjectTeam {
    pub project_number: String,
    pub team: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ObjectOwner {
    pub entity: String,
    pub entity_id: String,
}
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CustomerEncryption {
    pub encryption_algorithm: String,
    pub key_sha256: String,
}
