use crate::error::Error;

use chrono::{DateTime, Utc};
use gouth::Token;
use reqwest::{
    self,
    header::{HeaderMap, AUTHORIZATION},
    StatusCode,
};
use serde::{Deserialize, Serialize};

use std::collections::HashMap;
use std::env;

pub struct Subscription {
    name: String,
    project_id: String,
    topic: String,
    service_endpoint: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SubscriptionCreatePayload {
    topic: String,
    // TODO: include more fields where applicable:
    // https://cloud.google.com/pubsub/docs/reference/rest/v1/projects.subscriptions/create#request-body
}

impl Subscription {
    /// Creates a pub/sub subscription from the available environment variables. Requires each of
    /// DD_GCP_PROJECT_ID, DD_TOPIC_NAME, DD_SUBSCRIPTION_NAME, PUBSUB_SERVICE to be set.
    pub async fn from_env() -> Result<Subscription, Error> {
        let sub = Subscription {
            name: env::var("DD_SUBSCRIPTION_NAME")
                .expect("DD_SUBSCRIPTION_NAME environment variable not set"),
            project_id: env::var("DD_GCP_PROJECT_ID")
                .expect("DD_GCP_PROJECT_ID environment variable not set"),
            topic: env::var("DD_TOPIC_NAME").expect("DD_TOPIC_NAME environment variable not set"),
            service_endpoint: env::var("PUBSUB_SERVICE")
                .expect("PUBSUB_SERVICE environment variable not set"),
        };

        subscribe(&sub).await?;

        Ok(sub)
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

/// Creates a subscription using the "pull" method.
async fn subscribe(sub: &Subscription) -> Result<(), Error> {
    let url = format!(
        "{}/v1/projects/{}/subscriptions/{}",
        sub.service_endpoint, sub.project_id, sub.name
    );
    let client = reqwest::Client::new();
    let sub_payload = SubscriptionCreatePayload {
        topic: format!("projects/{}/topics/{}", sub.project_id, sub.topic.clone()),
    };
    let mut headers = HeaderMap::new();
    headers.insert(
        AUTHORIZATION,
        get_gcp_auth_token()?
            .parse()
            .expect("failed to parse header value for auth token"),
    );
    let resp = client
        .put(&url)
        .headers(headers)
        .json(&sub_payload)
        .send()
        .await
        .map_err(|e| Error::Http(format!("failed to make subscription request: {}", e)))?;

    match resp.status() {
        StatusCode::OK => Ok(()),
        StatusCode::NOT_FOUND => Err(Error::Http(format!(
            "pubsub subscription failed, topic '{}' does not exist",
            sub.topic
        ))),
        StatusCode::CONFLICT => Ok(()),
        _ => panic!(
            "pubsub subscription failed, unexpected response with status code: {}, and body: {:?}",
            resp.status(),
            resp.text().await
        ),
    }
}

fn get_gcp_auth_token() -> Result<String, Error> {
    let token = Token::new().map_err(|e| Error::Generic(Box::new(e)))?;
    match token.header_value() {
        Ok(arc) => Ok(arc.as_ref().into()),
        Err(e) => Err(Error::Generic(Box::new(e))),
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
#[serde(rename_all = "camelCase")]
pub struct Attributes {
    pub notification_config: String,
    pub event_type: Event,
    pub payload_format: PayloadFormat,
    pub bucket_id: String,
    pub object_id: String,
    pub object_generation: i32,
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
    pub generation: i32,
    pub metageneration: i32,
    pub content_type: String,
    pub time_created: DateTime<Utc>,
    pub updated: DateTime<Utc>,
    pub time_deleted: DateTime<Utc>,
    pub temporary_hold: bool,
    pub event_based_hold: bool,
    pub retention_expiration_time: DateTime<Utc>,
    pub storage_class: String,
    pub time_storage_class_updated: DateTime<Utc>,
    pub size: usize,
    pub md5_hash: String,
    pub media_link: String,
    pub content_encoding: String,
    pub content_disposition: String,
    pub content_language: String,
    pub cache_control: String,
    pub metadata: HashMap<String, String>,
    pub acl: Vec<ObjectAccessControls>,
    pub owner: ObjectOwner,
    pub crc32c: String,
    pub component_count: usize,
    pub etag: String,
    pub customer_encryption: CustomerEncryption,
    pub kms_key_name: String,
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
