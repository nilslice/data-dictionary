use std::collections::HashMap;
use std::env;

use crate::dict::{Classification, DatasetConfig};
use crate::error::Error;
use crate::gcp_client::GcpClient;
use crate::util;

use reqwest::{
    header::{HeaderMap, CONTENT_TYPE},
    Method, StatusCode,
};

pub struct BucketManager {
    service_endpoint: String,
    bucket_name_internal: String,
    bucket_name_public: String,
    bucket_name_restricted: String,
    bucket_name_confidential: String,
    client: GcpClient,
}

impl BucketManager {
    pub fn from_env(client: GcpClient) -> Self {
        // TODO: clean this up, it has become disgusting...
        Self {
            service_endpoint: env::var("DD_STORAGE_SERVICE")
                .expect("DD_STORAGE_SERVICE environment variable not set"),
            bucket_name_internal: env::var("DD_BUCKET_NAME_PRIVATE")
                .expect("DD_BUCKET_NAME_PRIVATE environment variable not set"),
            bucket_name_public: env::var("DD_BUCKET_NAME_PUBLIC")
                .expect("DD_BUCKET_NAME_PUBLIC environment variable not set"),
            bucket_name_restricted: env::var("DD_BUCKET_NAME_RESTRICTED")
                .expect("DD_BUCKET_NAME_RESTRICTED environment variable not set"),
            bucket_name_confidential: env::var("DD_BUCKET_NAME_CONFIDENTIAL")
                .expect("DD_BUCKET_NAME_CONFIDENTIAL environment variable not set"),
            client,
        }
    }

    pub async fn register_dataset(&self, config: &DatasetConfig) -> Result<(), Error> {
        let bucket = match config.classification {
            Classification::Internal => &self.bucket_name_internal,
            Classification::Public => &self.bucket_name_public,
            Classification::Restricted => &self.bucket_name_restricted,
            Classification::Confidential => &self.bucket_name_confidential,
        };
        let url = format!(
            "{}/upload/storage/v1/b/{}/o?uploadType=media&name={}",
            self.service_endpoint,
            bucket,
            format!("{}/{}", &config.name, util::FILENAME_DD_JSON),
        );
        let mut headers = HeaderMap::new();
        headers.insert(
            CONTENT_TYPE,
            "application/json"
                .parse()
                .expect("failed to parse content-type header from sting"),
        );
        let data = serde_json::to_vec(config).map_err(|e| Error::Generic(Box::new(e)))?;
        let resp = self
            .client
            .request(Method::POST, &url)?
            .body(data)
            .send()
            .await
            .map_err(|e| Error::Generic(Box::new(e)))?;

        match resp.status() {
            StatusCode::OK => Ok(()),
            StatusCode::FORBIDDEN => {
                let msg = "forbidden: invalid credentials for GCP bucket manager".into();
                log::error!("{}", &msg);
                Err(Error::Auth(msg))
            }
            StatusCode::NOT_FOUND => {
                let msg = format!(
                    "failed to access storage endpoint '{}': {}",
                    &url,
                    resp.status()
                );
                log::error!("{}", &msg);
                Err(Error::Http(msg))
            }
            _ => {
                let msg = format!(
                    "failed to access GCP bucket, status code: {}",
                    resp.status()
                );
                log::error!("{}", &msg);
                Err(Error::Http(msg))
            }
        }
    }
}

impl From<&BucketManager> for HashMap<Classification, String> {
    fn from(bm: &BucketManager) -> Self {
        let mut buckets = HashMap::new();
        buckets.insert(Classification::Internal, bm.bucket_name_internal.clone());
        buckets.insert(Classification::Public, bm.bucket_name_public.clone());
        buckets.insert(
            Classification::Confidential,
            bm.bucket_name_confidential.clone(),
        );
        buckets.insert(Classification::Restricted, bm.bucket_name_restricted.clone());
        buckets
    }
}
