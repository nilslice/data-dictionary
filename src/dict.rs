use crate::error::Error;
use crate::service::DataService;

use chrono::{DateTime, Utc};
use log::info;
use postgres_types::{FromSql, ToSql};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Defines the special partition name, "latest", which is always the most recently updated
/// partition per dataset.
pub const PARTITION_LATEST: &str = "latest";

/// A Manager is the person or team responsible for the creation and maintenance of one or many
/// datasets. Manager can be an admin, and thus able to modify any dataset.
#[derive(Debug, Serialize)]
pub struct Manager {
    pub id: i32,
    pub email: String,
    pub api_key: Uuid,
    pub admin: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(skip_serializing)]
    pub salt: String,
    #[serde(skip_serializing)]
    pub hash: Vec<u8>,
}

/// The implementation wraps most DataService trait methods, which are meant to be abstracted over a
/// concrete data source. For local development and testing, a local, mocked, or in-memory database
/// may be used, compared with a remote database server when deployed.
impl Manager {
    /// Inserts a manager record into the database, where the `email` field must be unique.
    pub async fn register(
        svc: &mut impl DataService,
        email: impl AsRef<str>,
        password: impl AsRef<str>,
    ) -> Result<Manager, Error> {
        info!("registering manager: {}", email.as_ref());
        svc.register_manager(email.as_ref(), password.as_ref())
            .await
    }

    /// Retrieves a manager record from the database, if one is found.
    pub async fn find(svc: &mut impl DataService, api_key: Uuid) -> Result<Manager, Error> {
        info!("finding manager by api key: {}", api_key);
        svc.find_manager(&api_key).await
    }

    /// Validates that a manager's credentials are valid.
    pub async fn authenticate(
        svc: &mut impl DataService,
        email: impl AsRef<str>,
        password: impl AsRef<str>,
    ) -> Result<Manager, Error> {
        info!("authenticating manager: {}", email.as_ref());
        svc.auth_manager(email.as_ref(), password.as_ref()).await
    }

    /// Inserts a dataset record into the database, where the `name` field must be unique.
    pub async fn register_dataset(
        &self,
        svc: &mut impl DataService,
        name: impl AsRef<str>,
        compression: Compression,
        format: Format,
        classification: Classification,
        schema: DatasetSchema,
        description: impl AsRef<str>,
    ) -> Result<Dataset, Error> {
        info!(
            "registering dataset '{}' by manager: {}",
            name.as_ref(),
            self.api_key
        );
        svc.register_dataset(
            &self,
            name.as_ref(),
            compression,
            format,
            classification,
            schema,
            description.as_ref(),
        )
        .await
    }

    /// Retrieves all datasets managed by the current manager.
    pub async fn datasets(&self, svc: &mut impl DataService) -> Result<Vec<Dataset>, Error> {
        info!("listing datasets managed by: {}", self.api_key);
        svc.manager_datasets(&self.api_key).await
    }
}

/// A Format is used to indicate the data format within the file(s).
#[derive(Debug, FromSql, ToSql, Serialize, Deserialize, Clone)]
#[postgres(name = "format_t")]
pub enum Format {
    #[postgres(name = "plaintext")]
    #[serde(rename = "plaintext")]
    PlainText,
    #[postgres(name = "json")]
    #[serde(rename = "json")]
    Json,
    #[postgres(name = "ndjson")]
    #[serde(rename = "ndjson")]
    NdJson,
    #[postgres(name = "csv")]
    #[serde(rename = "csv")]
    Csv,
    #[postgres(name = "tsv")]
    #[serde(rename = "tsv")]
    Tsv,
    #[postgres(name = "protobuf")]
    #[serde(rename = "protobuf")]
    Protobuf,
}

impl std::fmt::Display for Format {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", format!("{:?}", self).to_lowercase())
    }
}

#[test]
fn test_display_encoding() {
    assert_eq!("plaintext", format!("{}", Format::PlainText));
    assert_eq!("json", format!("{}", Format::Json));
    assert_eq!("ndjson", format!("{}", Format::NdJson));
    assert_eq!("csv", format!("{}", Format::Csv));
    assert_eq!("tsv", format!("{}", Format::Tsv));
    assert_eq!("protobuf", format!("{}", Format::Protobuf));
}

pub trait FileExt {
    fn to_ext(&self) -> &str;
}

impl FileExt for Format {
    fn to_ext(&self) -> &str {
        match self {
            Format::PlainText => "txt",
            Format::Json => "json",
            Format::NdJson => "ndjson",
            Format::Csv => "csv",
            Format::Tsv => "tsv",
            Format::Protobuf => "pb",
        }
    }
}

/// A Compression is used to indicate the type of compression used (if any) within the file(s).
#[derive(Debug, FromSql, ToSql, Serialize, Deserialize, Clone)]
#[postgres(name = "compression_t")]
pub enum Compression {
    #[postgres(name = "uncompressed")]
    #[serde(rename = "uncompressed")]
    Uncompressed,
    #[postgres(name = "zip")]
    #[serde(rename = "zip")]
    Zip,
    #[postgres(name = "tar")]
    #[serde(rename = "tar")]
    Tar,
}

impl std::fmt::Display for Compression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", format!("{:?}", self).to_lowercase())
    }
}

#[test]
fn test_display_compression() {
    assert_eq!("uncompressed", format!("{}", Compression::Uncompressed));
    assert_eq!("zip", format!("{}", Compression::Zip));
    assert_eq!("tar", format!("{}", Compression::Tar));
}

impl FileExt for Compression {
    fn to_ext(&self) -> &str {
        match self {
            Compression::Uncompressed => "",
            Compression::Tar => "tar.gz",
            Compression::Zip => "zip",
        }
    }
}

/// A Classification is used to indicate the level of security needed to protect datasets.
#[derive(Debug, FromSql, ToSql, Serialize, Deserialize, Clone)]
#[postgres(name = "classification_t")]
pub enum Classification {
    #[postgres(name = "confidential")]
    #[serde(rename = "confidential")]
    Confidential,
    #[postgres(name = "sensitive")]
    #[serde(rename = "sensitive")]
    Sensitive,
    #[postgres(name = "private")]
    #[serde(rename = "private")]
    Private,
    #[postgres(name = "public")]
    #[serde(rename = "public")]
    Public,
}

impl std::fmt::Display for Classification {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", format!("{:?}", self).to_lowercase())
    }
}

#[test]
fn test_display_classification() {
    assert_eq!("confidential", format!("{}", Classification::Confidential));
    assert_eq!("sensitive", format!("{}", Classification::Sensitive));
    assert_eq!("private", format!("{}", Classification::Private));
    assert_eq!("public", format!("{}", Classification::Public));
}

/// A DatasetSchema is the "schema" key found in a dd.json config file
pub type DatasetSchema = std::collections::HashMap<String, Option<String>>;

/// A Dataset is the parent node of partitions, where each dataset is split up into one or many
/// partitions, typically based on date or size.
#[derive(Debug, Serialize, Deserialize)]
pub struct Dataset {
    pub id: i32,
    pub manager_id: i32,
    pub name: String,
    pub classification: Classification,
    pub compression: Compression,
    pub format: Format,
    pub description: String,
    pub schema: DatasetSchema,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

// DatasetConfig is the dd.json file
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DatasetConfig {
    pub name: String,
    pub classification: Classification,
    pub compression: Compression,
    pub format: Format,
    pub description: String,
    pub schema: DatasetSchema,
}

/// concrete data source. For local development and testing, a local, mocked, or in-memory database
/// may be used, compared with a remote database server when deployed.
impl Dataset {
    /// Retrieves a Dataset record from the database, if one is found.
    pub async fn find(svc: &mut impl DataService, name: impl AsRef<str>) -> Result<Dataset, Error> {
        info!("finding dataset: {}", name.as_ref());
        svc.find_dataset(name.as_ref()).await
    }

    /// Searches for a Dataset based on the term provided.
    pub async fn search(
        svc: &mut impl DataService,
        term: impl AsRef<str>,
    ) -> Result<Vec<Dataset>, Error> {
        info!("searching datasets for: {}", term.as_ref());
        svc.search_datasets(term.as_ref()).await
    }

    /// Retrieves all datasets from the database.
    pub async fn list(
        svc: &mut impl DataService,
        params: Option<RangeParams>,
    ) -> Result<Vec<Dataset>, Error> {
        if let Some(params) = params {
            info!(
                "listing datasets for specified range start: {:?} to end: {:?}, count: {:?}, offset: {:?}",
                params.start, params.end, params.count, params.offset,
            );
        } else {
            info!("listing all datasets");
        }
        svc.list_datasets(params).await
    }

    pub async fn delete(self, svc: &mut impl DataService) -> Result<(), Error> {
        info!("deleting dataset '{}' and its partitions", self.name);
        svc.delete_dataset(&self).await
    }

    /// Inserts a partition into the database, using the current dataset as its reference.
    pub async fn register_partition(
        &self,
        svc: &mut impl DataService,
        name: impl AsRef<str>,
        url: impl AsRef<str>,
        size: i64,
    ) -> Result<Partition, Error> {
        info!(
            "registering partition '{}' for dataset: {}",
            name.as_ref(),
            &self.name
        );
        svc.register_partition(&self, name.as_ref(), url.as_ref(), size)
            .await
    }

    pub async fn delete_partition(
        &self,
        svc: &mut impl DataService,
        name: impl AsRef<str>,
    ) -> Result<(), Error> {
        info!(
            "deleting partition '{}' for dataset: {}",
            name.as_ref(),
            &self.name
        );
        svc.delete_partition(&self, name.as_ref()).await
    }

    /// Retrieves a partition based on the name provided, within the current dataset.
    pub async fn partition(
        &self,
        svc: &mut impl DataService,
        name: impl AsRef<str>,
    ) -> Result<Partition, Error> {
        info!(
            "finding partition '{}' for dataset: {}",
            name.as_ref(),
            &self.name
        );
        svc.find_partition(&self, name.as_ref()).await
    }

    /// Retrieves the "latest" partition for the current dataset.
    pub async fn latest_partition(&self, svc: &mut impl DataService) -> Result<Partition, Error> {
        self.partition(svc, PARTITION_LATEST).await
    }

    /// Retrieves a set of partitions based on the range paramaters provided, optionally using any
    /// combination of start/end times, result count, and offset values.
    pub async fn partitions(
        &self,
        svc: &mut impl DataService,
        params: Option<RangeParams>,
    ) -> Result<Vec<Partition>, Error> {
        if let Some(params) = params {
            info!(
                "listing partitions for specified range start: {:?} to end: {:?}, count: {:?}, offset: {:?} in dataset: {}",
                params.start, params.end, params.count, params.offset, self.id,
            );
        } else {
            info!("listing all partitions for dataset: {}", self.id);
        }

        svc.list_partitions(&self, params).await
    }
}

/// A Partition is a partial dataset, containing a subset of data. Each partition within a Dataset
/// must follow the same schema, compression, and format.
#[derive(Debug, PartialEq, Serialize)]
pub struct Partition {
    #[serde(rename(serialize = "partition_id"))]
    pub id: i32,
    #[serde(rename(serialize = "partition_name"))]
    pub name: String,
    #[serde(rename(serialize = "partition_url"))]
    pub url: String,
    #[serde(rename(serialize = "partition_size"))]
    pub size: i64,
    pub dataset_id: i32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Params specify how a Dataset's Partition results should be returned.
#[derive(Debug, Default, Clone, Copy)]
pub struct RangeParams {
    pub start: Option<DateTime<Utc>>,
    pub end: Option<DateTime<Utc>>,
    pub offset: Option<i32>,
    pub count: Option<i32>,
}
