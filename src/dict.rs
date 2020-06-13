use crate::error::Error;
use crate::service::DataService;

use chrono::{DateTime, Utc};
use log::info;
use postgres_types::{FromSql, ToSql};

use uuid::Uuid;

/// Defines the special partition name, "latest", which is always the most recently updated
/// partition per dataset.
pub const PARTITION_LATEST: &str = "latest";

/// A Manager is the person or team responsible for the creation and maintenance of one or many
/// datasets. Manager can be an admin, and thus able to modify any dataset.
#[derive(Debug)]
pub struct Manager {
    pub id: i32,
    pub email: String,
    pub api_key: Uuid,
    pub admin: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
    pub salt: String,
    pub hash: Vec<u8>,
}

/// The implementation wraps most DataService trait methods, which are meant to be abstracted over a
/// concrete data source. For local development and testing, a local, mocked, or in-memory database
/// may be used, compared with a remote database server when deployed.
impl Manager {
    /// Inserts a manager record into the database, where the `email` field must be unique.
    pub fn register(
        svc: &mut impl DataService,
        email: impl AsRef<str>,
        password: impl AsRef<str>,
    ) -> Result<Manager, Error> {
        info!("registering manager: {}", email.as_ref());
        svc.register_manager(email.as_ref(), password.as_ref())
    }

    /// Retrieves a manager record from the database, if one is found.
    pub fn find(svc: &mut impl DataService, api_key: Uuid) -> Result<Manager, Error> {
        info!("finding manager by api key: {}", api_key);
        svc.find_manager(&api_key)
    }

    /// Validates that a manager's credentials are valid.
    pub fn authenticate(
        svc: &mut impl DataService,
        email: impl AsRef<str>,
        password: impl AsRef<str>,
    ) -> Result<Manager, Error> {
        info!("authenticating manager: {}", email.as_ref());
        svc.auth_manager(email.as_ref(), password.as_ref())
    }

    /// Inserts a dataset record into the database, where the `name` field must be unique.
    pub fn register_dataset(
        &self,
        svc: &mut impl DataService,
        name: impl AsRef<str>,
        compression: Compression,
        encoding: Encoding,
        classification: Classification,
        description: impl AsRef<str>,
    ) -> Result<Dataset, Error> {
        info!(
            "registering dataset '{}' by manager: {}",
            name.as_ref(),
            self.api_key
        );
        svc.register_dataset(
            &self,
            name,
            compression,
            encoding,
            classification,
            description,
        )
    }

    /// Retrieves all datasets managed by the current manager.
    pub fn datasets(&self, svc: &mut impl DataService) -> Result<Vec<Dataset>, Error> {
        info!("listing datasets managed by: {}", self.api_key);
        svc.manager_datasets(&self.api_key)
    }
}

/// An Encoding is used to indicate the data encoding within the file(s).
#[derive(Debug, FromSql, ToSql)]
#[postgres(name = "encoding_t")]
pub enum Encoding {
    #[postgres(name = "plaintext")]
    PlainText,
    #[postgres(name = "json")]
    Json,
    #[postgres(name = "ndjson")]
    NdJson,
    #[postgres(name = "csv")]
    Csv,
    #[postgres(name = "tsv")]
    Tsv,
    #[postgres(name = "protobuf")]
    Protobuf,
}

/// A Compression is used to indicate the type of compression used (if any) within the file(s).
#[derive(Debug, FromSql, ToSql)]
#[postgres(name = "compression_t")]
pub enum Compression {
    #[postgres(name = "uncompressed")]
    Uncompressed,
    #[postgres(name = "zip")]
    Zip,
    #[postgres(name = "tar")]
    Tar,
}

/// A Classification is used to indicate the level of security needed to protect datasets.
#[derive(Debug, FromSql, ToSql)]
#[postgres(name = "classification_t")]
pub enum Classification {
    #[postgres(name = "confidential")]
    Confidential,
    #[postgres(name = "sensitive")]
    Sensitive,
    #[postgres(name = "private")]
    Private,
    #[postgres(name = "public")]
    Public,
}

/// A Dataset is the parent node of partitions, where each dataset is split up into one or many
/// partitions, typically based on date or size.
#[derive(Debug)]
pub struct Dataset {
    pub id: i32,
    pub name: String,
    pub manager_id: i32,
    pub class: Classification,
    pub compression: Compression,
    pub encoding: Encoding,
    pub description: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
}

/// The implementation wraps most DataService trait methods, which are meant to be abstracted over a
/// concrete data source. For local development and testing, a local, mocked, or in-memory database
/// may be used, compared with a remote database server when deployed.
impl Dataset {
    /// Retrieves a Dataset record from the database, if one is found.
    pub fn find(svc: &mut impl DataService, name: impl AsRef<str>) -> Result<Dataset, Error> {
        info!("finding dataset: {}", name.as_ref());
        svc.find_dataset(name.as_ref())
    }

    /// Retrieves all datasets from the database.
    pub fn list(svc: &mut impl DataService) -> Result<Vec<Dataset>, Error> {
        info!("listing all datasets");
        svc.list_datasets()
    }

    /// Inserts a partition into the database, using the current dataset as its reference.
    pub fn register_partition(
        &self,
        svc: &mut impl DataService,
        name: impl AsRef<str>,
    ) -> Result<Partition, Error> {
        info!(
            "registering partition '{}' for dataset: {}",
            name.as_ref(),
            &self.name
        );
        svc.register_partition(&self, name.as_ref())
    }

    /// Retrieves a partition based on the name provided, within the current dataset.
    pub fn partition(
        &self,
        svc: &mut impl DataService,
        name: impl AsRef<str>,
    ) -> Result<Partition, Error> {
        info!(
            "finding partition '{}' for dataset: {}",
            name.as_ref(),
            &self.name
        );
        svc.find_partition(&self, name.as_ref())
    }

    /// Retrieves a set of partitions based on the range paramaters provided, optionally using any
    /// combination of start/end times, result count, and offset values.
    pub fn _partition_range(
        self,
        svc: impl DataService,
        params: &RangeParams,
    ) -> Result<Vec<Partition>, Error> {
        info!(
            "finding partitions for specified range {:?} to {:?}, count: {:?}, offset: {:?}",
            params.start, params.end, params.count, params.offset
        );
        svc.range_partitions(&self, params)
    }

    /// Retrieves the "latest" partition for the current dataset.
    pub fn latest_partition(&self, svc: &mut impl DataService) -> Result<Partition, Error> {
        self.partition(svc, PARTITION_LATEST)
    }

    /// Retrieves all partitions for the current dataset.
    pub fn partitions(&self, svc: &mut impl DataService) -> Result<Vec<Partition>, Error> {
        info!("listing all partitions for dataset: {}", &self.name);
        svc.list_partitions(&self)
    }
}

/// A Partition is a partial dataset, containing a subset of data. Each partition within a Dataset
/// must follow the same schema, compression, and encoding.
#[derive(Debug, PartialEq)]
pub struct Partition {
    pub id: i32,
    pub name: String,
    pub dataset_id: i32,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
}

/// Params specify how a Dataset's Partition results should be returned.
#[derive(Debug)]
pub struct RangeParams<'a> {
    pub start: Option<DateTime<Utc>>,
    pub end: Option<DateTime<Utc>>,
    pub offset: Option<&'a i32>,
    pub count: Option<&'a i32>,
}
