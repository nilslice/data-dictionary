use crate::dict::{
    Classification, Compression, Dataset, DatasetSchema, Format, Manager, Partition, RangeParams,
};
use crate::error::Error;

use async_trait::async_trait;
use uuid::Uuid;

#[async_trait]
pub trait DataService {
    async fn register_dataset(
        &mut self,
        manager: &Manager,
        name: &str,
        compression: Compression,
        format: Format,
        classification: Classification,
        schema: DatasetSchema,
        description: &str,
    ) -> Result<Dataset, Error>;

    async fn find_dataset(&mut self, name: &str) -> Result<Dataset, Error>;

    async fn search_datasets(&mut self, term: &str) -> Result<Vec<Dataset>, Error>;

    async fn list_datasets(&mut self, params: Option<RangeParams>) -> Result<Vec<Dataset>, Error>;

    async fn delete_dataset(&mut self, dataset: &Dataset) -> Result<(), Error>;

    async fn register_partition(
        &mut self,
        dataset: &Dataset,
        partition_name: &str,
        partition_url: &str,
        partition_size: i64,
    ) -> Result<Partition, Error>;

    async fn delete_partition(
        &mut self,
        dataset: &Dataset,
        partition_name: &str,
    ) -> Result<(), Error>;

    async fn find_partition(
        &mut self,
        dataset: &Dataset,
        partition_name: &str,
    ) -> Result<Partition, Error>;

    async fn list_partitions(
        &mut self,
        dataset: &Dataset,
        params: Option<RangeParams>,
    ) -> Result<Vec<Partition>, Error>;

    async fn register_manager(&mut self, email: &str, password: &str) -> Result<Manager, Error>;

    async fn find_manager(&mut self, api_key: &Uuid) -> Result<Manager, Error>;

    async fn auth_manager(&mut self, email: &str, password: &str) -> Result<Manager, Error>;

    async fn manager_datasets(&mut self, api_key: &Uuid) -> Result<Vec<Dataset>, Error>;
}
