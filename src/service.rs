use crate::dict::{
    Classification, Compression, Dataset, Encoding, Manager, Partition, RangeParams,
};
use crate::error::Error;

use uuid::Uuid;

pub trait DataService {
    fn register_dataset(
        &mut self,
        manager: &Manager,
        name: impl AsRef<str>,
        compression: Compression,
        encoding: Encoding,
        classification: Classification,
        description: impl AsRef<str>,
    ) -> Result<Dataset, Error>;

    fn find_dataset(&mut self, name: impl AsRef<str>) -> Result<Dataset, Error>;

    fn list_datasets(&mut self) -> Result<Vec<Dataset>, Error>;

    fn register_partition(
        &mut self,
        dataset: &Dataset,
        parition_name: impl AsRef<str>,
    ) -> Result<Partition, Error>;

    fn find_partition(
        &mut self,
        dataset: &Dataset,
        partition_name: impl AsRef<str>,
    ) -> Result<Partition, Error>;

    fn range_partitions(
        self,
        dataset: &Dataset,
        params: &RangeParams,
    ) -> Result<Vec<Partition>, Error>;

    fn list_partitions(&mut self, dataset: &Dataset) -> Result<Vec<Partition>, Error>;

    fn register_manager(
        &mut self,
        email: impl AsRef<str>,
        password: impl AsRef<str>,
    ) -> Result<Manager, Error>;

    fn find_manager(&mut self, api_key: &Uuid) -> Result<Manager, Error>;

    fn auth_manager(
        &mut self,
        email: impl AsRef<str>,
        password: impl AsRef<str>,
    ) -> Result<Manager, Error>;

    fn manager_datasets(&mut self, api_key: &Uuid) -> Result<Vec<Dataset>, Error>;
}
