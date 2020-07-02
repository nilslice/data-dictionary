use std::env;

use crate::db::range_query;
use crate::db::sql;
use crate::dict::{
    Classification, Compression, Dataset, DatasetSchema, Format, Manager, Partition, RangeParams,
    PARTITION_LATEST,
};
use crate::error::Error;
use crate::service::DataService;

use argon2rs;
use async_trait::async_trait;
use bb8_postgres::{bb8::Pool, PostgresConnectionManager};
use log;
use postgres_types::ToSql;
use rand::Rng;
use tokio_postgres::{row::Row, NoTls};
use uuid::Uuid;

pub mod migrate {
    use refinery::embed_migrations as embed;
    embed!("migrations");
}

pub const CHARACTER_SET: &str = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

pub fn rand(length: usize, using: String) -> String {
    let mut rng = rand::thread_rng();
    let mut output = String::new();
    for _ in 0..length {
        let i = rng.gen_range(0, using.len());
        output.push(using.as_bytes()[i] as char);
    }

    output
}

#[test]
fn test_rand() {
    let mut last = String::new();
    for _ in 0..200 {
        let current = rand(32, CHARACTER_SET.into());
        assert!(current.len() == 32);
        assert!(last != current);
        last = current;
    }
}

type DbPool = Pool<PostgresConnectionManager<NoTls>>;

#[derive(Clone)]
pub struct Db {
    pub client: DbPool,
}

pub struct PoolConfig {
    pub min_idle: u32,
    pub max_size: u32,
}

impl Db {
    pub async fn connect(params: Option<String>, cfg: Option<PoolConfig>) -> Result<Self, Error> {
        Ok(Db {
            client: Db::create_pool(params, cfg).await?,
        })
    }

    pub async fn migrate(&mut self) -> Result<(), Error> {
        migrate::migrations::runner()
            .run_async(&mut *self.client.get().await?)
            .await
            .map(|_| ())
            .map_err(|e| Error::Generic(Box::new(e)))
    }

    pub async fn create_pool(
        params: Option<String>,
        cfg: Option<PoolConfig>,
    ) -> Result<DbPool, Error> {
        let manager = PostgresConnectionManager::new_from_stringlike(
            &params.unwrap_or_else(|| {
                env::var("DD_DATABASE_PARAMS")
                    .unwrap_or_else(|_| "host=127.0.0.1 user=postgres port=5432".into())
            }),
            NoTls,
        )?;

        let mut min_idle: u32 = 1;
        let mut max_size: u32 = 1;

        if let Some(pool_cfg) = cfg {
            min_idle = pool_cfg.min_idle;
            max_size = pool_cfg.max_size;
        }

        Pool::builder()
            .min_idle(Some(min_idle))
            .max_size(max_size)
            .build(manager)
            .await
            .map_err(|e| Error::Generic(Box::new(e)))
    }
}

impl From<&Row> for Dataset {
    fn from(row: &Row) -> Self {
        Self {
            id: row.get("dataset_id"),
            name: row.get("dataset_name"),
            manager_id: row.get("manager_id"),
            classification: row.get("dataset_classification"),
            compression: row.get("dataset_compression"),
            format: row.get("dataset_format"),
            description: row.get("dataset_desc"),
            schema: row.get("dataset_schema"),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
        }
    }
}

impl From<Row> for Dataset {
    fn from(row: Row) -> Self {
        Self {
            id: row.get("dataset_id"),
            name: row.get("dataset_name"),
            manager_id: row.get("manager_id"),
            classification: row.get("dataset_classification"),
            compression: row.get("dataset_compression"),
            format: row.get("dataset_format"),
            description: row.get("dataset_desc"),
            schema: row.get("dataset_schema"),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
        }
    }
}

impl From<&Row> for Partition {
    fn from(row: &Row) -> Self {
        Self {
            id: row.get("partition_id"),
            name: row.get("partition_name"),
            url: row.get("partition_url"),
            size: row.get("partition_size"),
            dataset_id: row.get("dataset_id"),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
        }
    }
}

impl From<Row> for Partition {
    fn from(row: Row) -> Self {
        Self {
            id: row.get("partition_id"),
            name: row.get("partition_name"),
            url: row.get("partition_url"),
            size: row.get("partition_size"),
            dataset_id: row.get("dataset_id"),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
        }
    }
}

impl From<Row> for Manager {
    fn from(row: Row) -> Self {
        Self {
            id: row.get("manager_id"),
            email: row.get("manager_email"),
            api_key: row.get("api_key"),
            admin: row.get("is_admin"),
            salt: row.get("manager_salt"),
            hash: row.get("manager_hash"),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
        }
    }
}

impl From<&Row> for Manager {
    fn from(row: &Row) -> Self {
        Self {
            id: row.get("manager_id"),
            email: row.get("manager_email"),
            api_key: row.get("api_key"),
            admin: row.get("is_admin"),
            salt: row.get("manager_salt"),
            hash: row.get("manager_hash"),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
        }
    }
}
#[derive(Debug)]
enum PartitionQuery {
    Named,
    Latest,
}

fn un_send(s: &(dyn ToSql + Sync + Send)) -> Box<&(dyn ToSql + Sync)> {
    Box::new(s)
}

#[async_trait]
impl DataService for Db {
    async fn register_dataset(
        &mut self,
        manager: &Manager,
        name: &str,
        compression: Compression,
        format: Format,
        classification: Classification,
        schema: DatasetSchema,
        description: &str,
    ) -> Result<Dataset, Error> {
        Ok(self
            .client
            .get()
            .await?
            .query_one(
                sql::REGISTER_DATASET,
                &[
                    &name,
                    &manager.id,
                    &compression,
                    &format,
                    &classification,
                    &schema,
                    &description,
                ],
            )
            .await?
            .into())
    }

    async fn find_dataset(&mut self, name: &str) -> Result<Dataset, Error> {
        Ok(self
            .client
            .get()
            .await?
            .query_one(sql::FIND_DATASET, &[&name])
            .await?
            .into())
    }

    async fn list_datasets(&mut self) -> Result<Vec<Dataset>, Error> {
        Ok(self
            .client
            .get()
            .await?
            .query(sql::LIST_DATASETS, &[])
            .await?
            .iter()
            .map(Dataset::from)
            .collect())
    }

    async fn delete_dataset(&mut self, dataset: &Dataset) -> Result<(), Error> {
        self.client
            .get()
            .await?
            .execute(sql::DELETE_DATASET, &[&dataset.name])
            .await
            .map(|_| ())
            .map_err(|e| Error::Generic(Box::new(e)))
    }

    async fn register_partition(
        &mut self,
        dataset: &Dataset,
        partition_name: &str,
        partition_url: &str,
        partition_size: i64,
    ) -> Result<Partition, Error> {
        if partition_name == PARTITION_LATEST {
            log::error!(
                "attempt to register partition with name 'latest' for dataset name={} id={}",
                dataset.name,
                dataset.id
            );

            return Err(Error::InputValidation(
                "cannot use reserved name 'latest' for partition".into(),
            ));
        }

        Ok(self
            .client
            .get()
            .await?
            .query_one(
                sql::REGISTER_PARTITION,
                &[
                    &partition_name,
                    &partition_url,
                    &partition_size,
                    &dataset.id,
                ],
            )
            .await?
            .into())
    }

    async fn delete_partition(
        &mut self,
        dataset: &Dataset,
        partition_name: &str,
    ) -> Result<(), Error> {
        self.client
            .get()
            .await?
            .execute(sql::DELETE_PARTITION, &[&dataset.id, &partition_name])
            .await
            .map(|_| ())
            .map_err(|e| Error::Generic(Box::new(e)))
    }

    async fn find_partition(
        &mut self,
        dataset: &Dataset,
        partition_name: &str,
    ) -> Result<Partition, Error> {
        let mut sql_querytype: (&str, PartitionQuery) =
            (sql::FIND_PARTITION, PartitionQuery::Named);

        if partition_name == PARTITION_LATEST {
            sql_querytype.0 = sql::FIND_PARTITION_LATEST;
            sql_querytype.1 = PartitionQuery::Latest;
        }

        match sql_querytype.1 {
            PartitionQuery::Named => Ok(self
                .client
                .get()
                .await?
                .query_one(sql_querytype.0, &[&partition_name, &dataset.id])
                .await?
                .into()),
            PartitionQuery::Latest => Ok(self
                .client
                .get()
                .await?
                .query_one(sql_querytype.0, &[&dataset.id])
                .await?
                .into()),
        }
    }

    async fn range_partitions(
        &mut self,
        dataset: &Dataset,
        params: &RangeParams,
    ) -> Result<Vec<Partition>, Error> {
        let (query, boxed_bindvars) = range_query::partitions(params);
        let mut bindvars = boxed_bindvars
            .iter()
            .map(|v| v.as_ref())
            .collect::<Vec<&(dyn ToSql + Sync + Send)>>();
        // prepend the dataset id to the bind vars, since it is used for all range queries
        bindvars.insert(0, &dataset.id);

        let bindvars: Vec<&(dyn ToSql + Sync)> =
            bindvars.iter().map(|v| *un_send(*v).as_ref()).collect();

        Ok(self
            .client
            .get()
            .await?
            .query(&query as &str, &bindvars[..])
            .await?
            .iter()
            .map(Partition::from)
            .collect())
    }

    async fn list_partitions(&mut self, dataset: &Dataset) -> Result<Vec<Partition>, Error> {
        Ok(self
            .client
            .get()
            .await?
            .query(sql::LIST_PARTITIONS, &[&dataset.id])
            .await?
            .iter()
            .map(Partition::from)
            .collect())
    }

    async fn register_manager(&mut self, email: &str, password: &str) -> Result<Manager, Error> {
        match env::var("DD_MANAGER_EMAIL_DOMAIN") {
            Ok(domain) => {
                if !email.contains(&format!("@{}", domain)) {
                    return Err(Error::InputValidation(format!(
                        "invalid email pattern, must be <user>@{} address",
                        domain
                    )));
                }
            }
            Err(e) => log::error!(
                "skipping manager email domain validation, 'DD_MANAGER_EMAIL_DOMAIN' {}",
                e
            ),
        }

        let salt = rand(32, CHARACTER_SET.into());
        let hash = argon2rs::argon2d_simple(&password, &salt).to_vec();
        let api_key = Uuid::new_v4();
        Ok(self
            .client
            .get()
            .await?
            .query_one(sql::REGISTER_MANAGER, &[&email, &hash, &salt, &api_key])
            .await?
            .into())
    }

    async fn find_manager(&mut self, api_key: &Uuid) -> Result<Manager, Error> {
        Ok(self
            .client
            .get()
            .await?
            .query_one(sql::FIND_MANAGER, &[api_key])
            .await?
            .into())
    }

    async fn auth_manager(&mut self, email: &str, password: &str) -> Result<Manager, Error> {
        let manager: Manager = self
            .client
            .get()
            .await?
            .query_one(sql::AUTH_MANAGER, &[&email])
            .await?
            .into();

        // validate that the password provided is the same as our stored value
        let hash = argon2rs::argon2d_simple(&password, &manager.salt);
        if hash != manager.hash.as_slice() {
            Err(Error::Auth(format!("invalid credentials for '{}'", email)))
        } else {
            Ok(manager)
        }
    }

    async fn manager_datasets(&mut self, api_key: &Uuid) -> Result<Vec<Dataset>, Error> {
        Ok(self
            .client
            .get()
            .await?
            .query(sql::MANAGED_DATASETS, &[api_key])
            .await?
            .iter()
            .map(Dataset::from)
            .collect())
    }
}
