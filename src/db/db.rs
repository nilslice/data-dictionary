use std::env;

use crate::db::range_query;
use crate::db::sql;
use crate::dict::{
    Classification, Compression, Dataset, DatasetSchema, Encoding, Manager, Partition, RangeParams,
    PARTITION_LATEST,
};
use crate::error::Error;
use crate::service::DataService;

use argon2rs;
use async_trait::async_trait;
use log::error;
use postgres_types::ToSql;
use rand::Rng;
use tokio;
use tokio_postgres::{row::Row, Client, NoTls};
use uuid::Uuid;

pub mod migrate {
    use refinery::embed_migrations as embed;
    embed!("migrations");
}

pub struct Db {
    pub client: Client,
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

impl Db {
    pub async fn connect(params: Option<String>) -> Result<Self, Error> {
        let (client, conn) = tokio_postgres::connect(
            &params.unwrap_or_else(|| {
                env::var("DD_DATABASE_PARAMS")
                    .unwrap_or_else(|_| "host=127.0.0.1 user=postgres port=5432".into())
            }),
            NoTls,
        )
        .await?;
        tokio::spawn(async move {
            if let Err(e) = conn.await {
                eprintln!("connection error: {}", e);
            }
        });

        Ok(Db { client })
    }

    pub async fn migrate(&mut self) -> Result<(), Error> {
        match migrate::migrations::runner()
            .run_async(&mut self.client)
            .await
        {
            Err(e) => Err(Error::Generic(Box::new(e))),
            _ => Ok(()),
        }
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
            encoding: row.get("dataset_encoding"),
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
            encoding: row.get("dataset_encoding"),
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
        encoding: Encoding,
        classification: Classification,
        schema: DatasetSchema,
        description: &str,
    ) -> Result<Dataset, Error> {
        let stmt = self.client.prepare(sql::REGISTER_DATASET).await?;
        Ok(self
            .client
            .query_one(
                &stmt,
                &[
                    &name,
                    &manager.id,
                    &compression,
                    &encoding,
                    &classification,
                    &schema,
                    &description,
                ],
            )
            .await?
            .into())
    }

    async fn find_dataset(&mut self, name: &str) -> Result<Dataset, Error> {
        let stmt = self.client.prepare(sql::FIND_DATASET).await?;
        Ok(self.client.query_one(&stmt, &[&name]).await?.into())
    }

    async fn list_datasets(&mut self) -> Result<Vec<Dataset>, Error> {
        let stmt = self.client.prepare(sql::LIST_DATASETS).await?;
        Ok(self
            .client
            .query(&stmt, &[])
            .await?
            .iter()
            .map(Dataset::from)
            .collect())
    }

    async fn delete_dataset(&mut self, dataset: &Dataset) -> Result<(), Error> {
        let stmt = self.client.prepare(sql::DELETE_DATASET).await?;
        self.client
            .execute(&stmt, &[&dataset.name])
            .await
            .map(|_| ())
            .map_err(|e| Error::Generic(Box::new(e)))
    }

    async fn register_partition(
        &mut self,
        dataset: &Dataset,
        partition_name: &str,
        partition_url: &str,
    ) -> Result<Partition, Error> {
        if partition_name == PARTITION_LATEST {
            error!(
                "attempt to register partition with name 'latest' for dataset name={} id={}",
                dataset.name, dataset.id
            );

            return Err(Error::InputValidation(
                "cannot use reserved name 'latest' for partition".into(),
            ));
        }

        let stmt = self.client.prepare(sql::REGISTER_PARTITION).await?;
        Ok(self
            .client
            .query_one(&stmt, &[&partition_name, &partition_url, &dataset.id])
            .await?
            .into())
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

        let stmt = self.client.prepare(sql_querytype.0).await?;
        match sql_querytype.1 {
            PartitionQuery::Named => Ok(self
                .client
                .query_one(&stmt, &[&partition_name, &dataset.id])
                .await?
                .into()),
            PartitionQuery::Latest => {
                Ok(self.client.query_one(&stmt, &[&dataset.id]).await?.into())
            }
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
            .query(&query as &str, &bindvars[..])
            .await?
            .iter()
            .map(Partition::from)
            .collect())
    }

    async fn list_partitions(&mut self, dataset: &Dataset) -> Result<Vec<Partition>, Error> {
        let stmt = self.client.prepare(sql::LIST_PARTITIONS).await?;
        Ok(self
            .client
            .query(&stmt, &[&dataset.id])
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
            Err(e) => error!(
                "skipping manager email domain validation, 'DD_MANAGER_EMAIL_DOMAIN' {}",
                e
            ),
        }

        let salt = rand(32, CHARACTER_SET.into());
        let hash = argon2rs::argon2d_simple(&password, &salt).to_vec();
        let api_key = Uuid::new_v4();
        let stmt = self.client.prepare(sql::REGISTER_MANAGER).await?;

        Ok(self
            .client
            .query_one(&stmt, &[&email, &hash, &salt, &api_key])
            .await?
            .into())
    }

    async fn find_manager(&mut self, api_key: &Uuid) -> Result<Manager, Error> {
        let stmt = self.client.prepare(sql::FIND_MANAGER).await?;
        Ok(self.client.query_one(&stmt, &[api_key]).await?.into())
    }

    async fn auth_manager(&mut self, email: &str, password: &str) -> Result<Manager, Error> {
        let stmt = self.client.prepare(sql::AUTH_MANAGER).await?;
        let manager: Manager = self.client.query_one(&stmt, &[&email]).await?.into();

        // validate that the password provided is the same as our stored value
        let hash = argon2rs::argon2d_simple(&password, &manager.salt);
        if hash != manager.hash.as_slice() {
            Err(Error::Auth(format!("invalid credentials for '{}'", email)))
        } else {
            Ok(manager)
        }
    }

    async fn manager_datasets(&mut self, api_key: &Uuid) -> Result<Vec<Dataset>, Error> {
        let stmt = self.client.prepare(sql::MANAGED_DATASETS).await?;
        Ok(self
            .client
            .query(&stmt, &[api_key])
            .await?
            .iter()
            .map(Dataset::from)
            .collect())
    }
}
