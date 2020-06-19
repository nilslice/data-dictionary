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
use log::error;
use postgres::{row::Row, types::ToSql, Client, NoTls};
use rand::Rng;
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

// postgresql://<user>[:<password>]@<host>[:<port>]/<database>[?sslmode=<ssl-mode>[&sslcrootcert=<path>]]
impl Db {
    pub fn connect(params: Option<String>) -> Result<Self, Error> {
        let client = Client::connect(
            &params.unwrap_or(
                env::var("DD_DATABASE_PARAMS")
                    .unwrap_or("host=127.0.0.1 user=postgres port=5432".into()),
            ),
            NoTls,
        )?;
        Ok(Db { client })
    }

    pub fn migrate(&mut self) -> Result<(), Error> {
        match migrate::migrations::runner().run(&mut self.client) {
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

impl DataService for Db {
    fn register_dataset(
        &mut self,
        manager: &Manager,
        name: impl AsRef<str>,
        compression: Compression,
        encoding: Encoding,
        classification: Classification,
        schema: DatasetSchema,
        description: impl AsRef<str>,
    ) -> Result<Dataset, Error> {
        let stmt = self.client.prepare(sql::REGISTER_DATASET)?;
        Ok(self
            .client
            .query_one(
                &stmt,
                &[
                    &name.as_ref(),
                    &manager.id,
                    &compression,
                    &encoding,
                    &classification,
                    &schema,
                    &description.as_ref(),
                ],
            )?
            .into())
    }

    fn find_dataset(&mut self, name: impl AsRef<str>) -> Result<Dataset, Error> {
        let stmt = self.client.prepare(sql::FIND_DATASET)?;
        Ok(self.client.query_one(&stmt, &[&name.as_ref()])?.into())
    }

    fn list_datasets(&mut self) -> Result<Vec<Dataset>, Error> {
        let stmt = self.client.prepare(sql::LIST_DATASETS)?;
        Ok(self
            .client
            .query(&stmt, &[])?
            .iter()
            .map(|r| Dataset::from(r))
            .collect())
    }

    fn register_partition(
        &mut self,
        dataset: &Dataset,
        partition_name: impl AsRef<str>,
        partition_url: impl AsRef<str>,
    ) -> Result<Partition, Error> {
        if partition_name.as_ref() == PARTITION_LATEST {
            error!(
                "attempt to register partition with name 'latest' for dataset name={} id={}",
                dataset.name, dataset.id
            );

            return Err(Error::InputValidation(
                "cannot use reserved name 'latest' for partition".into(),
            ));
        }

        let stmt = self.client.prepare(sql::REGISTER_PARTITION)?;
        Ok(self
            .client
            .query_one(
                &stmt,
                &[
                    &partition_name.as_ref(),
                    &partition_url.as_ref(),
                    &dataset.id,
                ],
            )?
            .into())
    }

    fn find_partition(
        &mut self,
        dataset: &Dataset,
        partition_name: impl AsRef<str>,
    ) -> Result<Partition, Error> {
        let mut sql_querytype: (&str, PartitionQuery) =
            (sql::FIND_PARTITION, PartitionQuery::Named);

        if partition_name.as_ref() == PARTITION_LATEST {
            sql_querytype.0 = sql::FIND_PARTITION_LATEST;
            sql_querytype.1 = PartitionQuery::Latest;
        }

        let stmt = self.client.prepare(sql_querytype.0)?;
        match sql_querytype.1 {
            PartitionQuery::Named => Ok(self
                .client
                .query_one(&stmt, &[&partition_name.as_ref(), &dataset.id])?
                .into()),
            PartitionQuery::Latest => Ok(self.client.query_one(&stmt, &[&dataset.id])?.into()),
        }
    }

    fn range_partitions(
        &mut self,
        dataset: &Dataset,
        params: &RangeParams,
    ) -> Result<Vec<Partition>, Error> {
        let (query, boxed_bindvars) = range_query::partitions(params);
        let mut bindvars = boxed_bindvars
            .iter()
            .map(|v| v.as_ref())
            .collect::<Vec<&(dyn ToSql + Sync)>>();
        // prepend the dataset id to the bind vars, since it is used for all range queries
        bindvars.insert(0, &dataset.id);

        Ok(self
            .client
            .query(&query as &str, &bindvars)?
            .iter()
            .map(|r| Partition::from(r))
            .collect())
    }

    fn list_partitions(&mut self, dataset: &Dataset) -> Result<Vec<Partition>, Error> {
        let stmt = self.client.prepare(sql::LIST_PARTITIONS)?;
        Ok(self
            .client
            .query(&stmt, &[&dataset.id])?
            .iter()
            .map(|r| Partition::from(r))
            .collect())
    }

    fn register_manager(
        &mut self,
        email: impl AsRef<str>,
        password: impl AsRef<str>,
    ) -> Result<Manager, Error> {
        match env::var("DD_MANAGER_EMAIL_DOMAIN") {
            Ok(domain) => {
                if !email.as_ref().contains(&format!("@{}", domain)) {
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
        let hash = argon2rs::argon2d_simple(password.as_ref(), &salt).to_vec();
        let api_key = Uuid::new_v4();
        let stmt = self.client.prepare(sql::REGISTER_MANAGER)?;

        Ok(self
            .client
            .query_one(&stmt, &[&email.as_ref(), &hash, &salt, &api_key])?
            .into())
    }

    fn find_manager(&mut self, api_key: &Uuid) -> Result<Manager, Error> {
        let stmt = self.client.prepare(sql::FIND_MANAGER)?;
        Ok(self.client.query_one(&stmt, &[api_key])?.into())
    }

    fn auth_manager(
        &mut self,
        email: impl AsRef<str>,
        password: impl AsRef<str>,
    ) -> Result<Manager, Error> {
        let stmt = self.client.prepare(sql::AUTH_MANAGER)?;
        let manager: Manager = self.client.query_one(&stmt, &[&email.as_ref()])?.into();

        // validate that the password provided is the same as our stored value
        let hash = argon2rs::argon2d_simple(password.as_ref(), &manager.salt);
        match hash != manager.hash.as_ref() {
            true => Err(Error::Auth(format!(
                "invalid credentials for '{}'",
                email.as_ref()
            ))),
            false => Ok(manager),
        }
    }

    fn manager_datasets(&mut self, api_key: &Uuid) -> Result<Vec<Dataset>, Error> {
        let stmt = self.client.prepare(sql::MANAGED_DATASETS)?;
        Ok(self
            .client
            .query(&stmt, &[api_key])?
            .iter()
            .map(|r| Dataset::from(r))
            .collect())
    }
}
