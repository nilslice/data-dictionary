use data_dictionary::db::{rand, Db, CHARACTER_SET};
use data_dictionary::dict::{Compression, Encoding};
use data_dictionary::error::Error;

pub struct TestDb {
    pub db: Db,
    schema: String,
}

impl Drop for TestDb {
    fn drop(&mut self) {
        self.db
            .client
            .simple_query(&format!("DROP SCHEMA IF EXISTS {} CASCADE;", self.schema))
            .expect("failed to drop test db and schema");
    }
}

mod migrate {
    use refinery::embed_migrations as embed;
    embed!("migrations");
}

pub fn reset_db(conn: &mut TestDb) -> Result<(), Error> {
    clear_db(conn)?;
    migrate_db(conn)?;
    Ok(())
}

pub fn migrate_db(conn: &mut TestDb) -> Result<(), Error> {
    migrate::migrations::runner()
        .run(&mut conn.db.client)
        .map_err(|e| Error::Generic(Box::new(e)))?;

    Ok(())
}

pub fn clear_db(conn: &mut TestDb) -> Result<(), Error> {
    conn.db
        .client
        .batch_execute(include_str!("sql/drop_all.sql"))
        .map_err(|e| Error::Generic(Box::new(e)))
}

pub fn new_test_db() -> Result<TestDb, Error> {
    let db = Db::connect(None)?;
    // create random schema name and use it for the current connection (used by single test)
    let mut test_db = TestDb {
        db,
        schema: get_rand(Rand::SchemaName),
    };
    test_db.db.client.batch_execute(&format!(
        "CREATE SCHEMA IF NOT EXISTS {}; SET search_path TO {};",
        test_db.schema, test_db.schema
    ))?;

    reset_db(&mut test_db)?;

    Ok(test_db)
}

fn rand_valid_email() -> String {
    format!(
        "{}@{}",
        rand(6, CHARACTER_SET.into()),
        std::env::var("DD_MANAGER_EMAIL_DOMAIN").unwrap_or("valid.email.com".into())
    )
}

fn rand_password() -> String {
    rand(20, CHARACTER_SET.into())
}

fn rand_partition_name(enc: Encoding, comp: Compression) -> String {
    let ts = chrono::Utc::now().to_string();
    let enc = match enc {
        Encoding::PlainText => "txt",
        Encoding::Json => "json",
        Encoding::NdJson => "ndjson",
        Encoding::Csv => "csv",
        Encoding::Tsv => "tsv",
        Encoding::Protobuf => "pb",
    };
    let comp = match comp {
        Compression::Uncompressed => "",
        Compression::Tar => "tar.gz",
        Compression::Zip => "zip",
    };
    format!("partition-{}.{}.{}", ts, enc, comp)
        .trim_end_matches(".")
        .into()
}

#[test]
fn test_rand_partition_name() {
    assert!(rand_partition_name(Encoding::Protobuf, Compression::Uncompressed).ends_with(".pb"));
    assert!(rand_partition_name(Encoding::Csv, Compression::Tar).ends_with(".csv.tar.gz"));
}

fn rand_schema_name() -> String {
    rand(20, CHARACTER_SET.replace("0123456789", ""))
}

pub enum Rand {
    Email,
    Password,
    String(usize),
    PartitionName(Encoding, Compression),
    SchemaName,
}

pub fn get_rand(of: Rand) -> String {
    match of {
        Rand::Email => rand_valid_email(),
        Rand::Password => rand_password(),
        Rand::String(s) => rand(s, CHARACTER_SET.into()),
        Rand::PartitionName(enc, comp) => rand_partition_name(enc, comp),
        Rand::SchemaName => rand_schema_name(),
    }
}
