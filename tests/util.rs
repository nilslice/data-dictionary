use data_dictionary::db::{rand, Db, CHARACTER_SET};
use data_dictionary::dict::{
    Classification, Compression, DatasetSchema, Encoding, FileExt, Manager,
};
use data_dictionary::error::Error;

pub struct TestDb {
    pub db: Db,
    schema: String,
}

pub async fn reset_db(conn: &mut TestDb) -> Result<(), Error> {
    clear_db(conn).await?;
    conn.db.migrate().await?;
    Ok(())
}

pub async fn clear_db(conn: &mut TestDb) -> Result<(), Error> {
    conn.db
        .client
        .batch_execute(include_str!("sql/drop_all.sql"))
        .await
        .map_err(|e| Error::Generic(Box::new(e)))
}

pub async fn new_test_db() -> Result<TestDb, Error> {
    let db = Db::connect(None).await?;
    // create random schema name and use it for the current connection (used by single test)
    let mut test_db = TestDb {
        db,
        schema: get_rand(Rand::SchemaName),
    };
    test_db
        .db
        .client
        .batch_execute(&format!(
            "CREATE SCHEMA IF NOT EXISTS {}; SET search_path TO {};",
            test_db.schema, test_db.schema
        ))
        .await?;

    reset_db(&mut test_db).await?;

    Ok(test_db)
}

pub async fn drop_test_db(conn: &mut TestDb) -> Result<(), Error> {
    let _ = conn
        .db
        .client
        .simple_query(&format!("DROP SCHEMA IF EXISTS {} CASCADE;", conn.schema))
        .await?;

    Ok(())
}

pub async fn create_manager(conn: &mut TestDb) -> Result<Manager, Error> {
    // create a manager
    let email = get_rand(Rand::Email);
    let password = get_rand(Rand::Password);
    Manager::register(&mut conn.db, email, password).await
}

fn rand_valid_email() -> String {
    format!(
        "{}@{}",
        rand(6, CHARACTER_SET.into()),
        std::env::var("DD_MANAGER_EMAIL_DOMAIN").unwrap_or_else(|_| "valid.email.com".to_string())
    )
}

fn rand_password() -> String {
    rand(20, CHARACTER_SET.into())
}

fn rand_partition_name(enc: Encoding, comp: Compression) -> String {
    let ts = chrono::Utc::now().to_string().replace(" ", "-");
    format!("partition-{}.{}.{}", ts, enc.to_ext(), comp.to_ext())
        .trim_end_matches('.')
        .into()
}

#[test]
fn test_rand_partition_name() {
    assert!(rand_partition_name(Encoding::Protobuf, Compression::Uncompressed).ends_with(".pb"));
    assert!(rand_partition_name(Encoding::Csv, Compression::Tar).ends_with(".csv.tar.gz"));
}

fn rand_partition_url(enc: Encoding, comp: Compression, class: Classification) -> String {
    format!(
        "cloud://org.datasets.dev.{}/{}/{}",
        class,
        get_rand(Rand::String(20)),
        rand_partition_name(enc, comp)
    )
}

#[test]
fn test_rand_url() {
    println!(
        "{}",
        rand_partition_url(Encoding::Json, Compression::Tar, Classification::Sensitive)
    );
}

fn rand_schema_name() -> String {
    rand(20, CHARACTER_SET.replace("0123456789", ""))
}

pub enum Rand {
    Email,
    Password,
    String(usize),
    PartitionName(Encoding, Compression),
    PartitionUrl(Encoding, Compression, Classification),
    SchemaName,
}

pub fn rand_schema() -> DatasetSchema {
    let mut schema = std::collections::HashMap::new();
    schema.insert("merchant_id".into(), Some("integer".into()));
    schema.insert("merchant_name".into(), Some("string".into()));
    schema.insert("mrr_cents".into(), Some("integer".into()));
    schema.insert("churn_rate".into(), Some("float".into()));
    schema.insert("last_billed".into(), Some("date".into()));

    schema
}

pub fn get_rand(of: Rand) -> String {
    match of {
        Rand::Email => rand_valid_email(),
        Rand::Password => rand_password(),
        Rand::String(s) => rand(s, CHARACTER_SET.into()),
        Rand::PartitionName(enc, comp) => rand_partition_name(enc, comp),
        Rand::PartitionUrl(enc, comp, class) => rand_partition_url(enc, comp, class),
        Rand::SchemaName => rand_schema_name(),
    }
}
