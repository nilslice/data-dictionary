use data_dictionary::db::{rand, Db, CHARACTER_SET};
use data_dictionary::dict::{Classification, Compression, Encoding};
use data_dictionary::error::Error;
use data_dictionary::service::DataService;

use uuid::Uuid;

mod migrate {
    use refinery::embed_migrations as embed;
    embed!("migrations");
}

fn reset_db(conn: &mut Db) -> Result<(), Error> {
    clear_db(conn)?;
    migrate_db(conn)?;

    Ok(())
}

fn migrate_db(conn: &mut Db) -> Result<(), Error> {
    migrate::migrations::runner()
        .run(&mut conn.client)
        .map_err(|e| Error::Generic(Box::new(e)))?;

    Ok(())
}

fn clear_db(conn: &mut Db) -> Result<(), Error> {
    conn.client
        .batch_execute(include_str!("sql/drop_all.sql"))
        .map_err(|e| Error::Generic(Box::new(e)))
}

fn new_test_db() -> Result<Db, Error> {
    let mut db = Db::connect(None)?;
    reset_db(&mut db)?;

    Ok(db)
}

#[test]
fn try_clear_db() {
    let mut db = new_test_db().unwrap();
    clear_db(&mut db).unwrap();
}

#[test]
fn test_dataset() {
    let mut db = new_test_db().unwrap();

    // create a manager
    let email = format!("{}@recurly.com", rand(10, CHARACTER_SET.into()));
    let password = "test_password";
    let manager = db.register_manager(&email, &password).unwrap();

    // create a dataset
    let dataset = db
        .register_dataset(
            &manager,
            "test_dataset",
            Compression::None,
            Encoding::Json,
            Classification::Sensitive,
            "this is the test dataset, used for testing datasets",
        )
        .unwrap();
    println!("dataset: {:?}", dataset);
}

#[test]
fn test_manager() {
    let mut db = new_test_db().unwrap();

    // insert a manager using an email and password
    let email = format!("{}@recurly.com", rand(10, CHARACTER_SET.into()));
    let password = "test_password";

    // check that the manager value is ok and has expected fields set
    let registered = db.register_manager(&email, &password);
    assert!(registered.is_ok());
    let registered = registered.unwrap();
    assert_ne!(registered.api_key.to_string(), "");
    assert_ne!(registered.api_key, Uuid::nil());
    assert_ne!(registered.hash, vec![]);
    assert_ne!(registered.salt, "");

    // set test email validation domain so validation fails
    // check that invalid email address patterns fail registration
    std::env::set_var("DD_MANAGER_EMAIL_DOMAIN", "test.com");
    let invalid = db.register_manager("bad@validation.com", "12345678");
    assert!(invalid.is_err());

    // check that duplicate email registration attempts fail
    let dup = db.register_manager(&email, &password);
    assert!(dup.is_err());

    // check that an authentication check passes, and the same values are maintained
    let authed = db.auth_manager(&email, &password);
    assert!(authed.is_ok());
    let authed = authed.unwrap();
    assert_eq!(registered.hash, authed.hash);
    assert_eq!(registered.api_key, authed.api_key);

    // check that invalid passwords fail authentication
    let invalid = db.auth_manager(&email, "invalidPassword");
    assert!(invalid.is_err());
}
