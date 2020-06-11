mod util;
use util::Rand::{Email, Password};

use data_dictionary::db::{rand, CHARACTER_SET};
use data_dictionary::dict::{Classification, Compression, Encoding};
use data_dictionary::dict::{Dataset, Manager};
use data_dictionary::service::DataService;

use uuid::Uuid;

#[test]
fn try_clear_db() {
    let mut db = util::new_test_db().unwrap();
    util::clear_db(&mut db).unwrap();
}

#[test]
fn test_dataset() {
    let mut db = util::new_test_db().unwrap();

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
fn test_lib() {
    let mut db = util::new_test_db().unwrap();

    // create a manager
    let email = &util::get_rand(Email);
    let password = &util::get_rand(Password);
    let manager = Manager::register(&mut db, email, password).unwrap();
    assert_ne!(manager.id, 0);
    assert_ne!(manager.hash, vec![]);
    assert_eq!(manager.email, email.as_ref());

    // check manager authentication
    assert!(Manager::authenticate(&mut db, email, password).is_ok());
    assert!(Manager::authenticate(&mut db, email, "invalid_password").is_err());

    let dataset_name = "test_dataset_name";
    let description = "the description of the dataset for testing.";
    // create a dataset for a manager
    let dataset = manager
        .register_dataset(
            &mut db,
            dataset_name,
            Compression::None,
            Encoding::Protobuf,
            Classification::Sensitive,
            description,
        )
        .unwrap();

    // find the dataset
    let dataset_result = Dataset::find(&mut db, dataset_name);
    assert!(dataset_result.is_ok());
    let dataset = dataset_result.unwrap();
    assert_ne!(dataset.id, 0);
    assert_eq!(dataset.description, description);

    let partition_name = "test_partition_of_test_dataset-2020-06-10.test_dataset.pb";
    // add a partition to the dataset
    let partition_result = dataset.register_partition(&mut db, partition_name);
    assert!(partition_result.is_ok());
    let partition = partition_result.unwrap();
    assert_ne!(partition.id, 0);
    let parition_id = partition.id;
    assert_eq!(partition.name, partition_name);

    // find the partition, expect to fail with no matching partition name
    let partition_result = dataset.partition(&mut db, "not_the_partition_name.tar.gz");
    assert!(partition_result.is_err());

    // sucessfully find the partition
    let partition_result = dataset.partition(&mut db, partition_name);
    assert!(partition_result.is_ok());
    assert_eq!(partition.id, parition_id);
    let partition = partition_result.unwrap();
    assert_ne!(partition.id, 0);
    assert_eq!(partition.id, parition_id); // check that the id is the same from registeration

    let added_dataset_name = "added_test_dataset";
    // add another dataset, use to test manager-owned datasets
    let added_dataset_result = manager.register_dataset(
        &mut db,
        added_dataset_name,
        Compression::Tar,
        Encoding::NdJson,
        Classification::Public,
        "a new dataset, added after the initial one.",
    );
    assert!(added_dataset_result.is_ok());
    let added_dataset = added_dataset_result.unwrap();
    assert_ne!(added_dataset.id, 0);
    assert_ne!(added_dataset.id, dataset.id);
    assert_ne!(added_dataset.name, dataset.name);
    assert_eq!(added_dataset.manager_id, dataset.manager_id);

    // test manager-owned datasets
    let manager_datasets_result = manager.datasets(&mut db);
    assert!(manager_datasets_result.is_ok());
    let manager_datasets = manager_datasets_result.unwrap();
    assert!(manager_datasets.len() == 2);
    let a = manager_datasets.get(0).unwrap() as &Dataset;
    let b = manager_datasets.get(1).unwrap() as &Dataset;
    assert!(a.created_at.lt(&b.created_at));
    let dataset_ids: Vec<i32> = manager_datasets.iter().map(|ds| ds.id).collect();
    assert!(dataset_ids.contains(&dataset.id));
    assert!(dataset_ids.contains(&added_dataset.id));
}

#[test]
fn test_manager() {
    let mut db = util::new_test_db().unwrap();

    // insert a manager using an email and password
    let email = util::get_rand(Email);
    let password = util::get_rand(Password);

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
