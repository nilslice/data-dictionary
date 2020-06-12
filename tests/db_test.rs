mod util;
use util::Rand::{Email, PartitionName, Password, String};

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
    let email = util::get_rand(Email);
    let password = util::get_rand(Password);
    let manager = db.register_manager(&email, &password).unwrap();

    let dataset_name: &str = &util::get_rand(String(18));
    let dataset_desc: &str = &util::get_rand(String(42));
    // create a dataset
    let dataset_result = db.register_dataset(
        &manager,
        dataset_name,
        Compression::None,
        Encoding::Json,
        Classification::Sensitive,
        dataset_desc,
    );
    assert!(dataset_result.is_ok());
    let dataset = dataset_result.unwrap();
    assert_ne!(dataset.id, 0);
    assert_eq!(dataset.manager_id, manager.id);
    assert_eq!(dataset.name, dataset_name);
    assert_eq!(dataset.description, dataset_desc);

    // test failure when dataset has no partitions
    let latest_result = dataset.latest_partition(&mut db);
    assert!(latest_result.is_err());

    // add a partition and validate latest partition exists
    let partition_name = util::get_rand(String(20));
    let partition_result = dataset.register_partition(&mut db, partition_name);
    assert!(partition_result.is_ok());
    let partition = partition_result.unwrap();
    assert_ne!(partition.id, 0);
    let latest_result = dataset.latest_partition(&mut db);
    assert!(latest_result.is_ok());
    let latest = latest_result.unwrap();
    assert_eq!(latest.id, partition.id);
}

#[test]
fn test_module_integration() {
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
    let dataset_desc = "the description of the dataset for testing.";
    // create a dataset for a manager
    let dataset_result = manager.register_dataset(
        &mut db,
        dataset_name,
        Compression::None,
        Encoding::Protobuf,
        Classification::Sensitive,
        dataset_desc,
    );
    assert!(dataset_result.is_ok());
    let dataset = dataset_result.unwrap();
    assert_ne!(dataset.id, 0);
    assert_eq!(dataset.manager_id, manager.id);
    assert_eq!(dataset.name, dataset_name);
    assert_eq!(dataset.description, dataset_desc);

    // find the dataset
    let dataset_result = Dataset::find(&mut db, dataset_name);
    assert!(dataset_result.is_ok());
    let dataset = dataset_result.unwrap();
    assert_ne!(dataset.id, 0);
    assert_eq!(dataset.description, dataset_desc);

    let partition_name = util::get_rand(PartitionName(Encoding::Protobuf, Compression::None));
    // add a partition to the dataset
    let partition_result = dataset.register_partition(&mut db, &partition_name);
    assert!(partition_result.is_ok());
    let partition = partition_result.unwrap();
    assert_ne!(partition.id, 0);
    let parition_id = partition.id;
    assert_eq!(partition.name, partition_name);

    // find the partition, expect to fail with no matching partition name
    let partition_result = dataset.partition(
        &mut db,
        &util::get_rand(PartitionName(Encoding::Csv, Compression::Zip)),
    );
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
