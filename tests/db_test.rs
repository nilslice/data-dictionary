mod util;
use util::Rand::{Email, PartitionName, Password, String};

use data_dictionary::dict::{Classification, Compression, Encoding};
use data_dictionary::dict::{Dataset, Manager};
use data_dictionary::service::DataService;

use uuid::Uuid;

#[test]
fn try_clear_db() {
    let mut test_db = util::new_test_db().unwrap();
    util::clear_db(&mut test_db).unwrap();
}

#[test]
fn test_dataset() {
    let mut test_db = util::new_test_db().unwrap();

    // create a manager
    let email = util::get_rand(Email);
    let password = util::get_rand(Password);
    let manager = test_db.db.register_manager(&email, &password).unwrap();

    let dataset_name: &str = &util::get_rand(String(18));
    let dataset_desc: &str = &util::get_rand(String(42));
    // create a dataset
    let dataset_result = test_db.db.register_dataset(
        &manager,
        dataset_name,
        Compression::Uncompressed,
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
    let latest_result = dataset.latest_partition(&mut test_db.db);
    assert!(latest_result.is_err());

    // add a partition and validate latest partition exists
    let partition_name = util::get_rand(String(20));
    let partition_result = dataset.register_partition(&mut test_db.db, partition_name);
    assert!(partition_result.is_ok());
    let partition = partition_result.unwrap();
    assert_ne!(partition.id, 0);
    let latest_result = dataset.latest_partition(&mut test_db.db);
    assert!(latest_result.is_ok());
    let latest = latest_result.unwrap();
    assert_eq!(latest.id, partition.id);
    assert_eq!(latest.name, partition.name);
}

#[test]
fn test_module_integration() {
    let mut test_db = util::new_test_db().unwrap();

    // create a manager
    let email: &str = &util::get_rand(Email);
    let password: &str = &util::get_rand(Password);
    let manager = Manager::register(&mut test_db.db, email, password).unwrap();
    assert_ne!(manager.id, 0);
    assert_ne!(manager.hash, vec![]);
    assert_eq!(manager.email, email.as_ref());

    // check manager authentication
    assert!(Manager::authenticate(&mut test_db.db, email, password).is_ok());
    assert!(Manager::authenticate(&mut test_db.db, email, "invalid_password").is_err());

    // find the manager from the database
    let api_key = manager.api_key;
    let manager_result = Manager::find(&mut test_db.db, api_key);
    assert!(manager_result.is_ok());
    let manager = manager_result.unwrap();
    assert_ne!(manager.id, 0);
    assert_eq!(manager.email, email);

    let dataset_name: &str = &util::get_rand(String(10));
    let dataset_desc: &str = &util::get_rand(String(40));
    // create a dataset for a manager
    let dataset_result = manager.register_dataset(
        &mut test_db.db,
        dataset_name,
        Compression::Uncompressed,
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
    let dataset_result = Dataset::find(&mut test_db.db, dataset_name);
    assert!(dataset_result.is_ok());
    let dataset = dataset_result.unwrap();
    assert_ne!(dataset.id, 0);
    assert_eq!(dataset.description, dataset_desc);

    let partition_name =
        util::get_rand(PartitionName(Encoding::Protobuf, Compression::Uncompressed));
    // add a partition to the dataset
    let partition_result = dataset.register_partition(&mut test_db.db, &partition_name);
    assert!(partition_result.is_ok());
    let partition = partition_result.unwrap();
    assert_ne!(partition.id, 0);
    let parition_id = partition.id;
    assert_eq!(partition.name, partition_name);

    // find the partition, expect to fail with no matching partition name
    let partition_result = dataset.partition(
        &mut test_db.db,
        &util::get_rand(PartitionName(Encoding::Csv, Compression::Zip)),
    );
    assert!(partition_result.is_err());

    // sucessfully find the partition
    let partition_result = dataset.partition(&mut test_db.db, partition_name);
    assert!(partition_result.is_ok());
    assert_eq!(partition.id, parition_id);
    let partition = partition_result.unwrap();
    assert_ne!(partition.id, 0);
    assert_eq!(partition.id, parition_id); // check that the id is the same from registeration

    let added_dataset_name = util::get_rand(String(20));
    // add another dataset, use to test manager-owned datasets
    let added_dataset_result = manager.register_dataset(
        &mut test_db.db,
        added_dataset_name,
        Compression::Tar,
        Encoding::NdJson,
        Classification::Public,
        util::get_rand(String(40)),
    );
    assert!(added_dataset_result.is_ok());
    let added_dataset = added_dataset_result.unwrap();
    assert_ne!(added_dataset.id, 0);
    assert_ne!(added_dataset.id, dataset.id);
    assert_ne!(added_dataset.name, dataset.name);
    assert_eq!(added_dataset.manager_id, dataset.manager_id);

    // test manager-owned datasets
    let manager_datasets_result = manager.datasets(&mut test_db.db);
    assert!(manager_datasets_result.is_ok());
    let manager_datasets = manager_datasets_result.unwrap();
    assert!(manager_datasets.len() == 2);
    let a = manager_datasets.get(0).unwrap() as &Dataset;
    let b = manager_datasets.get(1).unwrap() as &Dataset;
    assert!(a.created_at.lt(&b.created_at));
    let dataset_ids: Vec<i32> = manager_datasets.iter().map(|ds| ds.id).collect();
    assert!(dataset_ids.contains(&dataset.id));
    assert!(dataset_ids.contains(&added_dataset.id));

    // list all datasets and compare results to known added values
    let all_datasets_result = Dataset::list(&mut test_db.db);
    assert!(all_datasets_result.is_ok());
    let all_datasets = all_datasets_result.unwrap();
    let known_dataset_ids = manager_datasets
        .iter()
        .map(|d| d.id)
        .collect::<Vec<i32>>()
        .sort();
    let all_dataset_ids = all_datasets
        .iter()
        .map(|d| d.id)
        .collect::<Vec<i32>>()
        .sort();
    assert_eq!(known_dataset_ids, all_dataset_ids);

    let mut known_added_partitions = vec![];

    // add a partition to the new dataset
    let added_partition_name: &str =
        &util::get_rand(PartitionName(Encoding::PlainText, Compression::Zip));
    let added_partition_result =
        added_dataset.register_partition(&mut test_db.db, added_partition_name);
    assert!(added_partition_result.is_ok());
    let added_partition = added_partition_result.unwrap();
    assert_ne!(added_partition.id, 0);
    assert_eq!(added_partition.name, added_partition_name);

    // add the partition id to our list for tracking
    known_added_partitions.push(added_partition.id);

    // check for the latest partition on the added datasest, test it is the same as the partition
    // which was most recently added
    let latest_from_added_result = added_dataset.latest_partition(&mut test_db.db);
    assert!(latest_from_added_result.is_ok());
    let latest_from_added = latest_from_added_result.unwrap();
    assert_eq!(latest_from_added, added_partition);

    // add another partition to the new dataset, test that it belongs to the same dataset as
    // previously added partition
    let added_dataset_add_partition_result = added_dataset.register_partition(
        &mut test_db.db,
        util::get_rand(PartitionName(Encoding::NdJson, Compression::Tar)),
    );
    assert!(added_dataset_add_partition_result.is_ok());
    let added_dataset_add_partition = added_dataset_add_partition_result.unwrap();
    assert_eq!(
        added_dataset_add_partition.dataset_id,
        added_partition.dataset_id
    );

    // add the partition id to our list for tracking
    known_added_partitions.push(added_dataset_add_partition.id);

    // list all partitions for the added dataset, compare with known partition values within it
    let all_added_partitions_result = added_dataset.partitions(&mut test_db.db);
    assert!(all_added_partitions_result.is_ok());
    let all_added_partitions = all_added_partitions_result.unwrap();
    let mut all_added_partition_ids = all_added_partitions
        .iter()
        .map(|p| p.id)
        .collect::<Vec<i32>>();
    assert_eq!(
        all_added_partition_ids.sort(),
        known_added_partitions.sort()
    );
}

#[test]
fn test_manager() {
    let mut test_db = util::new_test_db().unwrap();

    // insert a manager using an email and password
    let email = util::get_rand(Email);
    let password = util::get_rand(Password);

    // check that the manager value is ok and has expected fields set
    let registered = test_db.db.register_manager(&email, &password);
    assert!(registered.is_ok());
    let registered = registered.unwrap();
    assert_ne!(registered.api_key.to_string(), "");
    assert_ne!(registered.api_key, Uuid::nil());
    assert_ne!(registered.hash, vec![]);
    assert_ne!(registered.salt, "");

    // set test email validation domain so validation fails
    // check that invalid email address patterns fail registration
    std::env::set_var("DD_MANAGER_EMAIL_DOMAIN", "test.com");
    let invalid = test_db
        .db
        .register_manager("bad@validation.com", "12345678");
    assert!(invalid.is_err());

    // check that duplicate email registration attempts fail
    let dup = test_db.db.register_manager(&email, &password);
    assert!(dup.is_err());

    // check that an authentication check passes, and the same values are maintained
    let authed = test_db.db.auth_manager(&email, &password);
    assert!(authed.is_ok());
    let authed = authed.unwrap();
    assert_eq!(registered.hash, authed.hash);
    assert_eq!(registered.api_key, authed.api_key);

    // check that invalid passwords fail authentication
    let invalid = test_db.db.auth_manager(&email, "invalidPassword");
    assert!(invalid.is_err());
}
