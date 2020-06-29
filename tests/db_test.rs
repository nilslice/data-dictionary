mod testutil;
use testutil::Rand::{Email, PartitionName, PartitionUrl, Password, String};

use data_dictionary::dict::{Classification, Compression, Format};
use data_dictionary::dict::{Dataset, DatasetConfig, Manager, Partition};
use data_dictionary::service::DataService;

use chrono::{DateTime, Utc};
use uuid::Uuid;

#[tokio::test]
async fn try_clear_db() {
    let mut test_db = testutil::new_test_db().await.unwrap();
    testutil::clear_db(&mut test_db).await.unwrap();
    testutil::drop_test_db(&mut test_db).await.unwrap();
}

#[tokio::test]
async fn test_dataset() {
    let mut test_db = testutil::new_test_db().await.unwrap();

    let manager = testutil::create_manager(&mut test_db).await.unwrap();

    let dataset_name = testutil::get_rand(String(18));
    let dataset_desc = testutil::get_rand(String(42));
    // create a dataset
    let dataset = test_db
        .db
        .register_dataset(
            &manager,
            &dataset_name,
            Compression::Uncompressed,
            Format::Json,
            Classification::Sensitive,
            testutil::rand_schema(),
            &dataset_desc,
        )
        .await
        .unwrap();
    assert_ne!(dataset.id, 0);
    assert_eq!(dataset.manager_id, manager.id);
    assert_eq!(dataset.name, dataset_name);
    assert_eq!(dataset.description, dataset_desc);
    let dataset_id = dataset.id;

    // find the dataset from the database
    let dataset = test_db.db.find_dataset(&dataset_name).await.unwrap();
    assert_eq!(dataset.id, dataset_id);

    // expect finding dataset to fail when using unregistered name
    let dataset_result = test_db.db.find_dataset("bad_dataset_name").await;
    assert!(dataset_result.is_err());

    for _ in 0..99 {
        let dataset = test_db
            .db
            .register_dataset(
                &manager,
                &testutil::get_rand(String(20)),
                Compression::Zip,
                Format::Csv,
                Classification::Private,
                testutil::rand_schema(),
                &testutil::get_rand(String(100)),
            )
            .await
            .unwrap();

        let partition_result = test_db
            .db
            .register_partition(
                &dataset,
                &testutil::get_rand(PartitionName(Format::PlainText, Compression::Uncompressed)),
                &testutil::get_rand(PartitionUrl(
                    Format::PlainText,
                    Compression::Uncompressed,
                    Classification::Sensitive,
                )),
            )
            .await;
        assert!(partition_result.is_ok());
    }

    let all_datasets = test_db.db.list_datasets().await.unwrap();
    assert_eq!(all_datasets.len(), 100 as usize);

    // delete the last dataset added and verify that it no longer exists
    let last_dataset = all_datasets.last().unwrap();
    let delete_result = test_db.db.delete_dataset(last_dataset).await;
    assert!(delete_result.is_ok());
    let dataset_result = test_db.db.find_dataset(&last_dataset.name).await;
    assert!(dataset_result.is_err());

    // test failure when dataset has no partitions
    let latest_result = dataset.latest_partition(&mut test_db.db).await;
    assert!(latest_result.is_err());

    // add a partition and validate latest partition exists
    let partition_name = testutil::get_rand(PartitionName(Format::Protobuf, Compression::Tar));
    let partition_url = testutil::get_rand(PartitionUrl(
        Format::Protobuf,
        Compression::Tar,
        Classification::Public,
    ));
    let partition_result = dataset
        .register_partition(&mut test_db.db, partition_name, partition_url)
        .await;
    assert!(partition_result.is_ok());
    let partition = partition_result.unwrap();
    assert_ne!(partition.id, 0);
    let latest_result = dataset.latest_partition(&mut test_db.db).await;
    assert!(latest_result.is_ok());
    let latest = latest_result.unwrap();
    assert_eq!(latest.id, partition.id);
    assert_eq!(latest.name, partition.name);

    // add another partition so next test can delete it and see that latest changes
    let new_added_partition = dataset
        .register_partition(
            &mut test_db.db,
            testutil::get_rand(PartitionName(Format::Csv, Compression::Zip)),
            testutil::get_rand(PartitionUrl(
                Format::Csv,
                Compression::Zip,
                Classification::Private,
            )),
        )
        .await
        .unwrap();

    // verify the latest partition updated
    let new_latest_found = dataset.latest_partition(&mut test_db.db).await.unwrap();
    assert_eq!(new_latest_found.id, new_added_partition.id);
    assert_eq!(new_latest_found.name, new_added_partition.name);

    // delete the new latest partition and validate that the latest partition changes
    let prev_before_new_added_latest = latest;
    dataset
        .delete_partition(&mut test_db.db, &new_added_partition.name)
        .await
        .unwrap();
    let updated_latest = dataset.latest_partition(&mut test_db.db).await.unwrap();
    assert_eq!(updated_latest.id, prev_before_new_added_latest.id);
    assert_eq!(updated_latest.name, prev_before_new_added_latest.name);
    assert_eq!(
        updated_latest.created_at,
        prev_before_new_added_latest.created_at
    );

    testutil::drop_test_db(&mut test_db).await.unwrap();
}

#[tokio::test]
async fn test_module_integration() {
    let mut test_db = testutil::new_test_db().await.unwrap();

    // create a manager
    let email: &str = &testutil::get_rand(Email);
    let password: &str = &testutil::get_rand(Password);
    let manager = Manager::register(&mut test_db.db, email, password)
        .await
        .unwrap();
    assert_ne!(manager.id, 0);
    assert_ne!(manager.hash.len(), 0);
    assert_eq!(manager.email, email);

    // check manager authentication
    assert!(Manager::authenticate(&mut test_db.db, email, password)
        .await
        .is_ok());
    assert!(
        Manager::authenticate(&mut test_db.db, email, "invalid_password")
            .await
            .is_err()
    );

    // find the manager from the database
    let api_key = manager.api_key;
    let manager = Manager::find(&mut test_db.db, api_key).await.unwrap();
    assert_ne!(manager.id, 0);
    assert_eq!(manager.email, email);

    let dataset_name = &testutil::get_rand(String(10));
    let dataset_desc = &testutil::get_rand(String(40));
    // create a dataset for a manager
    let dataset = manager
        .register_dataset(
            &mut test_db.db,
            dataset_name,
            Compression::Uncompressed,
            Format::Protobuf,
            Classification::Sensitive,
            testutil::rand_schema(),
            dataset_desc,
        )
        .await
        .unwrap();
    assert_ne!(dataset.id, 0);
    assert_eq!(dataset.manager_id, manager.id);
    assert_eq!(&dataset.name, dataset_name);
    assert_eq!(&dataset.description, dataset_desc);

    // find the dataset
    let dataset = Dataset::find(&mut test_db.db, dataset_name).await.unwrap();
    assert_ne!(dataset.id, 0);
    assert_eq!(&dataset.description, dataset_desc);

    let partition_name =
        testutil::get_rand(PartitionName(Format::Protobuf, Compression::Uncompressed));
    let partition_url = testutil::get_rand(PartitionUrl(
        Format::Protobuf,
        Compression::Uncompressed,
        Classification::Private,
    ));
    // add a partition to the dataset
    let partition = dataset
        .register_partition(&mut test_db.db, &partition_name, &partition_url)
        .await
        .unwrap();
    assert_ne!(partition.id, 0);
    let parition_id = partition.id;
    assert_eq!(partition.name, partition_name);

    // find the partition, expect to fail with no matching partition name
    let partition_result = dataset
        .partition(
            &mut test_db.db,
            &testutil::get_rand(PartitionName(Format::Csv, Compression::Zip)),
        )
        .await;
    assert!(partition_result.is_err());

    // sucessfully find the partition
    let partition = dataset
        .partition(&mut test_db.db, partition_name)
        .await
        .unwrap();
    assert_ne!(partition.id, 0);
    assert_eq!(partition.id, parition_id); // check that the id is the same from registeration

    let added_dataset_name = testutil::get_rand(String(20));
    // add another dataset, use to test manager-owned datasets
    let added_dataset = manager
        .register_dataset(
            &mut test_db.db,
            added_dataset_name,
            Compression::Tar,
            Format::NdJson,
            Classification::Public,
            testutil::rand_schema(),
            testutil::get_rand(String(40)),
        )
        .await
        .unwrap();
    assert_ne!(added_dataset.id, 0);
    assert_ne!(added_dataset.id, dataset.id);
    assert_ne!(added_dataset.name, dataset.name);
    assert_eq!(added_dataset.manager_id, dataset.manager_id);

    // test manager-owned datasets
    let manager_datasets = manager.datasets(&mut test_db.db).await.unwrap();
    assert!(manager_datasets.len() == 2);
    let a = manager_datasets.get(0).unwrap() as &Dataset;
    let b = manager_datasets.get(1).unwrap() as &Dataset;
    assert!(a.created_at.lt(&b.created_at));
    let dataset_ids: Vec<i32> = manager_datasets.iter().map(|ds| ds.id).collect();
    assert!(dataset_ids.contains(&dataset.id));
    assert!(dataset_ids.contains(&added_dataset.id));

    // list all datasets and compare results to known added values
    let all_datasets = Dataset::list(&mut test_db.db).await.unwrap();
    let mut known_dataset_ids = manager_datasets.iter().map(|d| d.id).collect::<Vec<i32>>();
    known_dataset_ids.sort();
    let mut all_dataset_ids = all_datasets.iter().map(|d| d.id).collect::<Vec<i32>>();
    all_dataset_ids.sort();
    assert_eq!(known_dataset_ids, all_dataset_ids);

    let mut known_added_partitions = vec![];

    // add a partition to the new dataset
    let added_partition_name =
        &testutil::get_rand(PartitionName(Format::PlainText, Compression::Zip));
    let added_partition_url = &testutil::get_rand(PartitionUrl(
        Format::PlainText,
        Compression::Zip,
        Classification::Confidential,
    ));
    let added_partition = added_dataset
        .register_partition(&mut test_db.db, added_partition_name, added_partition_url)
        .await
        .unwrap();
    assert_ne!(added_partition.id, 0);
    assert_eq!(&added_partition.name, added_partition_name);

    // add the partition id to our list for tracking
    known_added_partitions.push(added_partition.id);

    // check for the latest partition on the added datasest, test it is the same as the partition
    // which was most recently added
    let latest_from_added = added_dataset
        .latest_partition(&mut test_db.db)
        .await
        .unwrap();
    assert_eq!(latest_from_added, added_partition);

    // add another partition to the new dataset, test that it belongs to the same dataset as
    // previously added partition
    let added_dataset_add_partition = added_dataset
        .register_partition(
            &mut test_db.db,
            testutil::get_rand(PartitionName(Format::NdJson, Compression::Tar)),
            testutil::get_rand(PartitionUrl(
                Format::NdJson,
                Compression::Tar,
                Classification::Public,
            )),
        )
        .await
        .unwrap();
    assert_eq!(
        added_dataset_add_partition.dataset_id,
        added_partition.dataset_id
    );

    // add the partition id to our list for tracking
    known_added_partitions.push(added_dataset_add_partition.id);

    // list all partitions for the added dataset, compare with known partition values within it
    let all_added_partitions = added_dataset.partitions(&mut test_db.db).await.unwrap();
    let mut all_added_partition_ids = all_added_partitions
        .iter()
        .map(|p| p.id)
        .collect::<Vec<i32>>();
    all_added_partition_ids.sort();
    known_added_partitions.sort();
    assert_eq!(all_added_partition_ids, known_added_partitions);

    // add a partition with the reserved name "latest", expect it to fail
    let bad_partition_result = dataset
        .register_partition(
            &mut test_db.db,
            data_dictionary::dict::PARTITION_LATEST,
            testutil::get_rand(PartitionUrl(
                Format::Csv,
                Compression::Uncompressed,
                Classification::Public,
            )),
        )
        .await;
    assert!(bad_partition_result.is_err());

    testutil::drop_test_db(&mut test_db).await.unwrap();
}

#[tokio::test]
async fn test_range_query() {
    let mut test_db = testutil::new_test_db().await.unwrap();

    let manager = Manager::register(
        &mut test_db.db,
        testutil::get_rand(Email),
        testutil::get_rand(Password),
    )
    .await
    .unwrap();
    let dataset = manager
        .register_dataset(
            &mut test_db.db,
            testutil::get_rand(String(25)),
            Compression::Uncompressed,
            Format::Tsv,
            Classification::Public,
            testutil::rand_schema(),
            testutil::get_rand(String(50)),
        )
        .await
        .unwrap();

    let partition_count = 30;
    // insert partitions, spread over some duration
    for _ in 0..partition_count {
        dataset
            .register_partition(
                &mut test_db.db,
                testutil::get_rand(PartitionName(Format::Protobuf, Compression::Tar)),
                testutil::get_rand(PartitionUrl(
                    Format::Protobuf,
                    Compression::Tar,
                    Classification::Public,
                )),
            )
            .await
            .unwrap();

        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    let partitions_all = dataset.partitions(&mut test_db.db).await.unwrap();
    assert_eq!(partitions_all.len(), partition_count);
    // test that range ordering is correct, sorted by created_at ASC
    // TODO: reimplement this test once `is_sorted_by_key` is stablized
    let mut last: &Partition = partitions_all.get(0).unwrap();
    for p in partitions_all.iter().skip(1) {
        assert!(p.created_at > last.created_at);
        assert_ne!(p.name, "");
        assert_ne!(p.url, "");
        last = p;
    }

    // test no parameters
    let mut params = Default::default();
    let partitions = dataset
        .partition_range(&mut test_db.db, &params)
        .await
        .unwrap();
    assert_eq!(partitions.len(), partition_count);

    let midpoint: DateTime<Utc> = partitions.get(partition_count / 2 - 1).unwrap().created_at;

    // test start date parameter
    params = Default::default();
    params.start = Some(midpoint);
    let after_midpoint = dataset
        .partition_range(&mut test_db.db, &params)
        .await
        .unwrap();
    for p in after_midpoint.iter() {
        assert!(p.created_at.ge(&midpoint));
    }

    // test end date parameter
    params = Default::default();
    params.end = Some(midpoint);
    let before_midpoint = dataset
        .partition_range(&mut test_db.db, &params)
        .await
        .unwrap();
    for p in before_midpoint.iter() {
        assert!(p.created_at.le(&midpoint));
    }

    // test count parameter
    params = Default::default();
    for test_count in 1..partition_count {
        params.count = Some(test_count as i32);
        let specific_count = dataset
            .partition_range(&mut test_db.db, &params)
            .await
            .unwrap();
        assert_eq!(specific_count.len(), test_count as usize);
    }

    // test that only the available amount of partitions is returned, even if a higher count is used
    params.count = Some((partition_count + 1) as i32);
    let actual_count = dataset
        .partition_range(&mut test_db.db, &params)
        .await
        .unwrap();
    assert_eq!(actual_count.len(), partition_count as usize);

    // test offset parameter
    params = Default::default();
    params.offset = Some(20);
    let actual_count = dataset
        .partition_range(&mut test_db.db, &params)
        .await
        .unwrap();
    assert_eq!(
        actual_count.len(),
        partition_count - params.offset.unwrap() as usize
    );

    // test an offset higher than the available number of records
    params.offset = Some((partition_count * 2) as i32);
    let actual_count = dataset
        .partition_range(&mut test_db.db, &params)
        .await
        .unwrap();
    assert_eq!(actual_count.len(), 0);

    // test start + end parameters
    params = Default::default();
    params.start = Some(midpoint);
    // use an end date somewhere near the tail of the full set of partitions
    params.end = Some(partitions_all.get(partition_count - 3).unwrap().created_at);
    let actual_count = dataset
        .partition_range(&mut test_db.db, &params)
        .await
        .unwrap();
    let end = params.end.unwrap();
    let start = params.start.unwrap();
    for p in actual_count.iter() {
        assert!(p.created_at.le(&end));
        assert!(p.created_at.ge(&start));
    }

    // test start + end + count parameters
    params.count = Some(3);
    let actual_count_limit = dataset
        .partition_range(&mut test_db.db, &params)
        .await
        .unwrap();
    assert_eq!(actual_count_limit.len(), 3 as usize);

    // test start + end + count + offset parameters
    params.offset = Some(1);
    let actual_count_limit_offset = dataset
        .partition_range(&mut test_db.db, &params)
        .await
        .unwrap();
    assert!(actual_count_limit
        .get(0)
        .unwrap()
        .created_at
        .lt(&actual_count_limit_offset.get(0).unwrap().created_at));

    // test start + end + offset
    params.count = None;
    let actual_count_offset = dataset
        .partition_range(&mut test_db.db, &params)
        .await
        .unwrap();
    assert!(actual_count_limit
        .get(0)
        .unwrap()
        .created_at
        .lt(&actual_count_offset.get(0).unwrap().created_at));

    testutil::drop_test_db(&mut test_db).await.unwrap();
}

#[tokio::test]
async fn test_manager() {
    let mut test_db = testutil::new_test_db().await.unwrap();

    // insert a manager using an email and password
    let email = testutil::get_rand(Email);
    let password = testutil::get_rand(Password);

    // check that the manager value is ok and has expected fields set
    let registered = test_db
        .db
        .register_manager(&email, &password)
        .await
        .unwrap();
    assert_ne!(registered.api_key.to_string(), "");
    assert_ne!(registered.api_key, Uuid::nil());
    assert_ne!(registered.hash.len(), 0);
    assert_ne!(registered.salt, "");

    // find the known manager in the database
    let found = test_db.db.find_manager(&registered.api_key).await.unwrap();
    assert_eq!(found.id, registered.id);

    // expect finding manager with bad uuid to fail
    assert!(test_db.db.find_manager(&Uuid::default()).await.is_err());

    // set test email validation domain so validation fails
    // check that invalid email address patterns fail registration
    std::env::set_var("DD_MANAGER_EMAIL_DOMAIN", "test.com");
    let invalid = test_db
        .db
        .register_manager("bad@validation.com", "12345678")
        .await;
    assert!(invalid.is_err());

    // check that duplicate email registration attempts fail
    let dup = test_db.db.register_manager(&email, &password).await;
    assert!(dup.is_err());

    // check that an authentication check passes, and the same values are maintained
    let authed = test_db.db.auth_manager(&email, &password).await.unwrap();
    assert_eq!(registered.hash, authed.hash);
    assert_eq!(registered.api_key, authed.api_key);

    // check that invalid passwords fail authentication
    let invalid = test_db.db.auth_manager(&email, "invalidPassword").await;
    assert!(invalid.is_err());

    testutil::drop_test_db(&mut test_db).await.unwrap();
}

use serde_json;

#[tokio::test]
async fn test_dataset_from_config() {
    let mut test_db = testutil::new_test_db().await.unwrap();
    let manager = testutil::create_manager(&mut test_db).await.unwrap();
    let dd: &str = include_str!("json/dd.json");
    let config: DatasetConfig = serde_json::from_str(dd).unwrap();
    let dataset = manager
        .register_dataset(
            &mut test_db.db,
            config.name,
            config.compression,
            config.format,
            config.classification,
            config.schema,
            config.description,
        )
        .await
        .unwrap();

    assert_ne!(dataset.id, 0);
    for key_value in &[
        ("id", "integer"),
        ("merchant_name", "string"),
        ("mrr_cents", "integer"),
        ("churn_rate", "float"),
        ("subs_gained", "integer"),
        ("subs_lost", "integer"),
    ] {
        assert_eq!(
            dataset.schema.get(key_value.0).unwrap(),
            &Some(key_value.1.to_owned())
        );
    }

    // find the same dataset in the database and check its values
    let found = Dataset::find(&mut test_db.db, &dataset.name).await.unwrap();
    assert_eq!(found.name, dataset.name);
    assert_eq!(found.id, dataset.id);
    assert_eq!(found.schema, dataset.schema);

    testutil::drop_test_db(&mut test_db).await.unwrap();
}
