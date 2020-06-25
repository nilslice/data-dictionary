use std::path::Path;

use crate::db::Db;
use crate::dict::Dataset;
use crate::error::Error;
use crate::pubsub::{Attributes, Event, Payload};

pub const FILENAME_DD_JSON: &str = "dd.json";

/// Payloads may represent either a dataset (object path + dd.json "DatasetConfig") or a new
/// partition which would have a path root equivalent to an existing dataset name.
pub async fn handle_payload(db: &mut Db, b64_data: &str, attrs: &Attributes) -> Result<(), Error> {
    let payload = base64_dec::<Payload>(b64_data)?;
    log::info!(
        "handling pubsub event: {:?}, object: {}",
        attrs.event_type,
        payload.name
    );

    let path = Path::new(&payload.name);
    let dataset = Dataset::find(db, dataset_name(path)?).await?;

    match attrs.event_type {
        Event::ObjectFinalize | Event::ObjectMetadataUpdate | Event::ObjectArchive => {
            if let Some(name) = partition_name(path)? {
                if let Err(e) = dataset
                    .register_partition(db, &name, payload.self_link)
                    .await
                {
                    log::error!(
                        "failed to register partition '{}' for dataset '{}': {}",
                        name,
                        dataset.name,
                        e
                    );
                }
            }

            Ok(())
        }
        Event::ObjectDelete => {
            // skip if the ObjectDelete event was sent from an overwrite operation
            if attrs.overwritten_by_generation.is_some() {
                return Ok(());
            }

            if let Some(name) = partition_name(path)? {
                if let Err(e) = dataset.delete_partition(db, &name).await {
                    log::error!(
                        "failed to delete partition '{}' from dataset '{}': {}",
                        name,
                        dataset.name,
                        e
                    );
                    return Err(e);
                }
                return Ok(());
            }

            let dataset_name = dataset.name.clone();
            if let Err(e) = dataset.delete(db).await {
                log::error!("failed to delete dataset '{}', error: {}", dataset_name, e);
                return Err(e);
            }
            return Ok(());
        }
    }
}

fn base64_dec<T: serde::de::DeserializeOwned>(data: &str) -> Result<T, Error> {
    let data = base64::decode(data).map_err(|e| Error::Generic(Box::new(e)))?;
    serde_json::from_slice(data.as_slice()).map_err(|e| Error::Generic(Box::new(e)))
}

fn dataset_name(path: &Path) -> Result<String, Error> {
    match path.components().nth(0) {
        Some(name) => {
            if let Some(s) = name.as_os_str().to_str() {
                Ok(s.to_string())
            } else {
                Err(Error::InputValidation(format!(
                    "bad input from pubsub, path: {:?}",
                    path
                )))
            }
        }
        None => Err(Error::InputValidation(format!(
            "bad input from pubsub, path contains no components: {:?}",
            path
        ))),
    }
}

fn partition_name(path: &Path) -> Result<Option<String>, Error> {
    match path.to_str() {
        Some(s) => {
            let dataset_name = dataset_name(path)?;
            let name = s.trim_start_matches(&format!("{}/", dataset_name)).into();
            if name == dataset_name || name == FILENAME_DD_JSON {
                Ok(None) // only a dataset name was found, no partition name
            } else {
                Ok(Some(name))
            }
        }
        None => Err(Error::InputValidation(format!(
            "bad input from pubsub, path has no string value: {:?}",
            path,
        ))),
    }
}

#[test]
fn test_dataset_partition_name() {
    let paths = &[
        ("example_dataset", "2020/03/25/some_partition.pb.tar.gz"),
        ("o7hrlkjbasd", "___pattern-1/23456/some_partition.pb.tar.gz"),
        ("w", "1_1__1___1_____1---3343-:some_partition.pb.tar.gz"),
        ("__r34-d--de-fsine3", "s"),
    ];
    for dataset_partition in paths {
        let test_dataset_name = dataset_partition.0;
        let test_partition_name = dataset_partition.1;
        let path = format!("{}/{}", test_dataset_name, test_partition_name);
        let path = Path::new(&path);
        assert_eq!(
            dataset_name(path).unwrap() as String,
            String::from(test_dataset_name)
        );
        assert_eq!(
            partition_name(path).unwrap().unwrap() as String,
            String::from(test_partition_name)
        );
        assert_eq!(partition_name(Path::new(test_dataset_name)).unwrap(), None);
    }
}
