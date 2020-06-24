use std::path::Path;

use data_dictionary::db::Db;
use data_dictionary::dict::Dataset;
use data_dictionary::error::Error;
use data_dictionary::pubsub::{base64_dec, Attributes, Event, Payload, Subscription};

#[tokio::main]
async fn main() -> Result<(), Error> {
    println!("running: src/bin/data-dictionary");
    let mut db = Db::connect(None).await?;
    db.migrate().await?;

    let sub = Subscription::from_env().await?;
    loop {
        std::thread::sleep(std::time::Duration::from_millis(1000));

        let resp = sub.pull().await?;
        if let Some(messages) = resp.received_messages {
            for msg in messages.iter() {
                let payload: Payload = base64_dec(&msg.message.data)?;
                handle_payload(&mut db, payload, &msg.message.attributes).await?;
                sub.ack(&msg.ack_id).await?;
            }
        }
    }
    Ok(())
}

/// Payloads may represent either a dataset (object path + dd.json "DatasetConfig") or a new
/// partition which would have a path dirname equivalent to an existing dataset name.
async fn handle_payload(db: &mut Db, p: Payload, attrs: &Attributes) -> Result<(), Error> {
    let path = Path::new(&p.name);
    let dataset = Dataset::find(db, dataset_name(path)?).await?;
    let partition_name = p.name;

    match attrs.event_type {
        Event::ObjectFinalize | Event::ObjectMetadataUpdate | Event::ObjectArchive => {
            // "upsert" a partition
            // if the object is found to be a dataset, log an error because a dataset should be
            // immutable to guarantee downstream users consistency.
            // TODO: fix so that {dataset}/dd.json is not stored as a partition
            println!(
                "new/update: dataset: {} partition: {}",
                dataset.name, partition_name
            );
            if let Err(e) = dataset
                .register_partition(db, partition_name, p.self_link)
                .await
            {
                println!("FAIL: {}", e);
            }
        }
        Event::ObjectDelete => {
            // delete a dataset or partition
            // skip if the ObjectDelete event was sent from an overwrite operation
            if let Some(_) = attrs.overwritten_by_generation {
                return Ok(());
            }

            println!(
                "delete: dataset: {} partition: {}",
                dataset.name, partition_name
            );
            if let Err(e) = Dataset::delete(db, &dataset).await {
                log::error!("failed to delete dataset '{}', error: {}", dataset.name, e);
            }
        }
    }

    // todo!()
    Ok(())
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
