use data_dictionary::{db::Db, dict::DatasetConfig, dict::Manager, error::Error};

#[tokio::main]
async fn main() -> Result<(), Error> {
    let email = std::env::var("EMAIL").unwrap_or("nilslice@example.com".into());
    let password = std::env::var("PASSWORD").unwrap_or("lol1234".into());
    let num_datasets: usize = std::env::var("NUM_DATASETS")
        .unwrap_or("50".into())
        .parse()
        .unwrap();

    let mut db = Db::connect(None, None).await?;
    db.migrate().await?;

    if let Ok(manager) = Manager::authenticate(&mut db, email, password).await {
        println!(
            "Authenticated\nemail: {}\napi_key: {}",
            manager.email, manager.api_key
        );
        let cfg: DatasetConfig = serde_json::from_str(include_str!("data/dd.json")).unwrap();
        let mut datasets = vec![];
        let start = std::time::Instant::now();
        for i in 1..=num_datasets {
            let cfg = cfg.clone();
            let name = format!(
                "{}_{}_{}",
                &cfg.name,
                i,
                chrono::Utc::now()
                    .to_string()
                    .replace(":", "")
                    .replace("-", "")
                    .replace(".", "_")
                    .replace(" ", "_")
            );
            let dataset = manager
                .register_dataset(
                    &mut db,
                    &name,
                    cfg.compression,
                    cfg.format,
                    cfg.classification,
                    cfg.schema,
                    &cfg.description,
                )
                .await?;

            datasets.push(dataset);
        }
        println!(
            "Registered {} dataset(s) in {:?}",
            datasets.len(),
            std::time::Instant::now().duration_since(start)
        );
    }

    Ok(())
}
