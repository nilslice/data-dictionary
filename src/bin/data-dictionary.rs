use std::process;
use std::sync::Arc;
use std::thread;

use data_dictionary::api;
use data_dictionary::bucket::BucketManager;
use data_dictionary::db::{Db, PoolConfig};
use data_dictionary::error::Error;
use data_dictionary::pubsub_rt;

use actix_cors::Cors;
use actix_web::{web, App, HttpServer};
use tokio::runtime::Runtime;

#[actix_rt::main]
async fn main() -> Result<(), Error> {
    env_logger::init();

    let cfg = PoolConfig {
        min_idle: 5,
        max_size: 30,
    };
    let mut db = Db::connect(None, Some(cfg)).await?;
    db.migrate().await?;

    let pubsubdb = db.clone();
    thread::spawn(|| {
        let runtime = Runtime::new();
        if let Ok(rt) = runtime {
            pubsub_rt::start(rt, pubsubdb, 1000)
        } else {
            log::error!(
                "failed to create pubsub runtime, messages will be re-queued: {}",
                runtime.err().expect("no runtime fail error specified")
            );
            process::exit(1);
        }
    });

    let app = HttpServer::new(move || {
        let bucket_manager = BucketManager::from_env(Default::default());
        let apidb = db.clone();
        App::new()
            .wrap(Cors::new().send_wildcard().finish())
            .data(api::Server {
                db: apidb,
                bucket_manager: Arc::new(bucket_manager),
            })
            .route(
                "/api/manager/register",
                web::post().to(api::register_manager),
            )
            .route("/api/datasets", web::get().to(api::list_datasets))
            .route(
                "/api/dataset/{dataset_name}/latest",
                web::get().to(api::latest_partition),
            )
            .route(
                "/api/dataset/{dataset_name}/{partition_name:.*}",
                web::get().to(api::find_partition),
            )
            .route(
                "/api/dataset/{dataset_name}",
                web::get().to(api::find_dataset),
            )
            .route(
                "/api/dataset/register",
                web::post().to(api::register_dataset),
            )
    });
    app.bind("127.0.0.1:8080")?.run().await?;
    Ok(())
}
