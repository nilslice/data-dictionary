use std::thread;
use std::time;

use data_dictionary::api;
use data_dictionary::db::{Db, PoolConfig};
use data_dictionary::error::{Error, PubsubAction};
use data_dictionary::pubsub::Subscriber;
use data_dictionary::util;

use actix_web::{web, App, HttpServer};
use tokio;

#[actix_rt::main]
async fn main() -> Result<(), Error> {
    env_logger::init();
    log::info!("running: src/bin/data-dictionary");
    let mut db = Db::connect(
        None, 
        Some(PoolConfig {
            min_idle: 5,
            max_size: 30,
        }),
    )
    .await?;
    db.migrate().await?;
    let apidb = db.clone();

    thread::spawn(|| {
        let runtime = tokio::runtime::Runtime::new();
        if let Ok(mut rt) = runtime {
            rt.block_on(async move {
                let sub = Subscriber::from_env().await.unwrap();
                log::info!("subscription '{}' created", sub.name());
                loop {
                    thread::sleep(time::Duration::from_millis(1000));

                    let resp = sub.pull().await.unwrap();
                    if let Some(messages) = resp.received_messages {
                        for msg in messages.iter() {
                            match util::handle_payload(
                                &mut db,
                                &msg.message.data,
                                &msg.message.attributes,
                            )
                            .await
                            {
                                Ok(_) => {
                                    if let Err(e) = sub.ack(&msg.ack_id).await {
                                        log::error!(
                                            "failed to ack pubsub message with ack_id '{}': {}",
                                            &msg.ack_id,
                                            e
                                        )
                                    }
                                }
                                Err(e) => {
                                    match e {
                                        Error::Pubsub(action) => match action {
                                            PubsubAction::IgnoreAndAck => {
                                                if let Err(e) = sub.ack(&msg.ack_id).await {
                                                    log::error!(
                                                        "failed to ack pubsub message with ack_id '{}': {}",
                                                        &msg.ack_id,
                                                        e
                                                    )
                                                } 
                                            }
                                        },
                                        _ => log::error!(
                                            "failed to handle event '{:?}' message_id = '{}', will be retried: {}",
                                            msg.message.attributes.event_type,
                                            msg.message.message_id,
                                            e
                                        )
                                    }
                                },
                            }
                        }
                    }
                }
            });
        } else {
            panic!(
                "failed to create pubsub runtime, messages will be queued: {}",
                runtime.err().expect("no runtime fail error")
            )
        }
    });

    let app = HttpServer::new(move || {
        let apidb = apidb.clone();
        App::new()
            .data(api::Server { db: apidb })
            .route("/datasets", web::get().to(api::list_datasets))
            .route(
                "/dataset/{dataset_name}/latest",
                web::get().to(api::latest_partition),
            )
            .route(
                "/dataset/{dataset_name}/{partition_name:.*}",
                web::get().to(api::find_partition),
            )
            .route("/dataset/register", web::post().to(api::register_dataset))
    });
    app.bind("127.0.0.1:8080").unwrap().run().await.unwrap();
    Ok(())
}
