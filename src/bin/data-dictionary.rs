use data_dictionary::db::Db;
use data_dictionary::error::Error;
use data_dictionary::pubsub::Subscription;
use data_dictionary::util;

#[tokio::main]
async fn main() -> Result<(), Error> {
    env_logger::init();
    log::info!("running: src/bin/data-dictionary");
    let mut db = Db::connect(None).await?;
    db.migrate().await?;

    let sub = Subscription::from_env().await?;
    log::info!("subscription '{}' created", sub.name());
    loop {
        std::thread::sleep(std::time::Duration::from_millis(1000));

        let resp = sub.pull().await?;
        if let Some(messages) = resp.received_messages {
            for msg in messages.iter() {
                util::handle_payload(&mut db, &msg.message.data, &msg.message.attributes).await?;
                sub.ack(&msg.ack_id).await?;
            }
        }
    }

    Ok(())
}
