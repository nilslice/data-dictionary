use data_dictionary::db::Db;
use data_dictionary::error::Error;
use data_dictionary::pubsub::{subscribe, Subscription};

#[tokio::main]
async fn main() -> Result<(), Error> {
    println!("running: src/bin/data-dictionary");
    let mut db = Db::connect(None).await?;
    db.migrate().await?;

    subscribe(&Subscription::from_env()).await?;
    Ok(())
}
