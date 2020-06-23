use data_dictionary::db::Db;
use data_dictionary::error::Error;
use data_dictionary::pubsub::Subscription;

#[tokio::main]
async fn main() -> Result<(), Error> {
    println!("running: src/bin/data-dictionary");
    let mut db = Db::connect(None).await?;
    db.migrate().await?;

    let sub = Subscription::from_env().await?;
    println!("{} {}", sub.name(), sub.topic());
    loop {
        std::thread::sleep(std::time::Duration::from_millis(1000));
        println!("{:?}", sub.pull().await);
    }
    Ok(())
}
