use data_dictionary::db::Db;
use data_dictionary::error::Error;

pub mod migrate {
    use refinery::embed_migrations as embed;
    embed!("migrations");
}

fn main() -> Result<(), Error> {
    println!("running: src/bin/data-dictionary");
    let mut db = Db::connect(None)?;
    migrate::migrations::runner()
        .run(&mut db.client)
        .map_err(|e| Error::Generic(Box::new(e)))?;
    Ok(())
}
