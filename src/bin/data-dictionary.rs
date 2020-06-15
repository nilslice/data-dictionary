use data_dictionary::db::Db;
use data_dictionary::error::Error;

fn main() -> Result<(), Error> {
    println!("running: src/bin/data-dictionary");
    let mut db = Db::connect(None)?;
    db.migrate()?;
    Ok(())
}
