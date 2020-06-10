use data_dictionary::db::Db;
use data_dictionary::error::Error;

mod migrate {
    use refinery::embed_migrations as embed;
    embed!("migrations");
}

pub fn reset_db(conn: &mut Db) -> Result<(), Error> {
    clear_db(conn)?;
    migrate_db(conn)?;

    Ok(())
}

pub fn migrate_db(conn: &mut Db) -> Result<(), Error> {
    migrate::migrations::runner()
        .run(&mut conn.client)
        .map_err(|e| Error::Generic(Box::new(e)))?;

    Ok(())
}

pub fn clear_db(conn: &mut Db) -> Result<(), Error> {
    conn.client
        .batch_execute(include_str!("sql/drop_all.sql"))
        .map_err(|e| Error::Generic(Box::new(e)))
}

pub fn new_test_db() -> Result<Db, Error> {
    let mut db = Db::connect(None)?;
    reset_db(&mut db)?;

    Ok(db)
}
