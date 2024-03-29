use log::trace;
use sqlx::{query, sqlite::SqliteRow, Error, Row, SqliteConnection};

const MIGRATION_VERSION_NUMBER: u32 = 1;

pub async fn create_migrations_tables(connection: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating migrations tables");

    query(
        "
        CREATE TABLE IF NOT EXISTS migrations (
            id INTEGER PRIMARY KEY,
            version INTEGER NOT NULL
        )
        ",
    )
    .execute(connection)
    .await?;

    Ok(())
}

pub async fn is_db_up_to_date(connection: &mut SqliteConnection) -> Result<bool, Error> {
    let result: Result<Option<u32>, Error> = query("SELECT version FROM migrations LIMIT 1")
        .map(|row: SqliteRow| row.get(0))
        .fetch_optional(connection)
        .await;

    match result {
        Ok(Some(version)) => Ok(version == MIGRATION_VERSION_NUMBER),
        Ok(None) => Ok(false),
        Err(_) => Ok(false),
    }
}

pub async fn set_db_version(connection: &mut SqliteConnection) -> Result<(), Error> {
    query("INSERT INTO migrations (id, version) VALUES(0, ?)")
        .bind(&MIGRATION_VERSION_NUMBER)
        .execute(connection)
        .await?;

    Ok(())
}
