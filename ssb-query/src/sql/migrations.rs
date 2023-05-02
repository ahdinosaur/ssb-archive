use log::trace;
use sqlx::{query, SqliteConnection, Error};

const MIGRATION_VERSION_NUMBER: u32 = 1;

pub async fn create_migrations_tables(connection: &mut SqliteConnection) -> Result<usize, Error> {
    trace!("Creating migrations tables");

    query(
        "CREATE TABLE IF NOT EXISTS migrations (
          id INTEGER PRIMARY KEY,
          version INTEGER
        )",
    )
    .execute(connection).await
}

pub async fn is_db_up_to_date(connection: &mut SqliteConnection) -> Result<bool, Error> {
    query
    connection
        .query_row_and_then("SELECT version FROM migrations LIMIT 1", (), |row| {
            row.get(0)
        })
        .map(|version: u32| version == MIGRATION_VERSION_NUMBER)
        .or(Ok(false))
}

pub fn set_db_version(connection: &mut SqliteConnection) -> Result<usize, Error> {
    connection.execute(
        "INSERT INTO migrations (id, version) VALUES(0, ?)",
        &[&MIGRATION_VERSION_NUMBER],
    )
}
