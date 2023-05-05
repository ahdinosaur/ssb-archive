use log::trace;
use sqlx::{query, sqlite::SqliteRow, Error, Row, SqliteConnection};

pub async fn find_or_create_key(
    connection: &mut SqliteConnection,
    key: &str,
) -> Result<i64, Error> {
    let result: Option<i64> = query("SELECT id FROM keys WHERE key=?1")
        .bind(key)
        .map(|row: SqliteRow| row.get(0))
        .fetch_optional(&mut *connection)
        .await?;

    if let Some(found_key) = result {
        Ok(found_key)
    } else {
        let created_key = query("INSERT INTO keys (key) VALUES (?)")
            .bind(key)
            .execute(&mut *connection)
            .await?;

        Ok(created_key.last_insert_rowid())
    }
}

pub async fn create_keys_tables(connection: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating messages tables");

    query(
        "CREATE TABLE IF NOT EXISTS keys (
          id INTEGER PRIMARY KEY,
          key TEXT UNIQUE
        )",
    )
    .execute(connection)
    .await?;

    Ok(())
}

pub async fn create_keys_indices(_connection: &mut SqliteConnection) -> Result<(), Error> {
    Ok(())
}
