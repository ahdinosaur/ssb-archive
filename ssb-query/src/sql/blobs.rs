use log::trace;
use sqlx::{query, sqlite::SqliteRow, Error, Row, SqliteConnection};

pub async fn find_or_create_blob(
    connection: &mut SqliteConnection,
    blob: &str,
) -> Result<i64, Error> {
    let result: Option<i64> = query("SELECT id FROM blobs WHERE blob=?")
        .bind(blob)
        .map(|row: SqliteRow| row.get(0))
        .fetch_optional(&mut *connection)
        .await?;

    if let Some(found_blob) = result {
        Ok(found_blob)
    } else {
        let created_blob = query("INSERT INTO blobs (blob) VALUES (?)")
            .bind(blob)
            .execute(&mut *connection)
            .await?;

        Ok(created_blob.last_insert_rowid())
    }
}

pub async fn create_blobs_tables(connection: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating blobs tables");

    query(
        "CREATE TABLE IF NOT EXISTS blobs (
          id INTEGER PRIMARY KEY,
          blob TEXT UNIQUE
        )",
    )
    .execute(connection)
    .await?;

    Ok(())
}
