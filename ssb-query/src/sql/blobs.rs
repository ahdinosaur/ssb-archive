use log::trace;
use sqlx::{query, sqlite::SqliteRow, Error, Row, SqliteConnection};
use ssb_core::BlobId;

pub async fn find_or_create_blob(
    connection: &mut SqliteConnection,
    blob_key: &BlobId,
) -> Result<i64, Error> {
    let result: Option<i64> = query("SELECT id FROM blobs WHERE blob=?")
        .bind(Into::<String>::into(blob_key))
        .map(|row: SqliteRow| row.get(0))
        .fetch_optional(&mut *connection)
        .await?;

    if let Some(found_blob) = result {
        Ok(found_blob)
    } else {
        let created_blob = query("INSERT INTO blobs (blob) VALUES (?)")
            .bind(Into::<String>::into(blob_key))
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
