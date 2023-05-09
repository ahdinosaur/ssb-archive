use log::trace;
use sqlx::{query, sqlite::SqliteRow, Error, Row, SqliteConnection};
use ssb_ref::BlobRef;

pub async fn find_or_create_blob_ref(
    connection: &mut SqliteConnection,
    blob_ref: &BlobRef,
) -> Result<i64, Error> {
    let result: Option<i64> = query("SELECT id FROM blob_refs WHERE blob_ref = ?")
        .bind(Into::<String>::into(blob_ref))
        .map(|row: SqliteRow| row.get(0))
        .fetch_optional(&mut *connection)
        .await?;

    if let Some(found_blob) = result {
        Ok(found_blob)
    } else {
        let created_blob = query("INSERT INTO blob_refs (blob_ref) VALUES (?)")
            .bind(Into::<String>::into(blob_ref))
            .execute(&mut *connection)
            .await?;

        Ok(created_blob.last_insert_rowid())
    }
}

pub async fn create_blob_refs_tables(connection: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating blob_refs tables");

    query(
        "
        CREATE TABLE IF NOT EXISTS blob_refs (
            id INTEGER PRIMARY KEY,
            blob_ref TEXT UNIQUE NOT NULL
        )
        ",
    )
    .execute(connection)
    .await?;

    Ok(())
}
