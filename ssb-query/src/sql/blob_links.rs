use log::trace;
use sqlx::{query, Error, SqliteConnection};

use crate::sql::*;

pub async fn create_blob_links_tables(connection: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating blob_links tables");

    query(
        "CREATE TABLE IF NOT EXISTS blob_links_raw (
          id INTEGER PRIMARY KEY,
          link_from_key_id INTEGER,
          link_to_blob_id INTEGER
        )",
    )
    .execute(connection)
    .await?;

    Ok(())
}

pub async fn create_blob_links_views(connection: &mut SqliteConnection) -> Result<(), Error> {
    query(
        "
        CREATE VIEW IF NOT EXISTS blob_links AS
        SELECT 
        blob_links_raw.id as id, 
        blob_links_raw.link_from_key_id as link_from_key_id, 
        blob_links_raw.link_to_blob_id as link_to_blob_id, 
        keys.key as link_from_key, 
        blobs.blob as link_to_blob
        FROM blob_links_raw 
        JOIN keys ON keys.id=blob_links_raw.link_from_key_id
        JOIN blobs ON blobs.id=blob_links_raw.link_to_blob_id
        ",
    )
    .execute(connection)
    .await?;

    Ok(())
}

pub async fn insert_blob_links(
    connection: &mut SqliteConnection,
    links: &[&serde_json::Value],
    message_key_id: i64,
) -> Result<(), Error> {
    for link in links
        .iter()
        .filter(|link| link.is_string())
        .map(|link| link.as_str().unwrap())
        .filter(|link| link.starts_with('&'))
    {
        let link_id = find_or_create_blob(&mut *connection, link).await?;
        query("INSERT INTO blob_links_raw (link_from_key_id, link_to_blob_id) VALUES (?, ?)")
            .bind(&message_key_id)
            .bind(&link_id)
            .execute(&mut *connection)
            .await?;
    }

    Ok(())
}

pub async fn create_blob_links_indices(connection: &mut SqliteConnection) -> Result<(), Error> {
    create_blob_links_index(connection).await
}

async fn create_blob_links_index(conn: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating blob links index");
    query("CREATE INDEX IF NOT EXISTS blob_links_index_to on blob_links_raw (link_to_blob_id)")
        .execute(&mut *conn)
        .await?;
    query("CREATE INDEX IF NOT EXISTS blob_links_index_from on blob_links_raw (link_from_key_id)")
        .execute(&mut *conn)
        .await?;

    Ok(())
}
