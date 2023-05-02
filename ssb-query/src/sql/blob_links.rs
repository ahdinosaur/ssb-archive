use log::trace;
use sqlx::{query, Error, SqliteConnection};

use crate::sql::*;

pub fn create_blob_links_tables(connection: &mut SqliteConnection) -> Result<usize, Error> {
    trace!("Creating blob_links tables");

    connection.execute(
        "CREATE TABLE IF NOT EXISTS blob_links_raw (
          id INTEGER PRIMARY KEY,
          link_from_key_id INTEGER,
          link_to_blob_id INTEGER
        )",
        (),
    )
}

pub fn create_blob_links_views(connection: &mut SqliteConnection) -> Result<usize, Error> {
    connection.execute(
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
        (),
    )
}

pub fn insert_blob_links(
    connection: &mut SqliteConnection,
    links: &[&serde_json::Value],
    message_key_id: i64,
) {
    let mut insert_link_stmt = connection
        .prepare_cached(
            "INSERT INTO blob_links_raw (link_from_key_id, link_to_blob_id) VALUES (?, ?)",
        )
        .unwrap();

    links
        .iter()
        .filter(|link| link.is_string())
        .map(|link| link.as_str().unwrap())
        .filter(|link| link.starts_with('&'))
        .map(|link| find_or_create_blob(&mut SqliteConnection, link).unwrap())
        .for_each(|link_id| {
            insert_link_stmt
                .execute(&[&message_key_id, &link_id])
                .unwrap();
        });
}

pub fn create_blob_links_indices(connection: &mut SqliteConnection) -> Result<usize, Error> {
    create_blob_links_index(connection)
}

fn create_blob_links_index(conn: &mut SqliteConnection) -> Result<usize, Error> {
    trace!("Creating blob links index");
    conn.execute(
        "CREATE INDEX IF NOT EXISTS blob_links_index_to on blob_links_raw (link_to_blob_id)",
        (),
    )?;
    conn.execute(
        "CREATE INDEX IF NOT EXISTS blob_links_index_from on blob_links_raw (link_from_key_id)",
        (),
    )
}
