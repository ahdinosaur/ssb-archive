use log::trace;
use sqlx::{query, Error, SqliteConnection};
use ssb_ref::BlobRef;

use crate::sql::*;

pub async fn create_blob_links_tables(connection: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating blob_links tables");

    query(
        "CREATE TABLE IF NOT EXISTS blob_links_raw (
          id INTEGER PRIMARY KEY,
          link_from_msg_ref_id INTEGER,
          link_to_blob_ref_id INTEGER
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
            blob_links_raw.link_from_msg_ref_id as link_from_msg_ref_id, 
            blob_links_raw.link_to_blob_ref_id as link_to_blob_ref_id, 
            msg_refs.ref as link_from_ref, 
            blob_refs.blob as link_to_blob
        FROM blob_links_raw 
        JOIN msg_refs ON msg_refs.id = blob_links_raw.link_from_msg_ref_id
        JOIN blob_refs ON blob_refs.id = blob_links_raw.link_to_blob_ref_id
        ",
    )
    .execute(connection)
    .await?;

    Ok(())
}

pub async fn insert_blob_links(
    connection: &mut SqliteConnection,
    blob_refs: &[&BlobRef],
    msg_ref_id: i64,
) -> Result<(), Error> {
    for blob_ref in blob_refs.iter() {
        let blob_ref_id = find_or_create_blob_ref(&mut *connection, blob_ref).await?;
        query(
            "INSERT INTO blob_links_raw (link_from_msg_ref_id, link_to_blob_ref_id) VALUES (?, ?)",
        )
        .bind(&msg_ref_id)
        .bind(&blob_ref_id)
        .execute(&mut *connection)
        .await?;
    }

    Ok(())
}

pub async fn create_blob_links_indices(connection: &mut SqliteConnection) -> Result<(), Error> {
    create_blob_links_to_index(&mut *connection).await;
    create_blob_links_from_index(&mut *connection).await;

    Ok(())
}

async fn create_blob_links_to_index(conn: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating blob links to index");
    query("CREATE INDEX IF NOT EXISTS blob_links_to_blob_ref_id_index on blob_links_raw (link_to_blob_ref_id)")
        .execute(&mut *conn)
        .await?;

    Ok(())
}

async fn create_blob_links_from_index(conn: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating blob links from index");
    query("CREATE INDEX IF NOT EXISTS blob_links_from_msg_ref_id_index on blob_links_raw (link_from_msg_ref_id)")
        .execute(&mut *conn)
        .await?;

    Ok(())
}
