use log::trace;
use serde_json::Value;
use sqlx::{query, Error, SqliteConnection};

use crate::sql::*;

pub async fn create_links_tables(connection: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating links tables");

    query(
        "CREATE TABLE IF NOT EXISTS links_raw (
          id INTEGER PRIMARY KEY,
          link_from_key_id INTEGER,
          link_to_key_id INTEGER
        )",
    )
    .execute(connection)
    .await?;

    Ok(())
}

pub async fn create_links_views(connection: &mut SqliteConnection) -> Result<(), Error> {
    query(
        "
        CREATE VIEW IF NOT EXISTS links AS
        SELECT 
        links_raw.id as id, 
        links_raw.link_from_key_id as link_from_key_id, 
        links_raw.link_to_key_id as link_to_key_id, 
        keys.key as link_from_key, 
        keys2.key as link_to_key
        FROM links_raw 
        JOIN keys ON keys.id=links_raw.link_from_key_id
        JOIN keys AS keys2 ON keys2.id=links_raw.link_to_key_id
        ",
    )
    .execute(connection)
    .await?;

    Ok(())
}

pub async fn insert_links(
    connection: &mut SqliteConnection,
    links: &[&Value],
    message_key_id: i64,
) -> Result<(), Error> {
    for link in links
        .iter()
        .filter_map(|link| link.as_str())
        .filter(|link| link.starts_with('%'))
    {
        let link_id = find_or_create_key(connection, link).await?;
        query("INSERT INTO links_raw (link_from_key_id, link_to_key_id) VALUES (?, ?)")
            .bind(message_key_id)
            .bind(link_id)
            .execute(connection)
            .await?;
    }

    Ok(())
}

pub async fn create_links_indices(connection: &mut SqliteConnection) -> Result<(), Error> {
    create_links_to_index(connection).await
}

async fn create_links_to_index(conn: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating links index");
    query(
        "CREATE INDEX IF NOT EXISTS links_to_id_index on links_raw (link_to_key_id, link_from_key_id)",
    )
    .execute(&mut *conn)
    .await?;

    query(
        "CREATE INDEX IF NOT EXISTS links_from_id_index on links_raw (link_from_key_id, link_to_key_id)",
    )
    .execute(&mut *conn)
    .await?;

    Ok(())
}
