use log::trace;
use sqlx::{query, Error, SqliteConnection};
use ssb_core::MsgKey;

use crate::sql::*;

pub async fn create_msg_links_tables(connection: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating msg_links tables");

    query(
        "CREATE TABLE IF NOT EXISTS msg_links_raw (
          id INTEGER PRIMARY KEY,
          link_from_key_id INTEGER,
          link_to_key_id INTEGER
        )",
    )
    .execute(connection)
    .await?;

    Ok(())
}

pub async fn create_msg_links_views(connection: &mut SqliteConnection) -> Result<(), Error> {
    query(
        "
        CREATE VIEW IF NOT EXISTS msg_links AS
        SELECT 
        msg_links_raw.id as id, 
        msg_links_raw.link_from_key_id as link_from_key_id, 
        msg_links_raw.link_to_key_id as link_to_key_id, 
        msg_keys.key as link_from_key, 
        msg_keys2.key as link_to_key
        FROM msg_links_raw 
        JOIN msg_keys ON msg_keys.id=msg_links_raw.link_from_key_id
        JOIN msg_keys AS msg_keys2 ON msg_keys2.id=msg_links_raw.link_to_key_id
        ",
    )
    .execute(connection)
    .await?;

    Ok(())
}

pub async fn insert_links(
    connection: &mut SqliteConnection,
    links: &[&MsgKey],
    message_key_id: i64,
) -> Result<(), Error> {
    for link in links {
        let link_id = find_or_create_msg_key(&mut *connection, link).await?;
        query("INSERT INTO msg_links_raw (link_from_key_id, link_to_key_id) VALUES (?, ?)")
            .bind(message_key_id)
            .bind(link_id)
            .execute(&mut *connection)
            .await?;
    }

    Ok(())
}

pub async fn create_msg_links_indices(connection: &mut SqliteConnection) -> Result<(), Error> {
    create_msg_links_to_index(&mut *connection).await?;
    create_msg_links_from_index(&mut *connection).await?;

    Ok(())
}

async fn create_msg_links_to_index(conn: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating links index");
    query(
        "CREATE INDEX IF NOT EXISTS links_to_id_index on msg_links_raw (link_to_key_id, link_from_key_id)",
    )
    .execute(conn)
    .await?;

    Ok(())
}

async fn create_msg_links_from_index(conn: &mut SqliteConnection) -> Result<(), Error> {
    query(
        "CREATE INDEX IF NOT EXISTS links_from_id_index on msg_links_raw (link_from_key_id, link_to_key_id)",
    )
    .execute(conn)
    .await?;

    Ok(())
}
