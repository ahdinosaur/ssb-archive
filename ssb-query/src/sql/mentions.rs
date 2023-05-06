use log::trace;
use sqlx::{query, Error, SqliteConnection};
use ssb_core::FeedId;

use crate::sql::*;

pub async fn create_mentions_tables(connection: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating mentions tables");

    query(
        "CREATE TABLE IF NOT EXISTS mentions_raw (
          id INTEGER PRIMARY KEY,
          link_from_key_id INTEGER,
          link_to_author_id INTEGER
        )",
    )
    .execute(connection)
    .await?;

    Ok(())
}

pub async fn insert_mentions(
    connection: &mut SqliteConnection,
    links: &[&FeedId],
    message_key_id: i64,
) -> Result<(), Error> {
    for link in links {
        let link_id = find_or_create_author(&mut *connection, link).await?;
        query("INSERT INTO mentions_raw (link_from_key_id, link_to_author_id) VALUES (?, ?)")
            .bind(message_key_id)
            .bind(link_id)
            .execute(&mut *connection)
            .await?;
    }

    Ok(())
}

pub async fn create_mentions_views(connection: &mut SqliteConnection) -> Result<(), Error> {
    query(
        "
        CREATE VIEW IF NOT EXISTS mentions AS
        SELECT 
        mentions_raw.id as id, 
        mentions_raw.link_from_key_id as link_from_key_id, 
        mentions_raw.link_to_author_id as link_to_author_id, 
        keys.key as link_from, 
        authors.author as link_to,
        messages_raw.flume_seq as flume_seq
        FROM mentions_raw 
        JOIN keys ON keys.id = mentions_raw.link_from_key_id
        JOIN authors ON authors.id = mentions_raw.link_to_author_id
        JOIN messages_raw ON messages_raw.key_id = mentions_raw.link_from_key_id
        ",
    )
    .execute(connection)
    .await?;

    Ok(())
}

pub async fn create_mentions_indices(connection: &mut SqliteConnection) -> Result<(), Error> {
    create_mentions_to_index(connection).await
}

async fn create_mentions_to_index(conn: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating mentions index");
    query(
        "CREATE INDEX IF NOT EXISTS mentions_id_to_index on mentions_raw (link_to_author_id, link_from_key_id)",
    )
    .execute(&mut *conn)
    .await?;

    query(
        "CREATE INDEX IF NOT EXISTS mentions_id_from_index on mentions_raw (link_from_key_id, link_to_author_id)",
    )
    .execute(&mut *conn)
    .await?;

    Ok(())
}
