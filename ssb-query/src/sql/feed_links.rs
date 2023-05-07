use log::trace;
use sqlx::{query, Error, SqliteConnection};
use ssb_core::FeedKey;

use crate::sql::*;

pub async fn create_feed_links_tables(connection: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating feed_links tables");

    query(
        "CREATE TABLE IF NOT EXISTS feed_links_raw (
          id INTEGER PRIMARY KEY,
          link_from_key_id INTEGER,
          link_to_feed_key_id INTEGER
        )",
    )
    .execute(connection)
    .await?;

    Ok(())
}

pub async fn insert_feed_links(
    connection: &mut SqliteConnection,
    links: &[&FeedKey],
    message_key_id: i64,
) -> Result<(), Error> {
    for link in links {
        let link_id = find_or_create_feed_key(&mut *connection, link).await?;
        query("INSERT INTO feed_links_raw (link_from_key_id, link_to_feed_key_id) VALUES (?, ?)")
            .bind(message_key_id)
            .bind(link_id)
            .execute(&mut *connection)
            .await?;
    }

    Ok(())
}

pub async fn create_feed_links_indices(connection: &mut SqliteConnection) -> Result<(), Error> {
    create_feed_links_to_index(connection).await
}

async fn create_feed_links_to_index(conn: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating feed_links index");
    query(
        "CREATE INDEX IF NOT EXISTS feed_links_id_to_index on feed_links_raw (link_to_feed_key_id, link_from_key_id)",
    )
    .execute(&mut *conn)
    .await?;

    query(
        "CREATE INDEX IF NOT EXISTS feed_links_id_from_index on feed_links_raw (link_from_key_id, link_to_feed_key_id)",
    )
    .execute(&mut *conn)
    .await?;

    Ok(())
}
