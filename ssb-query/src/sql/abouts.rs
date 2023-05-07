use log::trace;
use serde_json::Value;
use sqlx::{query, Error, SqliteConnection};
use ssb_core::{AboutContent, LinkKey, Msg};

use crate::sql::*;

pub async fn create_abouts_tables(connection: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating abouts tables");

    query(
        "CREATE TABLE IF NOT EXISTS abouts_raw (
          id INTEGER PRIMARY KEY,
          link_from_key_id INTEGER,
          link_to_author_id INTEGER,
          link_to_key_id INTEGER
        )",
    )
    .execute(connection)
    .await?;

    Ok(())
}

pub async fn insert_abouts(
    connection: &mut SqliteConnection,
    msg: &Msg<Value>,
    content: &AboutContent,
    msg_key_id: i64,
) -> Result<(), Error> {
    let (link_to_feed_key_id, link_to_msg_key_id) = match &content.about {
        LinkKey::Feed(feed_key) => {
            let feed_key_id = find_or_create_feed_key(connection, feed_key).await?;
            (Some(feed_key_id), None)
        }
        LinkKey::Msg(msg_id) => {
            let msg_key_id = find_or_create_msg_key(connection, msg_id).await?;
            (None, Some(msg_key_id))
        }
        _ => (None, None),
    };

    query("INSERT INTO abouts_raw (link_from_key_id, link_to_author_id, link_to_key_id) VALUES (?, ?, ?)")
        .bind(&msg_key_id)
        .bind(&link_to_feed_key_id)
        .bind(&link_to_msg_key_id)
        .execute(connection)
        .await?;

    Ok(())
}

pub async fn create_abouts_indices(connection: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating abouts index");
    query("CREATE INDEX IF NOT EXISTS abouts_raw_from_index on abouts_raw (link_from_key_id)")
        .execute(&mut *connection)
        .await?;
    query("CREATE INDEX IF NOT EXISTS abouts_raw_key_index on abouts_raw (link_to_key_id)")
        .execute(&mut *connection)
        .await?;
    query("CREATE INDEX IF NOT EXISTS abouts_raw_author_index on abouts_raw (link_to_author_id )")
        .execute(&mut *connection)
        .await?;
    Ok(())
}

pub async fn create_abouts_views(connection: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating abouts views");
    //resolve all the links, get the content of the msg.
    query(
        "
        CREATE VIEW IF NOT EXISTS abouts AS
        SELECT 
            abouts_raw.id as id, 
            abouts_raw.link_from_key_id as link_from_key_id, 
            abouts_raw.link_to_key_id as link_to_key_id, 
            abouts_raw.link_to_author_id as link_to_author_id, 
            keys_from.key as link_from_key, 
            keys_to.key as link_to_key, 
            authors_to.author as link_to_author,
            msgs.content as content,
            msgs.author as link_from_author
        FROM abouts_raw 
        JOIN keys AS keys_from ON keys_from.id=abouts_raw.link_from_key_id
        JOIN msgs ON link_from_key_id=msgs.key_id
        LEFT JOIN keys AS keys_to ON keys_to.id=abouts_raw.link_to_key_id
        LEFT JOIN authors AS authors_to ON authors_to.id=abouts_raw.link_to_author_id
        ",
    )
    .execute(connection)
    .await?;

    Ok(())
}
