use log::trace;
use serde_json::Value;
use sqlx::{query, Error, SqliteConnection};
use ssb_msg::{AboutContent, Msg};
use ssb_ref::LinkRef;

use crate::sql::*;

pub async fn create_abouts_tables(connection: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating abouts tables");

    query(
        "CREATE TABLE IF NOT EXISTS abouts_raw (
          id INTEGER PRIMARY KEY,
          link_from_msg_ref_id INTEGER,
          link_to_feed_ref_id INTEGER,
          link_to_msg_ref_id INTEGER
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
    msg_ref_id: i64,
) -> Result<(), Error> {
    let (link_to_feed_ref_id, link_to_msg_ref_id) = match &content.about {
        LinkRef::Feed(feed_ref) => {
            let feed_ref_id = find_or_create_feed_ref(connection, feed_ref).await?;
            (Some(feed_ref_id), None)
        }
        LinkRef::Msg(msg_ref) => {
            let msg_ref_id = find_or_create_msg_ref(connection, msg_ref).await?;
            (None, Some(msg_ref_id))
        }
        _ => (None, None),
    };

    query("INSERT INTO abouts_raw (link_from_msg_ref_id, link_to_feed_ref_id, link_to_msg_ref_id) VALUES (?, ?, ?)")
        .bind(&msg_ref_id)
        .bind(&link_to_feed_ref_id)
        .bind(&link_to_msg_ref_id)
        .execute(connection)
        .await?;

    Ok(())
}

pub async fn create_abouts_indices(connection: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating abouts index");
    query(
        "CREATE INDEX IF NOT EXISTS abouts_from_msg_ref_id_index on abouts_raw (link_from_msg_ref_id)",
    )
    .execute(&mut *connection)
    .await?;
    query(
        "CREATE INDEX IF NOT EXISTS abouts_to_msg_ref_id_index on abouts_raw (link_to_msg_ref_id)",
    )
    .execute(&mut *connection)
    .await?;
    query("CREATE INDEX IF NOT EXISTS abouts_to_feed_ref_id_index on abouts_raw (link_to_feed_ref_id )")
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
            abouts_raw.link_from_msg_ref_id as link_from_msg_ref_id, 
            abouts_raw.link_to_msg_ref_id as link_to_msg_ref_id, 
            abouts_raw.link_to_feed_ref_id as link_to_feed_ref_id, 
            msg_refs_from.msg_ref as link_from_msg_ref, 
            msg_refs_to.msg_ref as link_to_msg_ref, 
            feed_refs_to.feed_ref as link_to_feed_ref,
            msgs.content as content,
            msgs.feed_ref as link_from_feed_ref
        FROM abouts_raw 
        JOIN msg_refs AS msg_refs_from ON msg_refs_from.id = abouts_raw.link_from_msg_ref_id
        JOIN msgs ON link_from_msg_ref_id = msgs.msg_ref_id
        LEFT JOIN msg_refs AS msg_refs_to ON msg_refs_to.id = abouts_raw.link_to_msg_ref_id
        LEFT JOIN feed_refs AS feed_refs_to ON feed_refs_to.id=abouts_raw.link_to_feed_ref_id
        ",
    )
    .execute(connection)
    .await?;

    Ok(())
}
