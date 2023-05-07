use log::trace;
use serde_json::Value;
use sqlx::{query, Error, SqliteConnection};
use ssb_core::Msg;

use crate::sql::*;

pub async fn insert_msg(
    connection: &mut SqliteConnection,
    msg: &Msg<Value>,
    seq: i64,
    msg_key_id: i64,
    root_key_id: Option<i64>,
    fork_key_id: Option<i64>,
    is_decrypted: bool,
) -> Result<(), Error> {
    trace!("find or create feed_key");
    let feed_key_id = find_or_create_feed_key(connection, &msg.value.author).await?;

    trace!("insert msg");
    query("INSERT INTO msgs_raw (flume_seq, key_id, seq, received_time, asserted_time, root_id, fork_id, feed_key_id, content_type, content, is_decrypted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
        .bind(seq)
        .bind(msg_key_id)
        .bind(msg.value.sequence as i64)
        .bind(msg.timestamp_received)
        .bind(msg.value.timestamp_asserted)
        .bind(root_key_id)
        .bind(fork_key_id)
        .bind(feed_key_id)
        .bind(msg.value.content["type"].as_str())
        .bind(&msg.value.content)
        .bind(is_decrypted)
        .execute(connection)
        .await?;

    Ok(())
}

pub async fn create_msgs_tables(connection: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating msgs_raw tables");
    query(
        "CREATE TABLE IF NOT EXISTS msgs_raw (
          flume_seq INTEGER PRIMARY KEY,
          key_id INTEGER UNIQUE, 
          seq INTEGER,
          received_time REAL,
          asserted_time REAL,
          root_id INTEGER,
          fork_id INTEGER,
          feed_key_id INTEGER,
          content_type TEXT,
          content JSON,
          is_decrypted BOOLEAN
        )",
    )
    .execute(connection)
    .await?;

    Ok(())
}

pub async fn create_msgs_views(connection: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating msgs views");
    query(
        "
        CREATE VIEW IF NOT EXISTS msgs AS
        SELECT 
            flume_seq,
            key_id,
            seq,
            received_time,
            asserted_time,
            root_id,
            fork_id,
            feed_key_id,
            content,
            content_type,
            is_decrypted,
            msg_keys.key as key,
            root_keys.key as root,
            fork_keys.key as fork,
            feed_keys.feed_key as feed_key
        FROM msgs_raw 
        JOIN msg_keys ON msg_keys.id=msgs_raw.key_id
        LEFT JOIN msg_keys AS root_keys ON root_keys.id=msgs_raw.root_id
        LEFT JOIN msg_keys AS fork_keys ON fork_keys.id=msgs_raw.fork_id
        JOIN feed_keys ON feed_keys.id=msgs_raw.feed_key_id
        ",
    )
    .execute(connection)
    .await?;

    Ok(())
}

pub async fn create_msgs_indices(connection: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating msgs indices");

    create_content_type_index(connection).await?;
    create_root_index(connection).await?;
    create_fork_index(connection).await?;
    create_feed_key_index(connection).await?;

    Ok(())
}

async fn create_feed_key_index(connection: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating feed_key index");
    query("CREATE INDEX IF NOT EXISTS feed_key_id_index on msgs_raw (feed_key_id)")
        .execute(connection)
        .await?;

    Ok(())
}

async fn create_root_index(connection: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating root index");
    query("CREATE INDEX IF NOT EXISTS root_id_index on msgs_raw (root_id)")
        .execute(connection)
        .await?;

    Ok(())
}

async fn create_fork_index(connection: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating fork index");
    query("CREATE INDEX IF NOT EXISTS fork_id_index on msgs_raw (fork_id)")
        .execute(connection)
        .await?;

    Ok(())
}

async fn create_content_type_index(connection: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating content type index");
    query("CREATE INDEX IF NOT EXISTS content_type_index on msgs_raw (content_type)")
        .execute(connection)
        .await?;

    Ok(())
}
