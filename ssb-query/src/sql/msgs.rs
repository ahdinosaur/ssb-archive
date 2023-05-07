use log::trace;
use serde_json::Value;
use sqlx::{query, Error, SqliteConnection};
use ssb_msg::Msg;

use crate::sql::*;

pub async fn insert_msg(
    connection: &mut SqliteConnection,
    msg: &Msg<Value>,
    seq: i64,
    msg_ref_id: i64,
    root_msg_ref_id: Option<i64>,
    fork_msg_ref_id: Option<i64>,
    is_decrypted: bool,
) -> Result<(), Error> {
    trace!("find or create feed_ref");
    let feed_ref_id = find_or_create_feed_ref(connection, &msg.value.author).await?;

    trace!("insert msg");
    query(
        "INSERT INTO msgs_raw (
            flume_seq,
            msg_ref_id,
            seq,
            received_time,
            asserted_time,
            root_msg_ref_id,
            fork_msg_ref_id,
            feed_ref_id,
            content_type,
            content,
            is_decrypted
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(seq)
    .bind(msg_ref_id)
    .bind(msg.value.sequence as i64)
    .bind(msg.timestamp_received)
    .bind(msg.value.timestamp_asserted)
    .bind(root_msg_ref_id)
    .bind(fork_msg_ref_id)
    .bind(feed_ref_id)
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
          msg_ref_id INTEGER UNIQUE, 
          seq INTEGER,
          received_time REAL,
          asserted_time REAL,
          root_msg_ref_id INTEGER,
          fork_msg_ref_id INTEGER,
          feed_ref_id INTEGER,
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
            msg_ref_id,
            seq,
            received_time,
            asserted_time,
            root_msg_ref_id,
            fork_msg_ref_id,
            feed_ref_id,
            content,
            content_type,
            is_decrypted,
            msg_refs.msg_ref as msg_ref,
            root_msg_refs.msg_ref as root_msg_ref,
            fork_msg_refs.msg_ref as fork_msg_ref,
            feed_refs.feed_ref as feed_ref
        FROM msgs_raw 
        JOIN msg_refs ON msg_refs.id = msgs_raw.msg_ref_id
        LEFT JOIN msg_refs AS root_msg_refs ON root_msg_refs.id = msgs_raw.root_msg_ref_id
        LEFT JOIN msg_refs AS fork_msg_refs ON fork_msg_refs.id = msgs_raw.fork_msg_ref_id
        JOIN feed_refs ON feed_refs.id = msgs_raw.feed_ref_id
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
    create_feed_ref_index(connection).await?;

    Ok(())
}

async fn create_feed_ref_index(connection: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating feed_ref index");
    query("CREATE INDEX IF NOT EXISTS msgs_feed_ref_id_index on msgs_raw (feed_ref_id)")
        .execute(connection)
        .await?;

    Ok(())
}

async fn create_root_index(connection: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating root index");
    query("CREATE INDEX IF NOT EXISTS msgs_root_msg_ref_id_index on msgs_raw (root_msg_ref_id)")
        .execute(connection)
        .await?;

    Ok(())
}

async fn create_fork_index(connection: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating fork index");
    query("CREATE INDEX IF NOT EXISTS msgs_fork_msg_ref_id_index on msgs_raw (fork_msg_ref_id)")
        .execute(connection)
        .await?;

    Ok(())
}

async fn create_content_type_index(connection: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating content type index");
    query("CREATE INDEX IF NOT EXISTS msgs_content_type_index on msgs_raw (content_type)")
        .execute(connection)
        .await?;

    Ok(())
}
