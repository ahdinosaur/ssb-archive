use log::trace;
use serde_json::Value;
use sqlx::{query, Error, SqliteConnection};
use ssb_msg::Msg;

use crate::sql::*;

pub async fn insert_msg(
    connection: &mut SqliteConnection,
    msg: &Msg<Value>,
    log_seq: i64,
    msg_ref_id: i64,
    root_msg_ref_id: Option<i64>,
    fork_msg_ref_id: Option<i64>,
    is_decrypted: bool,
) -> Result<(), Error> {
    trace!("find or create feed_ref");
    let feed_ref_id = find_or_create_feed_ref(connection, &msg.value.author).await?;

    trace!("insert msg");
    query(
        "INSERT INTO msgs (
            msg_ref_id,
            log_seq,
            feed_seq,
            timestamp_received,
            timestamp_asserted,
            root_msg_ref_id,
            fork_msg_ref_id,
            feed_ref_id,
            content_type,
            content,
            is_decrypted
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(msg_ref_id)
    .bind(log_seq)
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
    trace!("Creating msgs tables");
    query(
        "CREATE TABLE IF NOT EXISTS msgs (
          msg_ref_id INTEGER UNIQUE, 
          log_seq INTEGER PRIMARY KEY,
          feed_seq INTEGER,
          timestamp_received REAL,
          timestamp_asserted REAL,
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
    query("CREATE INDEX IF NOT EXISTS msgs_feed_ref_id_index on msgs (feed_ref_id)")
        .execute(connection)
        .await?;

    Ok(())
}

async fn create_root_index(connection: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating root index");
    query("CREATE INDEX IF NOT EXISTS msgs_root_msg_ref_id_index on msgs (root_msg_ref_id)")
        .execute(connection)
        .await?;

    Ok(())
}

async fn create_fork_index(connection: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating fork index");
    query("CREATE INDEX IF NOT EXISTS msgs_fork_msg_ref_id_index on msgs (fork_msg_ref_id)")
        .execute(connection)
        .await?;

    Ok(())
}

async fn create_content_type_index(connection: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating content type index");
    query("CREATE INDEX IF NOT EXISTS msgs_content_type_index on msgs (content_type)")
        .execute(connection)
        .await?;

    Ok(())
}
