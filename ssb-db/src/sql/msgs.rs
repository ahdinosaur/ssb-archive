use log::trace;
use serde_json::Value;
use sqlx::{query, sqlite::SqliteRow, Error, Row, SqliteConnection};
use ssb_msg::Msg;
use ssb_ref::MsgRef;

use crate::sql::*;

pub async fn insert_msg(
    connection: &mut SqliteConnection,
    msg: &Msg<Value>,
    log_seq: &Sequence,
    msg_ref_id: i64,
    is_encrypted: bool,
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
            feed_ref_id,
            content_type,
            is_encrypted,
            is_decrypted
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(msg_ref_id)
    .bind(*log_seq as i64)
    .bind(msg.value.sequence as i64)
    .bind(msg.timestamp_received)
    .bind(msg.value.timestamp_asserted)
    .bind(feed_ref_id)
    .bind(msg.value.content.get("type").map(|v| v.as_str()))
    .bind(is_encrypted)
    .bind(is_decrypted)
    .execute(connection)
    .await?;

    Ok(())
}

pub async fn get_msg_log_seq(
    connection: &mut SqliteConnection,
    msg_ref: &MsgRef,
) -> Result<Option<Sequence>, Error> {
    let log_seq = query(
        "
        SELECT log_seq
        FROM msgs
        JOIN msg_refs ON msg_refs.id = msgs.msg_ref_id
        WHERE
          msg_refs.msg_ref = ?1
        ",
    )
    .bind(Into::<String>::into(msg_ref))
    .map(|row: SqliteRow| row.get::<i64, _>(0) as Sequence)
    .fetch_optional(connection)
    .await?;

    Ok(log_seq)
}

pub async fn create_msgs_tables(connection: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating msgs tables");
    query(
        "CREATE TABLE IF NOT EXISTS msgs (
          msg_ref_id INTEGER UNIQUE, 
          log_seq INTEGER PRIMARY KEY,
          feed_ref_id INTEGER,
          feed_seq INTEGER,
          timestamp_received REAL,
          timestamp_asserted REAL,
          content_type TEXT,
          is_encrypted BOOLEAN,
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

async fn create_content_type_index(connection: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating content type index");
    query("CREATE INDEX IF NOT EXISTS msgs_content_type_index on msgs (content_type)")
        .execute(connection)
        .await?;

    Ok(())
}
