use log::trace;
use serde_json::Value;
use sqlx::{query, Error, SqliteConnection};

use crate::sql::*;
use crate::Msg;

pub async fn insert_message(
    connection: &mut SqliteConnection,
    message: &Msg,
    seq: i64,
    message_key_id: i64,
    is_decrypted: bool,
) -> Result<(), Error> {
    trace!("get root key id");
    let root_key_id = match message.value.content["root"] {
        Value::String(ref key) => {
            let id = find_or_create_key(connection, key).await?;
            Some(id)
        }
        _ => None,
    };

    trace!("get fork key id");
    let fork_key_id = match message.value.content["fork"] {
        Value::String(ref key) => {
            let id = find_or_create_key(connection, key).await?;
            Some(id)
        }
        _ => None,
    };

    trace!("find or create author");
    let author_id = find_or_create_author(connection, &message.value.author).await?;

    trace!("insert message");
    query("INSERT INTO messages_raw (flume_seq, key_id, seq, received_time, asserted_time, root_id, fork_id, author_id, content_type, content, is_decrypted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
        .bind(seq)
        .bind(message_key_id)
        .bind(message.value.sequence)
        .bind(message.timestamp)
        .bind(message.value.timestamp)
        .bind(root_key_id)
        .bind(fork_key_id)
        .bind(author_id)
        .bind(message.value.content["type"].as_str())
        .bind(&message.value.content)
        .bind(is_decrypted)
        .execute(connection)
        .await?;

    Ok(())
}

pub async fn create_messages_tables(connection: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating messages tables");
    query(
        "CREATE TABLE IF NOT EXISTS messages_raw (
          flume_seq INTEGER PRIMARY KEY,
          key_id INTEGER UNIQUE, 
          seq INTEGER,
          received_time REAL,
          asserted_time REAL,
          root_id INTEGER,
          fork_id INTEGER,
          author_id INTEGER,
          content_type TEXT,
          content JSON,
          is_decrypted BOOLEAN
        )",
    )
    .execute(connection)
    .await?;

    Ok(())
}

pub async fn create_messages_views(connection: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating messages views");
    query(
        "
        CREATE VIEW IF NOT EXISTS messages AS
        SELECT 
        flume_seq,
        key_id,
        seq,
        received_time,
        asserted_time,
        root_id,
        fork_id,
        author_id,
        content,
        content_type,
        is_decrypted,
        keys.key as key,
        root_keys.key as root,
        fork_keys.key as fork,
        authors.author as author
        FROM messages_raw 
        JOIN keys ON keys.id=messages_raw.key_id
        LEFT JOIN keys AS root_keys ON root_keys.id=messages_raw.root_id
        LEFT JOIN keys AS fork_keys ON fork_keys.id=messages_raw.fork_id
        JOIN authors ON authors.id=messages_raw.author_id
        ",
    )
    .execute(connection)
    .await?;

    Ok(())
}

pub async fn create_messages_indices(connection: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating messages indices");

    create_content_type_index(connection).await?;
    create_root_index(connection).await?;
    create_fork_index(connection).await?;
    create_author_index(connection).await?;

    Ok(())
}

async fn create_author_index(connection: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating author index");
    query("CREATE INDEX IF NOT EXISTS author_id_index on messages_raw (author_id)")
        .execute(connection)
        .await?;

    Ok(())
}

async fn create_root_index(connection: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating root index");
    query("CREATE INDEX IF NOT EXISTS root_id_index on messages_raw (root_id)")
        .execute(connection)
        .await?;

    Ok(())
}

async fn create_fork_index(connection: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating fork index");
    query("CREATE INDEX IF NOT EXISTS fork_id_index on messages_raw (fork_id)")
        .execute(connection)
        .await?;

    Ok(())
}

async fn create_content_type_index(connection: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating content type index");
    query("CREATE INDEX IF NOT EXISTS content_type_index on messages_raw (content_type)")
        .execute(connection)
        .await?;

    Ok(())
}
