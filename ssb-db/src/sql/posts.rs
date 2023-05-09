use log::trace;
use serde_json::Value;
use sqlx::{query, Error, SqliteConnection};
use ssb_msg::{Msg, PostContent};

use crate::sql::*;

pub async fn insert_post(
    connection: &mut SqliteConnection,
    _msg: &Msg<Value>,
    post: &PostContent,
    msg_ref_id: i64,
) -> Result<(), Error> {
    let root_msg_ref_id = if let Some(root) = post.root.clone() {
        trace!("find or create root key id");
        Some(find_or_create_msg_ref(connection, &root).await?)
    } else {
        None
    };
    let fork_msg_ref_id = if let Some(fork) = post.fork.clone() {
        trace!("find or create fork key id");
        Some(find_or_create_msg_ref(connection, &fork).await?)
    } else {
        None
    };

    trace!("insert post");
    query(
        "INSERT INTO posts (
            msg_ref_id,
            root_msg_ref_id,
            fork_msg_ref_id
        ) VALUES (?, ?, ?)",
    )
    .bind(msg_ref_id)
    .bind(root_msg_ref_id)
    .bind(fork_msg_ref_id)
    .execute(connection)
    .await?;

    Ok(())
}

pub async fn create_posts_tables(connection: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating posts tables");
    query(
        "CREATE TABLE IF NOT EXISTS posts (
          msg_ref_id INTEGER UNIQUE, 
          root_msg_ref_id INTEGER,
          fork_msg_ref_id INTEGER
        )",
    )
    .execute(connection)
    .await?;

    Ok(())
}

pub async fn create_posts_indices(connection: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating posts indices");

    create_msg_index(connection).await?;
    create_root_index(connection).await?;
    create_fork_index(connection).await?;

    Ok(())
}

async fn create_msg_index(connection: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating msg index");
    query("CREATE INDEX IF NOT EXISTS posts_msg_ref_id_index on posts (msg_ref_id)")
        .execute(connection)
        .await?;

    Ok(())
}

async fn create_root_index(connection: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating root index");
    query("CREATE INDEX IF NOT EXISTS posts_root_msg_ref_id_index on posts (root_msg_ref_id)")
        .execute(connection)
        .await?;

    Ok(())
}

async fn create_fork_index(connection: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating fork index");
    query("CREATE INDEX IF NOT EXISTS posts_fork_msg_ref_id_index on posts (fork_msg_ref_id)")
        .execute(connection)
        .await?;

    Ok(())
}
