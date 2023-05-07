use log::trace;
use sqlx::{query, Error, SqliteConnection};
use ssb_core::MsgKey;

use crate::sql::*;

pub async fn insert_branches(
    connection: &mut SqliteConnection,
    branches: &[MsgKey],
    message_key_id: i64,
) -> Result<(), Error> {
    for branch in branches.iter() {
        let link_to_msg_key_id = find_or_create_msg_key(&mut *connection, branch).await?;
        query("INSERT INTO branches_raw (link_from_key_id, link_to_key_id) VALUES (?, ?)")
            .bind(&message_key_id)
            .bind(&link_to_msg_key_id)
            .execute(&mut *connection)
            .await?;
    }

    Ok(())
}

pub async fn create_branches_tables(connection: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating branches tables");

    query(
        "CREATE TABLE IF NOT EXISTS branches_raw (
          id INTEGER PRIMARY KEY,
          link_from_key_id INTEGER,
          link_to_key_id INTEGER
        )",
    )
    .execute(connection)
    .await?;

    Ok(())
}

pub async fn create_branches_indices(_connection: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating branches tables");

    Ok(())
}
