use log::trace;
use sqlx::{query, Error, SqliteConnection};
use ssb_ref::MsgRef;

use crate::sql::*;

pub async fn insert_post_branches(
    connection: &mut SqliteConnection,
    post_branches: &[MsgRef],
    msg_ref_id: i64,
) -> Result<(), Error> {
    for branch in post_branches.iter() {
        let link_to_msg_ref_id = find_or_create_msg_ref(&mut *connection, branch).await?;
        query("INSERT INTO post_branches (link_from_msg_ref_id, link_to_msg_ref_id) VALUES (?, ?)")
            .bind(&msg_ref_id)
            .bind(&link_to_msg_ref_id)
            .execute(&mut *connection)
            .await?;
    }

    Ok(())
}

pub async fn create_post_branches_tables(connection: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating post_branches tables");

    query(
        "CREATE TABLE IF NOT EXISTS post_branches (
          id INTEGER PRIMARY KEY,
          link_from_msg_ref_id INTEGER,
          link_to_msg_ref_id INTEGER
        )",
    )
    .execute(connection)
    .await?;

    Ok(())
}

pub async fn create_post_branches_indices(_connection: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating post_branches indices");

    Ok(())
}
