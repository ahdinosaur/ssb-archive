use log::trace;
use serde_json::Value;
use sqlx::{query, Error, SqliteConnection};
use ssb_msg::VoteContent;

use crate::sql::*;

pub async fn create_votes_tables(connection: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating votes tables");

    query(
        "
        CREATE TABLE IF NOT EXISTS votes (
            id INTEGER PRIMARY KEY,
            feed_seq INTEGER NOT NULL,
            link_from_feed_ref_id INTEGER NOT NULL,
            link_to_msg_ref_id INTEGER NOT NULL,
            value INTEGER NOT NULL,
            FOREIGN KEY (link_from_feed_ref_id)
                REFERENCES feed_refs (id)
                ON UPDATE RESTRICT
                ON DELETE RESTRICT,
            FOREIGN KEY (link_to_msg_ref_id)
                REFERENCES msg_refs (id)
                ON UPDATE RESTRICT
                ON DELETE RESTRICT
        )
        ",
    )
    .execute(connection)
    .await?;

    Ok(())
}

pub async fn insert_or_update_votes(
    connection: &mut SqliteConnection,
    msg: &Msg<Value>,
    content: &VoteContent,
) -> Result<(), Error> {
    let link_from_feed_ref_id = find_or_create_feed_ref(connection, &msg.value.author).await?;
    let link_to_msg_ref_id = find_or_create_msg_ref(connection, &content.vote.link).await?;

    let row: Option<(i64, i64)> = query(
        "SELECT id, feed_seq FROM votes WHERE link_from_feed_ref_id = ? AND link_to_msg_ref_id = ?",
    )
    .bind(&link_from_feed_ref_id)
    .bind(&link_to_msg_ref_id)
    .map(|row: sqlx::sqlite::SqliteRow| (row.get(0), row.get(1)))
    .fetch_optional(&mut *connection)
    .await?;

    if let Some((id, feed_seq)) = row {
        if feed_seq < msg.value.sequence as i64 {
            query("UPDATE votes SET value = ? WHERE id = ?")
                .bind(content.vote.value)
                .bind(id)
                .execute(connection)
                .await?;
        }
    } else {
        query(
            "INSERT INTO votes (feed_seq, link_from_feed_ref_id, link_to_msg_ref_id, value) VALUES (?, ?, ?, ?)",
        )
        .bind(msg.value.sequence as i64)
        .bind(&link_from_feed_ref_id)
        .bind(&link_to_msg_ref_id)
        .bind(content.vote.value)
        .execute(connection)
        .await?;
    }

    Ok(())
}

pub async fn create_votes_indices(connection: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating votes indices");
    query(
        "CREATE INDEX IF NOT EXISTS votes_link_from_feed_ref_id_index on votes (link_from_feed_ref_id)",
    )
    .execute(&mut *connection)
    .await?;

    query(
        "CREATE INDEX IF NOT EXISTS votes_link_to_msg_ref_id_index on votes (link_to_msg_ref_id)",
    )
    .execute(&mut *connection)
    .await?;

    Ok(())
}
