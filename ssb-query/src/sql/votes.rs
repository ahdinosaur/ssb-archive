use log::trace;
use serde_json::Value;
use sqlx::{query, Error, SqliteConnection};

use crate::sql::*;

pub async fn create_votes_tables(connection: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating votes tables");

    query(
        "CREATE TABLE IF NOT EXISTS votes_raw (
          id INTEGER PRIMARY KEY,
          link_from_author_id INTEGER,
          link_to_key_id INTEGER
        )",
    )
    .execute(connection)
    .await?;

    Ok(())
}

pub async fn insert_or_update_votes(
    connection: &mut SqliteConnection,
    message: &Msg,
) -> Result<(), Error> {
    if let Value::Number(value) = &message.value.content["vote"]["value"] {
        if let Value::String(link) = &message.value.content["vote"]["link"] {
            let author_id = find_or_create_author(connection, &message.value.author).await?;
            let link_to_key_id = find_or_create_key(connection, link).await?;

            if value.as_i64().unwrap() == 1 {
                query("INSERT INTO votes_raw (link_from_author_id, link_to_key_id) VALUES (?, ?)")
                    .bind(&author_id)
                    .bind(&link_to_key_id)
                    .execute(connection)
                    .await?;
            } else {
                query("DELETE FROM votes_raw WHERE link_from_author_id = ? AND link_to_key_id = ?")
                    .bind(&author_id)
                    .bind(&link_to_key_id)
                    .execute(connection)
                    .await?;
            }
        }
    }

    Ok(())
}

pub async fn create_votes_indices(connection: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating votes indices");
    query(
        "CREATE INDEX IF NOT EXISTS votes_raw_link_from_author_id_index on votes_raw (link_from_author_id)",
    )
    .execute(&mut *connection)
    .await?;

    query(
        "CREATE INDEX IF NOT EXISTS votes_raw_link_to_key_id_index on votes_raw (link_to_key_id)",
    )
    .execute(&mut *connection)
    .await?;

    Ok(())
}
