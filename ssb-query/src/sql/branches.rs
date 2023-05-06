use log::trace;
use serde_json::Value;
use sqlx::{query, Error, SqliteConnection};

use crate::sql::*;

pub async fn insert_branches(
    connection: &mut SqliteConnection,
    message: &Msg,
    message_key_id: i64,
) -> Result<(), Error> {
    if let Some(branches_value) = message.value.content.get("branch") {
        let branches = match branches_value {
            Value::Array(arr) => arr
                .iter()
                .map(|value| value.as_str().unwrap().to_string())
                .collect(),
            Value::String(branch) => vec![branch.as_str().to_string()],
            _ => Vec::new(),
        };

        for branch in branches.iter() {
            let link_to_key_id = find_or_create_key(&mut *connection, branch).await?;
            query("INSERT INTO branches_raw (link_from_key_id, link_to_key_id) VALUES (?, ?)")
                .bind(&message_key_id)
                .bind(&link_to_key_id)
                .execute(&mut *connection)
                .await?;
        }
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
