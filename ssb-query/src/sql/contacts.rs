use log::trace;
use serde_json::Value;
use sqlx::{query, Error, SqliteConnection};
use ssb_core::{ContactContent, Msg};

use crate::sql::*;

pub async fn create_contacts_tables(connection: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating contacts tables");

    query(
        "CREATE TABLE IF NOT EXISTS contacts_raw(
            id INTEGER PRIMARY KEY,
            author_id INTEGER,
            contact_author_id INTEGER,
            is_decrypted BOOLEAN,
            state INTEGER
        )",
    )
    .execute(connection)
    .await?;

    Ok(())
}

pub async fn insert_or_update_contacts(
    connection: &mut SqliteConnection,
    msg: &Msg<Value>,
    content: &ContactContent,
    _message_key_id: i64,
    is_decrypted: bool,
) -> Result<(), Error> {
    match &content.contact {
        None => {}
        Some(contact) => {
            //Ok what should this do:
            //  - if the record already exists
            //      - delete it if the new state is zero (this should only happen when record already
            //      exists because you can't unfollow someone you already don't follow.
            //      - update it if the new state is 1 or -1
            //  - else create the new record. State should be a 1 or a -1

            let is_blocking = content.blocking.unwrap_or(false);
            let is_following = content.following.unwrap_or(false);
            let state = if is_blocking {
                -1
            } else if is_following {
                1
            } else {
                0
            };

            let author_id = find_or_create_author(connection, &msg.value.author).await?;
            let contact_author_id = find_or_create_author(connection, contact).await?;

            let row: Option<i64> = query(
            "SELECT id FROM contacts_raw WHERE author_id = ? AND contact_author_id = ? AND is_decrypted = ?",
        )
        .bind(&author_id)
        .bind(&contact_author_id)
        .bind(is_decrypted)
        .map(|row: sqlx::sqlite::SqliteRow| row.get(0))
        .fetch_optional(&mut *connection)
        .await?;

            if let Some(id) = row {
                query("UPDATE contacts_raw SET state = ? WHERE id = ?")
                    .bind(state)
                    .bind(id)
                    .execute(connection)
                    .await?;
            } else {
                query(
                "INSERT INTO contacts_raw (author_id, contact_author_id, is_decrypted, state) VALUES (?, ?, ?, ?)",
            )
            .bind(author_id)
            .bind(contact_author_id)
            .bind(is_decrypted)
            .bind(state)
            .execute(connection)
            .await?;
            }
        }
    };

    Ok(())
}

pub async fn create_contacts_indices(connection: &mut SqliteConnection) -> Result<(), Error> {
    create_contacts_author_id_state_index(connection).await
}

async fn create_contacts_author_id_state_index(conn: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating contacts author_id index");

    query(
        "CREATE INDEX IF NOT EXISTS contacts_raw_contact_author_id_state_index on contacts_raw (contact_author_id)",
    )
    .execute(&mut *conn)
    .await?;

    query(
        "CREATE INDEX IF NOT EXISTS contacts_raw_author_id_state_index on contacts_raw (author_id, state)",
    )
    .execute(&mut *conn)
    .await?;

    Ok(())
}
