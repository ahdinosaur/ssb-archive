use log::trace;
use serde_json::Value;
use sqlx::{query, Error, SqliteConnection};
use ssb_msg::{ContactContent, Msg};

use crate::sql::*;

pub async fn create_contacts_tables(connection: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating contacts tables");

    query(
        "
        CREATE TABLE IF NOT EXISTS contacts(
            id INTEGER PRIMARY KEY,
            feed_ref_id INTEGER NOT NULL,
            contact_feed_ref_id INTEGER NOT NULL,
            is_decrypted BOOLEAN NOT NULL,
            state INTEGER NOT NULL,
            FOREIGN KEY (feed_ref_id)
                REFERENCES feed_refs (id)
                ON UPDATE RESTRICT
                ON DELETE RESTRICT,
            FOREIGN KEY (contact_feed_ref_id)
                REFERENCES feed_refs (id)
                ON UPDATE RESTRICT
                ON DELETE RESTRICT
        )
        ",
    )
    .execute(connection)
    .await?;

    Ok(())
}

pub async fn insert_or_update_contacts(
    connection: &mut SqliteConnection,
    msg: &Msg<Value>,
    content: &ContactContent,
    _msg_ref_id: i64,
    is_decrypted: bool,
) -> Result<(), Error> {
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

    let feed_ref_id = find_or_create_feed_ref(connection, &msg.value.author).await?;
    let contact_feed_ref_id = find_or_create_feed_ref(connection, &content.contact).await?;

    let row: Option<i64> = query(
        "SELECT id FROM contacts WHERE feed_ref_id = ? AND contact_feed_ref_id = ? AND is_decrypted = ?",
    )
        .bind(&feed_ref_id)
        .bind(&contact_feed_ref_id)
        .bind(is_decrypted)
        .map(|row: sqlx::sqlite::SqliteRow| row.get(0))
        .fetch_optional(&mut *connection)
        .await?;

    if let Some(id) = row {
        query("UPDATE contacts SET state = ? WHERE id = ?")
            .bind(state)
            .bind(id)
            .execute(connection)
            .await?;
    } else {
        query("INSERT INTO contacts (feed_ref_id, contact_feed_ref_id, is_decrypted, state) VALUES (?, ?, ?, ?)")
            .bind(feed_ref_id)
            .bind(contact_feed_ref_id)
            .bind(is_decrypted)
            .bind(state)
            .execute(connection)
            .await?;
    }

    Ok(())
}

pub async fn create_contacts_indices(connection: &mut SqliteConnection) -> Result<(), Error> {
    create_contacts_feed_ref_id_state_index(connection).await
}

async fn create_contacts_feed_ref_id_state_index(conn: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating contacts feed_ref_id index");

    query(
        "CREATE INDEX IF NOT EXISTS contacts_contact_feed_ref_id_state_index on contacts (contact_feed_ref_id)",
    )
    .execute(&mut *conn)
    .await?;

    query(
        "CREATE INDEX IF NOT EXISTS contacts_feed_ref_id_state_index on contacts (feed_ref_id, state)",
    )
    .execute(&mut *conn)
    .await?;

    Ok(())
}
