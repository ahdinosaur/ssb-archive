use log::trace;
use sqlx::{query, Error, SqliteConnection};

use crate::sql::*;

pub async fn create_abouts_tables(connection: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating abouts tables");

    query(
        "CREATE TABLE IF NOT EXISTS abouts_raw (
          id INTEGER PRIMARY KEY,
          link_from_key_id INTEGER,
          link_to_author_id INTEGER,
          link_to_key_id INTEGER
        )",
    )
    .execute(connection)
    .await?;

    Ok(())
}

pub async fn insert_abouts(
    connection: &mut SqliteConnection,
    message: &Msg,
    message_key_id: i64,
) -> Result<(), Error> {
    if let Value::String(about_key) = &message.value.content["about"] {
        let key;

        let (link_to_author_id, link_to_key_id) = match about_key.get(0..1) {
            Some("@") => {
                key = find_or_create_author(connection, about_key).await?;
                (Some(key), None)
            }
            Some("%") => {
                key = find_or_create_key(connection, about_key).await?;
                (None, Some(key))
            }
            _ => (None, None),
        };

        query("INSERT INTO abouts_raw (link_from_key_id, link_to_author_id, link_to_key_id) VALUES (?, ?, ?)")
            .bind(&message_key_id)
            .bind(&link_to_author_id)
            .bind(&link_to_key_id)
            .execute(connection)
            .await?;
    }

    Ok(())
}

pub async fn create_abouts_indices(connection: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating abouts index");
    query("CREATE INDEX IF NOT EXISTS abouts_raw_from_index on abouts_raw (link_from_key_id)")
        .execute(&mut *connection)
        .await?;
    query("CREATE INDEX IF NOT EXISTS abouts_raw_key_index on abouts_raw (link_to_key_id)")
        .execute(&mut *connection)
        .await?;
    query("CREATE INDEX IF NOT EXISTS abouts_raw_author_index on abouts_raw (link_to_author_id )")
        .execute(&mut *connection)
        .await?;
    Ok(())
}

pub async fn create_abouts_views(connection: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating abouts views");
    //resolve all the links, get the content of the message.
    query(
        "
        CREATE VIEW IF NOT EXISTS abouts AS
        SELECT 
            abouts_raw.id as id, 
            abouts_raw.link_from_key_id as link_from_key_id, 
            abouts_raw.link_to_key_id as link_to_key_id, 
            abouts_raw.link_to_author_id as link_to_author_id, 
            keys_from.key as link_from_key, 
            keys_to.key as link_to_key, 
            authors_to.author as link_to_author,
            messages.content as content,
            messages.author as link_from_author
        FROM abouts_raw 
        JOIN keys AS keys_from ON keys_from.id=abouts_raw.link_from_key_id
        JOIN messages ON link_from_key_id=messages.key_id
        LEFT JOIN keys AS keys_to ON keys_to.id=abouts_raw.link_to_key_id
        LEFT JOIN authors AS authors_to ON authors_to.id=abouts_raw.link_to_author_id
        ",
    )
    .execute(connection)
    .await?;

    Ok(())
}
