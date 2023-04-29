use log::trace;
use rusqlite::{Connection, Error, NO_PARAMS};

use crate::sql::*;

pub fn create_votes_tables(connection: &Connection) -> Result<usize, Error> {
    trace!("Creating votes tables");

    connection.execute(
        "CREATE TABLE IF NOT EXISTS votes_raw (
          id INTEGER PRIMARY KEY,
          link_from_author_id INTEGER,
          link_to_key_id INTEGER
        )",
        NO_PARAMS,
    )
}

pub fn insert_or_update_votes(connection: &Connection, message: &SsbMessage) {
    if let Value::Number(value) = &message.value.content["vote"]["value"] {
        if let Value::String(link) = &message.value.content["vote"]["link"] {
            let author_id = find_or_create_author(&connection, &message.value.author).unwrap();
            let link_to_key_id = find_or_create_key(connection, link).unwrap();

            if value.as_i64().unwrap() == 1 {
                connection
                    .prepare_cached(
                        "INSERT INTO votes_raw (link_from_author_id, link_to_key_id) VALUES (?, ?)",
                    )
                    .unwrap()
                    .execute(&[&author_id, &link_to_key_id])
                    .unwrap();
            } else {
                connection
                    .prepare_cached("DELETE FROM votes_raw WHERE link_from_author_id = ? AND link_to_key_id = ?")
                    .unwrap()
                    .execute(&[&author_id, &link_to_key_id])
                    .unwrap();
            }
        }
    }
}

pub fn create_votes_indices(connection: &Connection) -> Result<usize, Error> {
    trace!("Creating votes indices");
    connection.execute(
        "CREATE INDEX IF NOT EXISTS votes_raw_link_from_author_id_index on votes_raw (link_from_author_id)",
        NO_PARAMS,
    )?;
    connection.execute(
        "CREATE INDEX IF NOT EXISTS votes_raw_link_to_key_id_index on votes_raw (link_to_key_id)",
        NO_PARAMS,
    )
}
