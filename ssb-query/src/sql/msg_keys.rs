use log::trace;
use sqlx::{query, sqlite::SqliteRow, Error, Row, SqliteConnection};
use ssb_core::MsgKey;

pub async fn find_or_create_msg_key(
    connection: &mut SqliteConnection,
    key: &MsgKey,
) -> Result<i64, Error> {
    let result: Option<i64> = query("SELECT id FROM msg_keys WHERE key=?1")
        .bind(Into::<String>::into(key))
        .map(|row: SqliteRow| row.get(0))
        .fetch_optional(&mut *connection)
        .await?;

    if let Some(found_key) = result {
        Ok(found_key)
    } else {
        let created_key = query("INSERT INTO msg_keys (key) VALUES (?)")
            .bind(Into::<String>::into(key))
            .execute(&mut *connection)
            .await?;

        Ok(created_key.last_insert_rowid())
    }
}

pub async fn create_msg_keys_tables(connection: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating msg_keys tables");

    query(
        "CREATE TABLE IF NOT EXISTS msg_keys (
          id INTEGER PRIMARY KEY,
          key TEXT UNIQUE
        )",
    )
    .execute(connection)
    .await?;

    Ok(())
}

pub async fn create_msg_keys_indices(_connection: &mut SqliteConnection) -> Result<(), Error> {
    Ok(())
}
