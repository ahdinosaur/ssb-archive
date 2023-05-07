use log::trace;
use sqlx::{query, sqlite::SqliteRow, Error, Row, SqliteConnection};
use ssb_core::FeedKey;

pub async fn find_or_create_feed_key(
    connection: &mut SqliteConnection,
    feed_key: &FeedKey,
) -> Result<i64, Error> {
    let result: Option<i64> = query("SELECT id FROM feed_keys WHERE feed_key=?1")
        .bind(Into::<String>::into(feed_key))
        .map(|row: SqliteRow| row.get(0))
        .fetch_optional(&mut *connection)
        .await?;

    if let Some(found_feed_key) = result {
        Ok(found_feed_key)
    } else {
        let created_feed_key = query("INSERT INTO feed_keys (feed_key) VALUES (?)")
            .bind(Into::<String>::into(feed_key))
            .execute(&mut *connection)
            .await?;

        Ok(created_feed_key.last_insert_rowid())
    }
}

pub async fn create_feed_keys_tables(connection: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating feed_keys tables");

    query(
        "CREATE TABLE IF NOT EXISTS feed_keys (
          id INTEGER PRIMARY KEY,
          feed_key TEXT UNIQUE
        )",
    )
    .execute(connection)
    .await?;

    Ok(())
}

pub async fn create_feed_keys_indices(connection: &mut SqliteConnection) -> Result<(), Error> {
    Ok(())
}
