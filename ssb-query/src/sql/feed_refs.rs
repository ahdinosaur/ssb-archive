use log::trace;
use sqlx::{query, sqlite::SqliteRow, Error, Row, SqliteConnection};
use ssb_ref::FeedRef;

pub async fn find_or_create_feed_ref(
    connection: &mut SqliteConnection,
    feed_ref: &FeedRef,
) -> Result<i64, Error> {
    let result: Option<i64> = query("SELECT id FROM feed_refs WHERE feed_ref = ?1")
        .bind(Into::<String>::into(feed_ref))
        .map(|row: SqliteRow| row.get(0))
        .fetch_optional(&mut *connection)
        .await?;

    if let Some(found_feed_ref) = result {
        Ok(found_feed_ref)
    } else {
        let created_feed_ref = query("INSERT INTO feed_refs (feed_ref) VALUES (?)")
            .bind(Into::<String>::into(feed_ref))
            .execute(&mut *connection)
            .await?;

        Ok(created_feed_ref.last_insert_rowid())
    }
}

pub async fn create_feed_refs_tables(connection: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating feed_refs tables");

    query(
        "CREATE TABLE IF NOT EXISTS feed_refs (
          id INTEGER PRIMARY KEY,
          feed_ref TEXT UNIQUE
        )",
    )
    .execute(connection)
    .await?;

    Ok(())
}

pub async fn create_feed_refs_indices(connection: &mut SqliteConnection) -> Result<(), Error> {
    Ok(())
}
