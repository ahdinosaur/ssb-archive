use log::trace;
use sqlx::{query, sqlite::SqliteRow, Error, Row, SqliteConnection};
use ssb_core::FeedId;

pub async fn find_or_create_author(
    connection: &mut SqliteConnection,
    author: &FeedId,
) -> Result<i64, Error> {
    let result: Option<i64> = query("SELECT id FROM authors WHERE author=?1")
        .bind(Into::<String>::into(author))
        .map(|row: SqliteRow| row.get(0))
        .fetch_optional(&mut *connection)
        .await?;

    if let Some(found_author) = result {
        Ok(found_author)
    } else {
        let created_author = query("INSERT INTO authors (author) VALUES (?)")
            .bind(Into::<String>::into(author))
            .execute(&mut *connection)
            .await?;

        Ok(created_author.last_insert_rowid())
    }
}

pub async fn create_authors_tables(connection: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating authors tables");

    query(
        "CREATE TABLE IF NOT EXISTS authors (
          id INTEGER PRIMARY KEY,
          author TEXT UNIQUE
        )",
    )
    .execute(connection)
    .await?;

    Ok(())
}

pub async fn create_authors_indices(connection: &mut SqliteConnection) -> Result<(), Error> {
    Ok(())
}
