use log::trace;
use sqlx::{query, sqlite::SqliteRow, Error, Row, SqliteConnection};

pub async fn find_or_create_author(
    connection: &mut SqliteConnection,
    author: &str,
) -> Result<i64, Error> {
    let result: Option<i64> = query("SELECT id FROM authors WHERE author=?1")
        .bind(author)
        .map(|row: SqliteRow| row.get(0))
        .fetch_optional(&mut *connection)
        .await?;

    if let Some(found_author) = result {
        Ok(found_author)
    } else {
        let created_author = query("INSERT INTO authors (author) VALUES (?)")
            .bind(author)
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
          author TEXT UNIQUE,
          is_me BOOLEAN 
        )",
    )
    .execute(connection)
    .await?;

    Ok(())
}

pub async fn set_author_that_is_me(
    connection: &mut SqliteConnection,
    my_key: &str,
) -> Result<(), Error> {
    let my_key_id = find_or_create_author(&mut *connection, my_key).await?;

    query("UPDATE authors SET is_me = 1 WHERE id = ?")
        .bind(my_key_id)
        .execute(&mut *connection)
        .await?;

    Ok(())
}

pub async fn create_authors_indices(connection: &mut SqliteConnection) -> Result<(), Error> {
    create_is_me_index(&mut *connection).await?;
    Ok(())
}

async fn create_is_me_index(connection: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating is_me index");

    query("CREATE INDEX IF NOT EXISTS authors_is_me_index ON authors (is_me)")
        .execute(connection)
        .await?;

    Ok(())
}
