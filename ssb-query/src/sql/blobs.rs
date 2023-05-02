use log::trace;
use sqlx::{query, Error, SqliteConnection};

pub fn find_or_create_blob(connection: &mut SqliteConnection, blob: &str) -> Result<i64, Error> {
    let mut stmt = connection.prepare_cached("SELECT id FROM blobs WHERE blob=?1")?;

    stmt.query_row(&[blob], |row| row.get(0)).or_else(|_| {
        connection
            .prepare_cached("INSERT INTO blobs (blob) VALUES (?)")
            .map(|mut stmt| stmt.execute(&[blob]))
            .map(|_| connection.last_insert_rowid())
    })
}

pub fn create_blobs_tables(connection: &mut SqliteConnection) -> Result<usize, Error> {
    trace!("Creating blobs tables");
    connection.execute(
        "CREATE TABLE IF NOT EXISTS blobs (
          id INTEGER PRIMARY KEY,
          blob TEXT UNIQUE
        )",
        (),
    )
}
