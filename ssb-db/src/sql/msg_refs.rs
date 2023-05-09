use log::trace;
use sqlx::{query, sqlite::SqliteRow, Error, Row, SqliteConnection};
use ssb_ref::MsgRef;

pub async fn find_or_create_msg_ref(
    connection: &mut SqliteConnection,
    msg_ref: &MsgRef,
) -> Result<i64, Error> {
    let result: Option<i64> = query("SELECT id FROM msg_refs WHERE msg_ref=?1")
        .bind(Into::<String>::into(msg_ref))
        .map(|row: SqliteRow| row.get(0))
        .fetch_optional(&mut *connection)
        .await?;

    if let Some(found_msg_ref) = result {
        Ok(found_msg_ref)
    } else {
        let created_msg_ref = query("INSERT INTO msg_refs (msg_ref) VALUES (?)")
            .bind(Into::<String>::into(msg_ref))
            .execute(&mut *connection)
            .await?;

        Ok(created_msg_ref.last_insert_rowid())
    }
}

pub async fn create_msg_refs_tables(connection: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating msg_refs tables");

    query(
        "
        CREATE TABLE IF NOT EXISTS msg_refs (
            id INTEGER PRIMARY KEY,
            msg_ref TEXT UNIQUE NOT NULL
        )
        ",
    )
    .execute(connection)
    .await?;

    Ok(())
}

pub async fn create_msg_refs_indices(_connection: &mut SqliteConnection) -> Result<(), Error> {
    Ok(())
}
