use log::trace;
use sqlx::{query, Error, SqliteConnection};
use ssb_ref::MsgRef;

use crate::sql::*;

pub async fn create_msg_links_tables(connection: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating msg_links tables");

    query(
        "CREATE TABLE IF NOT EXISTS msg_links_raw (
          id INTEGER PRIMARY KEY,
          link_from_msg_ref_id INTEGER,
          link_to_msg_ref_id INTEGER
        )",
    )
    .execute(connection)
    .await?;

    Ok(())
}

pub async fn create_msg_links_views(connection: &mut SqliteConnection) -> Result<(), Error> {
    query(
        "
        CREATE VIEW IF NOT EXISTS msg_links AS
        SELECT 
            msg_links_raw.id as id, 
            msg_links_raw.link_from_msg_ref_id as link_from_msg_ref_id, 
            msg_links_raw.link_to_msg_ref_id as link_to_msg_ref_id, 
            msg_refs_from.msg_ref as link_from_msg_ref, 
            msg_refs_to.msg_ref as link_to_msg_ref
        FROM msg_links_raw 
        JOIN msg_refs AS msg_refs_from ON msg_refs_from.id = msg_links_raw.link_from_msg_ref_id
        JOIN msg_refs AS msg_refs_to ON msg_refs_to.id = msg_links_raw.link_to_msg_ref_id
        ",
    )
    .execute(connection)
    .await?;

    Ok(())
}

pub async fn insert_links(
    connection: &mut SqliteConnection,
    links: &[&MsgRef],
    msg_ref_id: i64,
) -> Result<(), Error> {
    for link in links {
        let link_id = find_or_create_msg_ref(&mut *connection, link).await?;
        query("INSERT INTO msg_links_raw (link_from_msg_ref_id, link_to_msg_ref_id) VALUES (?, ?)")
            .bind(msg_ref_id)
            .bind(link_id)
            .execute(&mut *connection)
            .await?;
    }

    Ok(())
}

pub async fn create_msg_links_indices(connection: &mut SqliteConnection) -> Result<(), Error> {
    create_msg_links_to_index(&mut *connection).await?;
    create_msg_links_from_index(&mut *connection).await?;

    Ok(())
}

async fn create_msg_links_to_index(conn: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating links index");
    query(
        "CREATE INDEX IF NOT EXISTS links_to_id_index on msg_links_raw (link_to_msg_ref_id, link_from_msg_ref_id)",
    )
    .execute(conn)
    .await?;

    Ok(())
}

async fn create_msg_links_from_index(conn: &mut SqliteConnection) -> Result<(), Error> {
    query(
        "CREATE INDEX IF NOT EXISTS links_from_id_index on msg_links_raw (link_from_msg_ref_id, link_to_msg_ref_id)",
    )
    .execute(conn)
    .await?;

    Ok(())
}
