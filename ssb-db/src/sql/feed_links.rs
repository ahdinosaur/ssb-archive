use log::trace;
use sqlx::{query, Error, SqliteConnection};
use ssb_ref::FeedRef;

use crate::sql::*;

pub async fn create_feed_links_tables(connection: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating feed_links tables");

    query(
        "
        CREATE TABLE IF NOT EXISTS feed_links (
            id INTEGER PRIMARY KEY,
            link_from_msg_ref_id INTEGER NOT NULL,
            link_to_feed_ref_id INTEGER NOT NULL,
            FOREIGN KEY (link_from_msg_ref_id)
                REFERENCES msg_refs (id)
                ON UPDATE RESTRICT
                ON DELETE RESTRICT,
            FOREIGN KEY (link_to_feed_ref_id)
                REFERENCES feed_refs (id)
                ON UPDATE RESTRICT
                ON DELETE RESTRICT
        )
        ",
    )
    .execute(connection)
    .await?;

    Ok(())
}

pub async fn insert_feed_links(
    connection: &mut SqliteConnection,
    links: &[&FeedRef],
    msg_ref_id: i64,
) -> Result<(), Error> {
    for link in links {
        let link_id = find_or_create_feed_ref(&mut *connection, link).await?;
        query("INSERT INTO feed_links (link_from_msg_ref_id, link_to_feed_ref_id) VALUES (?, ?)")
            .bind(msg_ref_id)
            .bind(link_id)
            .execute(&mut *connection)
            .await?;
    }

    Ok(())
}

pub async fn create_feed_links_indices(connection: &mut SqliteConnection) -> Result<(), Error> {
    create_feed_links_to_index(connection).await
}

async fn create_feed_links_to_index(conn: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating feed_links index");
    query(
        "CREATE INDEX IF NOT EXISTS feed_links_to_from_index on feed_links (link_to_feed_ref_id, link_from_msg_ref_id)",
    )
    .execute(&mut *conn)
    .await?;

    query(
        "CREATE INDEX IF NOT EXISTS feed_links_from_to_index on feed_links (link_from_msg_ref_id, link_to_feed_ref_id)",
    )
    .execute(&mut *conn)
    .await?;

    Ok(())
}
