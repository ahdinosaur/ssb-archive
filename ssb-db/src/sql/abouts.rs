use log::trace;
use serde_json::{Map, Value};
use sqlx::{query, Error, SqliteConnection};
use ssb_msg::{AboutContent, Msg};
use ssb_ref::LinkRef;

use crate::sql::*;

pub async fn create_abouts_tables(connection: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating abouts tables");

    query(
        "
        CREATE TABLE IF NOT EXISTS about_feeds (
            id INTEGER PRIMARY KEY,
            feed_seq INTEGER NOT NULL,
            link_from_feed_ref_id INTEGER NOT NULL,
            link_to_feed_ref_id INTEGER NOT NULL,
            content JSON NOT NULL,
            FOREIGN KEY (link_from_feed_ref_id)
                REFERENCES feed_refs (id)
                ON UPDATE RESTRICT
                ON DELETE RESTRICT,
            FOREIGN KEY (link_to_feed_ref_id)
                REFERENCES feed_refs (id)
                ON UPDATE RESTRICT
                ON DELETE RESTRICT
        )
        ",
    )
    .execute(&mut *connection)
    .await?;

    query(
        "
        CREATE TABLE IF NOT EXISTS about_msgs (
            id INTEGER PRIMARY KEY,
            feed_seq INTEGER NOT NULL,
            link_from_feed_ref_id INTEGER NOT NULL,
            link_to_msg_ref_id INTEGER NOT NULL,
            content JSON NOT NULL,
            FOREIGN KEY (link_from_feed_ref_id)
                REFERENCES feed_refs (id)
                ON UPDATE RESTRICT
                ON DELETE RESTRICT,
            FOREIGN KEY (link_to_msg_ref_id)
                REFERENCES msg_refs (id)
                ON UPDATE RESTRICT
                ON DELETE RESTRICT
        )
        ",
    )
    .execute(&mut *connection)
    .await?;

    Ok(())
}

pub async fn insert_abouts(
    connection: &mut SqliteConnection,
    msg: &Msg<Value>,
    content: &AboutContent,
) -> Result<(), Error> {
    let link_from_feed_ref_id =
        find_or_create_feed_ref(&mut *connection, &msg.value.author).await?;

    let mut json_content = msg.value.content.as_object().unwrap().clone();
    json_content.remove("type");
    json_content.remove("about");

    match &content.about {
        LinkRef::Feed(feed_ref) => {
            let link_to_feed_ref_id = find_or_create_feed_ref(&mut *connection, feed_ref).await?;

            let row: Option<(i64, i64, Value)> =
                query("SELECT id, feed_seq, content FROM about_feeds WHERE link_from_feed_ref_id = ? AND link_to_feed_ref_id = ?")
                    .bind(&link_from_feed_ref_id)
                    .bind(&link_to_feed_ref_id)
                    .map(|row: sqlx::sqlite::SqliteRow| (row.get(0), row.get(1), row.get(2)))
                    .fetch_optional(&mut *connection)
                    .await?;

            if let Some((id, feed_seq, mut db_content)) = row {
                if feed_seq < msg.value.sequence as i64 {
                    let db_object = db_content.as_object_mut().unwrap();
                    for (key, value) in json_content.iter() {
                        db_object.insert(key.clone(), value.clone());
                    }
                    query("UPDATE about_feeds SET content = ? WHERE id = ?")
                        .bind(db_content)
                        .bind(id)
                        .execute(connection)
                        .await?;
                }
            } else {
                query(
                    "
                    INSERT INTO about_feeds (
                        feed_seq,
                        link_from_feed_ref_id,
                        link_to_feed_ref_id,
                        content
                    ) VALUES (?, ?, ?, ?)
                    ",
                )
                .bind(msg.value.sequence as i64)
                .bind(&link_from_feed_ref_id)
                .bind(&link_to_feed_ref_id)
                .bind(Value::Object(json_content))
                .execute(connection)
                .await?;
            }
        }
        LinkRef::Msg(msg_ref) => {
            let link_to_msg_ref_id = find_or_create_msg_ref(connection, msg_ref).await?;

            let row: Option<(i64, i64, Value)> = query(
                "
                SELECT id, feed_seq, content
                FROM about_msgs
                WHERE link_from_feed_ref_id = ?
                AND link_to_msg_ref_id = ?
                ",
            )
            .bind(&link_from_feed_ref_id)
            .bind(&link_to_msg_ref_id)
            .map(|row: sqlx::sqlite::SqliteRow| (row.get(0), row.get(1), row.get(2)))
            .fetch_optional(&mut *connection)
            .await?;

            if let Some((id, feed_seq, mut db_content)) = row {
                if feed_seq < msg.value.sequence as i64 {
                    let db_object = db_content.as_object_mut().unwrap();
                    for (key, value) in json_content.iter() {
                        db_object.insert(key.clone(), value.clone());
                    }
                    query("UPDATE about_feeds SET content = ? WHERE id = ?")
                        .bind(db_content)
                        .bind(id)
                        .execute(connection)
                        .await?;
                }
            } else {
                query(
                    "
                    INSERT INTO about_msgs (
                        feed_seq,
                        link_from_feed_ref_id,
                        link_to_msg_ref_id,
                        content
                    ) VALUES (?, ?, ?, ?)
                    ",
                )
                .bind(msg.value.sequence as i64)
                .bind(&link_from_feed_ref_id)
                .bind(&link_to_msg_ref_id)
                .bind(Value::Object(json_content))
                .execute(connection)
                .await?;
            }
        }
        _ => {}
    };

    Ok(())
}

pub async fn create_abouts_indices(connection: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating abouts index");

    // about feeds

    query(
        "CREATE INDEX IF NOT EXISTS about_feeds_from_feed_ref_id_index on about_feeds (link_from_feed_ref_id)",
    )
    .execute(&mut *connection)
    .await?;

    query("CREATE INDEX IF NOT EXISTS about_feeds_to_feed_ref_id_index on about_feeds (link_to_feed_ref_id)")
        .execute(&mut *connection)
        .await?;

    // about msgs

    query(
        "CREATE INDEX IF NOT EXISTS about_msgs_from_feed_ref_id_index on about_msgs (link_from_feed_ref_id)",
    )
    .execute(&mut *connection)
    .await?;

    query(
        "CREATE INDEX IF NOT EXISTS about_msgs_to_msg_ref_id_index on about_msgs (link_to_msg_ref_id )",
    )
    .execute(&mut *connection)
    .await?;

    Ok(())
}
