use log::trace;
use serde_json::Value;
use sqlx::{query, Error, SqliteConnection};
use ssb_msg::{Msg, PostContent};

use crate::sql::*;

pub async fn insert_post(
    connection: &mut SqliteConnection,
    _msg: &Msg<Value>,
    post: &PostContent,
    msg_ref_id: i64,
) -> Result<(), Error> {
    let root_msg_ref_id = if let Some(root) = post.root.clone() {
        trace!("find or create root key id");
        Some(find_or_create_msg_ref(connection, &root).await?)
    } else {
        None
    };
    let fork_msg_ref_id = if let Some(fork) = post.fork.clone() {
        trace!("find or create fork key id");
        Some(find_or_create_msg_ref(connection, &fork).await?)
    } else {
        None
    };

    trace!("insert post");
    query(
        "INSERT INTO posts (
            msg_ref_id,
            root_msg_ref_id,
            fork_msg_ref_id
        ) VALUES (?, ?, ?)",
    )
    .bind(msg_ref_id)
    .bind(root_msg_ref_id)
    .bind(fork_msg_ref_id)
    .execute(connection)
    .await?;

    Ok(())
}

pub async fn create_posts_tables(connection: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating posts tables");
    query(
        "
        CREATE TABLE IF NOT EXISTS posts (
            msg_ref_id INTEGER UNIQUE, 
            root_msg_ref_id INTEGER,
            fork_msg_ref_id INTEGER,
            FOREIGN KEY (root_msg_ref_id)
                REFERENCES msg_refs (id)
                ON UPDATE RESTRICT
                ON DELETE RESTRICT,
            FOREIGN KEY (fork_msg_ref_id)
                REFERENCES msg_refs (id)
                ON UPDATE RESTRICT
                ON DELETE RESTRICT
        )
        ",
    )
    .execute(connection)
    .await?;

    Ok(())
}

pub async fn create_posts_indices(connection: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating posts indices");

    create_msg_index(connection).await?;
    create_root_index(connection).await?;
    create_fork_index(connection).await?;

    Ok(())
}

async fn create_msg_index(connection: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating msg index");
    query("CREATE INDEX IF NOT EXISTS posts_msg_ref_id_index on posts (msg_ref_id)")
        .execute(connection)
        .await?;

    Ok(())
}

async fn create_root_index(connection: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating root index");
    query("CREATE INDEX IF NOT EXISTS posts_root_msg_ref_id_index on posts (root_msg_ref_id)")
        .execute(connection)
        .await?;

    Ok(())
}

async fn create_fork_index(connection: &mut SqliteConnection) -> Result<(), Error> {
    trace!("Creating fork index");
    query("CREATE INDEX IF NOT EXISTS posts_fork_msg_ref_id_index on posts (fork_msg_ref_id)")
        .execute(connection)
        .await?;

    Ok(())
}

/*

get msgs in thread:

SELECT log_seq
FROM msgs
JOIN msg_refs ON msg_refs.id = msgs.msg_ref_id
JOIN posts ON posts.msg_ref_id = msgs.msg_ref_id
JOIN msg_refs AS root_msg_refs ON root_msg_refs.id = posts.root_msg_ref_id
WHERE
    root_msg_refs.msg_ref = "%CiSwKkjT8cic9mdfwNRP5izrrS5RBgjOBtne7ddlHw0=.sha256"
ORDER BY timestamp_asserted ASC


get forks from thread:

SELECT log_seq, msg_refs.msg_ref
FROM msgs
JOIN msg_refs ON msg_refs.id = msgs.msg_ref_id
JOIN posts ON posts.msg_ref_id = msgs.msg_ref_id
JOIN msg_refs AS fork_msg_refs ON fork_msg_refs.id = posts.fork_msg_ref_id
WHERE
    fork_msg_refs.msg_ref = "%CiSwKkjT8cic9mdfwNRP5izrrS5RBgjOBtne7ddlHw0=.sha256"
ORDER BY timestamp_asserted ASC


get authors in root post or thread replies or thread forks or backlinks:

SELECT
    feed_refs.feed_ref,
    JSON_EXTRACT(content, "$.name") as name,
    JSON_EXTRACT(content, "$.image") as image
FROM about_feeds
JOIN feed_refs ON
    about_feeds.link_to_feed_ref_id = feed_refs.id
    AND about_feeds.link_from_feed_ref_id = feed_refs.id
JOIN msgs ON msgs.feed_ref_id = feed_refs.id
JOIN msg_refs ON msg_refs.id = msgs.msg_ref_id
WHERE
    msg_refs.msg_ref = "%CiSwKkjT8cic9mdfwNRP5izrrS5RBgjOBtne7ddlHw0=.sha256"
UNION
SELECT
    feed_refs.feed_ref,
    JSON_EXTRACT(content, "$.name") as name,
    JSON_EXTRACT(content, "$.image") as image
FROM about_feeds
JOIN feed_refs ON
    about_feeds.link_to_feed_ref_id = feed_refs.id
    AND about_feeds.link_from_feed_ref_id = feed_refs.id
JOIN msgs ON msgs.feed_ref_id = feed_refs.id
JOIN msg_refs ON msg_refs.id = msgs.msg_ref_id
JOIN posts ON posts.msg_ref_id = msgs.msg_ref_id
LEFT JOIN msg_refs AS root_msg_refs ON root_msg_refs.id = posts.root_msg_ref_id
WHERE
    root_msg_refs.msg_ref = "%CiSwKkjT8cic9mdfwNRP5izrrS5RBgjOBtne7ddlHw0=.sha256"
UNION
SELECT
    feed_refs.feed_ref,
    JSON_EXTRACT(content, "$.name") as name,
    JSON_EXTRACT(content, "$.image") as image
FROM about_feeds
JOIN feed_refs ON
    about_feeds.link_to_feed_ref_id = feed_refs.id
    AND about_feeds.link_from_feed_ref_id = feed_refs.id
JOIN msgs ON msgs.feed_ref_id = feed_refs.id
JOIN msg_refs ON msg_refs.id = msgs.msg_ref_id
JOIN posts ON posts.msg_ref_id = msgs.msg_ref_id
LEFT JOIN msg_refs AS fork_msg_refs ON fork_msg_refs.id = posts.fork_msg_ref_id
WHERE
    fork_msg_refs.msg_ref = "%CiSwKkjT8cic9mdfwNRP5izrrS5RBgjOBtne7ddlHw0=.sha256"
UNION
SELECT
    feed_refs.feed_ref,
    JSON_EXTRACT(content, "$.name") as name,
    JSON_EXTRACT(content, "$.image") as image
FROM about_feeds
JOIN feed_refs ON
    about_feeds.link_to_feed_ref_id = feed_refs.id
    AND about_feeds.link_from_feed_ref_id = feed_refs.id
JOIN msgs ON msgs.feed_ref_id = feed_refs.id
JOIN msg_refs ON msg_refs.id = msgs.msg_ref_id
JOIN posts ON posts.msg_ref_id = msgs.msg_ref_id
JOIN msg_links ON msg_refs.id = msg_links.link_from_msg_ref_id
JOIN msg_refs AS link_to_msg_refs ON link_to_msg_refs.id = msg_links.link_to_msg_ref_id
WHERE
    link_to_msg_refs.msg_ref = "%CiSwKkjT8cic9mdfwNRP5izrrS5RBgjOBtne7ddlHw0=.sha256"
*/
