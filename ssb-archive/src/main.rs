// use std::{thread::sleep, time::Duration};

use ssb_db::{Database, Error as DatabaseError, SelectAllMsgsByFeedOptions};
use ssb_markdown::render;
use ssb_ref::{FeedRef, RefError};
use thiserror::Error as ThisError;

#[tokio::main]
async fn main() {
    match exec().await {
        Ok(()) => {}
        Err(err) => {
            eprintln!("{}", err);
        }
    }
}

#[derive(Debug, ThisError)]
enum Error {
    #[error("Database error: {0}")]
    Database(#[from] DatabaseError),
    #[error("Ref format error: {0}")]
    RefFormat(#[from] RefError),
}

async fn exec() -> Result<(), Error> {
    let mut db = Database::new(
        "/home/dinosaur/.ssb/flume/log.offset".into(),
        "/home/dinosaur/repos/ahdinosaur/ssb-archive/output.sqlite3".into(),
        Vec::new(),
    )
    .await?;

    loop {
        let log_latest = db.get_log_latest().await;
        let sql_latest = db.get_sql_latest().await?;
        if let (Some(log_offset), Some(sql_offset)) = (log_latest, sql_latest) {
            if log_offset == sql_offset {
                break;
            }
        }
        println!("log latest: {:?}", log_latest);
        println!("sql latest: {:?}", sql_latest);
        db.process(20_000).await?;
        // sleep(Duration::from_secs(1))
    }

    let feed_ref: FeedRef = "@6ilZq3kN0F+dXFHAPjAwMm87JEb/VdB+LC9eIMW3sa0=.ed25519"
        .to_owned()
        .try_into()?;
    let max_feed_seq = db.get_max_seq_by_feed(&feed_ref).await.unwrap();

    let messages = db
        .get_all_msgs_by_feed(SelectAllMsgsByFeedOptions {
            feed_ref: &feed_ref,
            content_type: "post",
            page_size: 10,
            less_than_feed_seq: max_feed_seq + 1,
            is_decrypted: false,
        })
        .await?;

    for message in messages {
        println!("{:?}", message.value.content["text"]);
        let content = message.value.content;
        let text = content["text"].as_str().unwrap();
        let html = render(text);
        println!("{:?}", html);
    }

    // select all posts by a user
    //   - greater than seq
    //   - limit 10
    /*
    SELECT
      seq,
      keys.key as key,
      authors.author as author,
      asserted_time,
      content_type,
      content,
      is_decrypted,
      root_keys.key as root,
      fork_keys.key as fork
    FROM messages_raw
    JOIN keys ON keys.id=messages_raw.key_id
    LEFT JOIN keys AS root_keys ON root_keys.id=messages_raw.root_id
    LEFT JOIN keys AS fork_keys ON fork_keys.id=messages_raw.fork_id
    JOIN authors ON authors.id=messages_raw.author_id
    WHERE
            authors.author = '@6ilZq3kN0F+dXFHAPjAwMm87JEb/VdB+LC9eIMW3sa0=.ed25519'
            AND content_type = 'post'
            AND seq > 10
    LIMIT 10
    */

    // select all reply posts in a thread
    /*
    SELECT
      keys.key as key,
      authors.author as author,
      asserted_time,
      content_type,
      content,
      is_decrypted,
      root_keys.key as root,
      fork_keys.key as fork
    FROM messages_raw
    JOIN keys ON keys.id=messages_raw.key_id
    LEFT JOIN keys AS root_keys ON root_keys.id=messages_raw.root_id
    LEFT JOIN keys AS fork_keys ON fork_keys.id=messages_raw.fork_id
    JOIN authors ON authors.id=messages_raw.author_id
    WHERE
      root = '%R/m9I+QW+AwEq7sObyEuAc1kCDGbk1neK0STJKSnpyY=.sha256'
    */

    // get self-described about
    /*
    SELECT
    (
        SELECT
            JSON_EXTRACT(messages_raw.content, "$.name") as name
        FROM abouts_raw
        JOIN keys AS keys_from ON keys_from.id = abouts_raw.link_from_key_id
        JOIN messages_raw ON link_from_key_id = messages_raw.key_id
        JOIN authors AS authors_from ON authors_from.id = messages_raw.author_id
        LEFT JOIN authors AS authors_to ON authors_to.id=abouts_raw.link_to_author_id
        WHERE
          JSON_EXTRACT(messages_raw.content, '$.name') IS NOT NULL
          AND authors_from.author = '@6ilZq3kN0F+dXFHAPjAwMm87JEb/VdB+LC9eIMW3sa0=.ed25519'
          AND authors_to.author = '@6ilZq3kN0F+dXFHAPjAwMm87JEb/VdB+LC9eIMW3sa0=.ed25519'
        ORDER BY abouts_raw.id
        DESC
        LIMIT 1
    ) as name,
    (
        SELECT
            JSON_EXTRACT(messages_raw.content, "$.image") as image
        FROM abouts_raw
        JOIN keys AS keys_from ON keys_from.id = abouts_raw.link_from_key_id
        JOIN messages_raw ON link_from_key_id = messages_raw.key_id
        JOIN authors AS authors_from ON authors_from.id = messages_raw.author_id
        LEFT JOIN authors AS authors_to ON authors_to.id=abouts_raw.link_to_author_id
        WHERE
          JSON_EXTRACT(messages_raw.content, '$.image') IS NOT NULL
          AND authors_from.author = '@6ilZq3kN0F+dXFHAPjAwMm87JEb/VdB+LC9eIMW3sa0=.ed25519'
          AND authors_to.author = '@6ilZq3kN0F+dXFHAPjAwMm87JEb/VdB+LC9eIMW3sa0=.ed25519'
        ORDER BY abouts_raw.id
        DESC
        LIMIT 1
    ) as image,
    (
        SELECT
            JSON_EXTRACT(messages_raw.content, "$.description") as description
        FROM abouts_raw
        JOIN keys AS keys_from ON keys_from.id = abouts_raw.link_from_key_id
        JOIN messages_raw ON link_from_key_id = messages_raw.key_id
        JOIN authors AS authors_from ON authors_from.id = messages_raw.author_id
        LEFT JOIN authors AS authors_to ON authors_to.id=abouts_raw.link_to_author_id
        WHERE
          JSON_EXTRACT(messages_raw.content, '$.description') IS NOT NULL
          AND authors_from.author = '@6ilZq3kN0F+dXFHAPjAwMm87JEb/VdB+LC9eIMW3sa0=.ed25519'
          AND authors_to.author = '@6ilZq3kN0F+dXFHAPjAwMm87JEb/VdB+LC9eIMW3sa0=.ed25519'
        ORDER BY abouts_raw.id
        DESC
        LIMIT 1
    ) as description
    */

    println!("Done!");

    Ok(())
}
