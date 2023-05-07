// use std::{thread::sleep, time::Duration};

use ssb_markdown::render;
use ssb_query::{sql::SqlViewError, SelectAllMsgsByFeedOptions, SsbQuery};
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
    #[error("Query error: {0}")]
    Query(#[from] SqlViewError),
    #[error("Ref format error: {0}")]
    RefFormat(#[from] RefError),
}

async fn exec() -> Result<(), Error> {
    let mut view = SsbQuery::new(
        "/home/dinosaur/.ssb/flume/log.offset".into(),
        "/home/dinosaur/repos/ahdinosaur/ssb-archive/output.sqlite3".into(),
        Vec::new(),
    )
    .await?;

    loop {
        let log_latest = view.get_log_latest().await;
        let view_latest = view.get_view_latest().await;
        match (log_latest, view_latest) {
            (Some(log_offset), Some(view_offset)) => {
                // HACK: handle cases where we aren't
                //   able to process the last few messages.
                //   (presumably because encrypted)
                if log_offset < view_offset + 10_000 {
                    break;
                }
                // otherwise would be:
                // if log_offset == view_offset {
                //     break;
                // }
            }
            _ => {}
        }
        println!("log latest: {:?}", view.get_log_latest().await);
        println!("view latest: {:?}", view.get_view_latest().await);
        view.process(20_000).await?;
        // sleep(Duration::from_secs(1))
    }

    let feed_ref: FeedRef = "@6ilZq3kN0F+dXFHAPjAwMm87JEb/VdB+LC9eIMW3sa0=.ed25519"
        .to_owned()
        .try_into()?;
    let max_seq = view.select_max_seq_by_feed(&feed_ref).await.unwrap();

    let messages = view
        .select_all_msgs_by_feed(SelectAllMsgsByFeedOptions {
            feed_ref: &feed_ref,
            content_type: "post",
            page_size: 10,
            less_than_seq: max_seq + 1,
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
