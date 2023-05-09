use flumedb::flume_view::Sequence;
use log::trace;
use serde_derive::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{
    query,
    sqlite::{SqliteConnectOptions, SqliteConnection, SqliteJournalMode, SqliteRow},
    ConnectOptions, Error as SqlError, Row,
};
use ssb_msg::{BlobLink, Link, Msg, MsgContent};
use std::str::FromStr;

mod abouts;
mod blob_links;
mod blob_refs;
mod branches;
mod contacts;
mod feed_links;
mod feed_refs;
mod migrations;
mod msg_links;
mod msg_refs;
mod msgs;
mod posts;
mod queries;
mod votes;
use self::abouts::*;
use self::blob_links::*;
use self::blob_refs::*;
use self::branches::*;
use self::contacts::*;
use self::feed_links::*;
use self::feed_refs::*;
pub(crate) use self::migrations::is_db_up_to_date;
use self::migrations::*;
use self::msg_links::*;
pub(crate) use self::msg_refs::find_or_create_msg_ref;
use self::msg_refs::*;
use self::msgs::*;
pub(crate) use self::msgs::{get_msg_log_seq, insert_msg};
use self::posts::*;
pub use self::queries::SelectAllMsgsByFeedOptions;
pub(crate) use self::queries::*;
use self::votes::*;

pub async fn create_connection(path: &str) -> Result<SqliteConnection, SqlError> {
    SqliteConnectOptions::from_str(path)?
        .journal_mode(SqliteJournalMode::Wal)
        .create_if_missing(true)
        .connect()
        .await
}

pub async fn setup_new_db(connection: &mut SqliteConnection) -> Result<(), SqlError> {
    create_tables(connection).await?;
    create_indices(connection).await?;

    set_db_version(connection).await?;

    Ok(())
}

pub async fn setup_db(connection: &mut SqliteConnection) -> Result<(), SqlError> {
    set_pragmas(connection).await?;

    Ok(())
}

pub async fn check_db_integrity(connection: &mut SqliteConnection) -> Result<bool, SqlError> {
    let res: String = query("PRAGMA integrity_check")
        .map(|row: SqliteRow| -> String { row.get(0) })
        .fetch_one(connection)
        .await?;

    if res == "ok" {
        Ok(true)
    } else {
        Ok(false)
    }
}

pub async fn insert_content(
    connection: &mut SqliteConnection,
    msg: &Msg<Value>,
    content: &MsgContent,
    msg_ref_id: i64,
    is_decrypted: bool,
) -> Result<(), SqlError> {
    match content {
        MsgContent::Post(post) => {
            if let Some(links) = &post.mentions {
                let mut msg_refs = Vec::new();
                let mut feed_refs = Vec::new();
                let mut blob_refs = Vec::new();
                let mut hashtag_keys = Vec::new();
                for link in links.iter() {
                    match link {
                        Link::Msg { link, .. } => msg_refs.push(link),
                        Link::Feed { link, .. } => feed_refs.push(link),
                        Link::Blob(BlobLink { link, .. }) => blob_refs.push(link),
                        Link::Hashtag { link } => hashtag_keys.push(link),
                    }
                }
                insert_links(connection, msg_refs.as_slice(), msg_ref_id).await?;
                insert_feed_links(connection, feed_refs.as_slice(), msg_ref_id).await?;
                insert_blob_links(connection, blob_refs.as_slice(), msg_ref_id).await?;
            }

            insert_post(connection, &msg, &post, msg_ref_id).await?;
            if let Some(branch) = &post.branch {
                insert_branches(connection, branch.as_slice(), msg_ref_id).await?;
            }
        }
        MsgContent::Contact(contact) => {
            insert_or_update_contacts(connection, &msg, &contact, msg_ref_id, is_decrypted).await?;
        }
        MsgContent::Vote(vote) => {
            insert_or_update_votes(connection, &msg, &vote).await?;
        }
        MsgContent::About(about) => {
            insert_abouts(connection, &msg, &about).await?;
        }
        MsgContent::Unknown => {
            // println!("Unknown content: {:?}", msg.value.content);
        }
    }

    Ok(())
}

pub async fn get_latest(connection: &mut SqliteConnection) -> Result<Option<Sequence>, SqlError> {
    let res: Option<i64> = query("SELECT MAX(log_seq) FROM msgs")
        .map(|row: SqliteRow| row.get(0))
        .fetch_optional(connection)
        .await?;

    trace!("got latest seq from db: {:?}", res);

    Ok(res.map(|v| v as Sequence))
}

async fn set_pragmas(connection: &mut SqliteConnection) -> Result<(), SqlError> {
    query("PRAGMA synchronous = OFF")
        .execute(&mut *connection)
        .await?;
    query("PRAGMA page_size = 4096")
        .execute(&mut *connection)
        .await?;
    Ok(())
}

async fn create_tables(connection: &mut SqliteConnection) -> Result<(), SqlError> {
    create_migrations_tables(connection).await?;
    create_msgs_tables(connection).await?;
    create_msg_refs_tables(connection).await?;
    create_msg_links_tables(connection).await?;
    create_feed_refs_tables(connection).await?;
    create_feed_links_tables(connection).await?;
    create_blob_refs_tables(connection).await?;
    create_blob_links_tables(connection).await?;
    create_contacts_tables(connection).await?;
    create_branches_tables(connection).await?;
    create_abouts_tables(connection).await?;
    create_votes_tables(connection).await?;
    create_posts_tables(connection).await?;

    Ok(())
}

async fn create_indices(connection: &mut SqliteConnection) -> Result<(), SqlError> {
    create_msgs_indices(connection).await?;
    create_msg_refs_indices(connection).await?;
    create_msg_links_indices(connection).await?;
    create_feed_refs_indices(connection).await?;
    create_feed_links_indices(connection).await?;
    create_blob_links_indices(connection).await?;
    create_contacts_indices(connection).await?;
    create_branches_indices(connection).await?;
    create_abouts_indices(connection).await?;
    create_votes_indices(connection).await?;
    create_posts_indices(connection).await?;
    Ok(())
}

/*
#[cfg(test)]
mod test {
    use super::*;
    use serde_json::*;

    #[test]
    fn find_values_in_object() {
        let obj = json!({ "key": 1, "value": {"link": "hello", "array": [{"link": "piet"}], "deeper": {"link": "world"}}});

        let mut vec = Vec::new();
        find_values_in_object_by_key(&obj, "link", &mut vec);

        assert_eq!(vec.len(), 3);
        assert_eq!(vec[0].as_str().unwrap(), "hello");
        assert_eq!(vec[1].as_str().unwrap(), "piet");
        assert_eq!(vec[2].as_str().unwrap(), "world");
    }

    #[test]
    fn open_connection() {
        let filename = "/tmp/test123456.sqlite3";
        let keys = Vec::new();
        std::fs::remove_file(filename.clone())
            .or::<Result<()>>(Ok(()))
            .unwrap();
        SqlView::new(filename, keys, "").await.unwrap();
        assert!(true)
    }

    #[test]
    fn append() {
        let expected_seq = 1234;
        let filename = "/tmp/test12345.sqlite3";
        let keys = Vec::new();
        std::fs::remove_file(filename.clone())
            .or::<Result<()>>(Ok(()))
            .unwrap();

        let mut view = SqlView::new(filename, keys, "").unwrap();
        let jsn = r#####"{
  "key": "%KKPLj1tWfuVhCvgJz2hG/nIsVzmBRzUJaqHv+sb+n1c=.sha256",
  "value": {
    "previous": "%xsMQA2GrsZew0GSxmDSBaoxDafVaUJ07YVaDGcp65a4=.sha256",
    "feed_ref": "@QlCTpvY7p9ty2yOFrv1WU1AE88aoQc4Y7wYal7PFc+w=.ed25519",
    "sequence": 4797,
    "timestamp": 1543958997985,
    "hash": "sha256",
    "content": {
      "type": "post",
      "root": "%9EdpeKC5CgzpQs/x99CcnbD3n6ugUlwm19F7ZTqMh5w=.sha256",
      "branch": "%sQV8QpyUNvh7fBAs2ts00Qo2gj44CQBmwonWJzm+AeM=.sha256",
      "reply": {
        "%9EdpeKC5CgzpQs/x99CcnbD3n6ugUlwm19F7ZTqMh5w=.sha256": "@+UMKhpbzXAII+2/7ZlsgkJwIsxdfeFi36Z5Rk1gCfY0=.ed25519",
        "%sQV8QpyUNvh7fBAs2ts00Qo2gj44CQBmwonWJzm+AeM=.sha256": "@vzoU7/XuBB5B0xueC9NHFr9Q76VvPktD9GUkYgN9lAc=.ed25519"
      },
      "channel": null,
      "recps": null,
      "text": "If I understand correctly, cjdns overlaying over old IP (which is basically all of the cjdns uses so far) still requires old IP addresses to introduce you to the cjdns network, so the chicken and egg problem is still there.",
      "feed_links": []
    },
    "signature": "mi5j/buYZdsiH8l6CVWRqdBKe+0UG6tVTOoVVjMhYl38Nkmb8wiIEfe7zu0JWuiHkaAIq+0/ZqYr6aV14j4fAw==.sig.ed25519"
  },
  "timestamp": 1543959001933
}
"#####;
        view.append(expected_seq, jsn.as_bytes());
        let seq = view
            .get_seq_by_key("%KKPLj1tWfuVhCvgJz2hG/nIsVzmBRzUJaqHv+sb+n1c=.sha256")
            .unwrap();
        assert_eq!(seq, expected_seq as i64);

        let seqs = view.get_seqs_by_type("post").unwrap();
        assert_eq!(seqs[0], expected_seq as i64);
    }

    #[test]
    fn test_db_integrity_ok() {
        let filename = "/tmp/test_integrity.sqlite3";
        let keys = Vec::new();
        std::fs::remove_file(filename.clone())
            .or::<Result<()>>(Ok(()))
            .unwrap();

        let mut view = SqlView::new(filename, keys, "").unwrap();
        view.check_db_integrity().unwrap();
    }

    #[test]
    fn test_db_integrity_fails() {
        let filename = "/tmp/test_integrity_bad.sqlite3";
        let keys = Vec::new();
        std::fs::remove_file(filename.clone())
            .or::<Result<()>>(Ok(()))
            .unwrap();

        let mut view = SqlView::new(filename.clone(), keys, "").unwrap();

        std::fs::write(filename, b"BANG").unwrap();

        match view.check_db_integrity() {
            Ok(_) => panic!(),
            Err(_) => assert!(true),
        }
    }
}
*/
