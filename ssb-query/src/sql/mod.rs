use base64::engine::{general_purpose::STANDARD as b64, Engine};
use flumedb::flume_view::Sequence;
use log::{info, trace};
use private_box::Keypair;
use serde_derive::{Deserialize, Serialize};
use serde_json::{from_value, Error as JsonError, Value};
use sqlx::{
    query,
    sqlite::{SqliteConnectOptions, SqliteConnection, SqliteJournalMode, SqliteRow},
    ConnectOptions, Connection, Error as SqlError, Row,
};
use ssb_msg::{BlobLink, Link, Msg, MsgContent};
use ssb_ref::RefError;
use std::str::FromStr;
use thiserror::Error as ThisError;

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
use self::migrations::*;
use self::msg_links::*;
use self::msg_refs::*;
pub(crate) use self::msgs::get_msg_log_seq;
use self::msgs::*;
use self::posts::*;
pub use self::queries::SelectAllMsgsByFeedOptions;
pub(crate) use self::queries::*;
use self::votes::*;

#[derive(Debug, ThisError)]
pub enum SqlViewError {
    #[error("Db failed integrity check")]
    DbFailedIntegrityCheck {},
    #[error("Sql error: {0}")]
    Sql(#[from] SqlError),
    #[error("Json error: {0}")]
    Json(#[from] JsonError),
    #[error("Ref format error: {0}")]
    RefFormat(#[from] RefError),
}

pub struct SqlView {
    pub connection: SqliteConnection,
    secret_keys: Vec<Keypair>,
}

async fn create_connection(path: &str) -> Result<SqliteConnection, SqlError> {
    SqliteConnectOptions::from_str(path)?
        .journal_mode(SqliteJournalMode::Wal)
        .create_if_missing(true)
        .connect()
        .await
}

impl SqlView {
    pub async fn new(path: &str, secret_keys: Vec<Keypair>) -> Result<SqlView, SqlViewError> {
        let mut connection = create_connection(path).await?;

        if let Ok(false) = is_db_up_to_date(&mut connection).await {
            info!("sqlite db is out of date. Deleting db and it will be rebuilt.");
            std::fs::remove_file(path).unwrap();

            connection = create_connection(path).await?;

            create_tables(&mut connection).await?;
            create_indices(&mut connection).await?;

            set_db_version(&mut connection).await?;
        }

        set_pragmas(&mut connection).await?;

        Ok(SqlView {
            connection,
            secret_keys,
        })
    }

    pub async fn append_batch(
        &mut self,
        items: &[(Sequence, Vec<u8>)],
    ) -> Result<(), SqlViewError> {
        trace!("Start batch append");

        let secret_keys = self.secret_keys.to_owned();
        let items_cloned = items.to_owned();
        self.connection
            .transaction::<'_, _, _, SqlViewError>(move |mut conn| {
                Box::pin(async move {
                    for item in items_cloned {
                        append_item(&mut conn, &secret_keys, &item.0, &item.1).await?;
                    }
                    Ok(())
                })
            })
            .await?;

        Ok(())
    }

    pub async fn check_db_integrity(&mut self) -> Result<(), SqlViewError> {
        let res: String = query("PRAGMA integrity_check")
            .map(|row: SqliteRow| -> String { row.get(0) })
            .fetch_one(&mut self.connection)
            .await?;

        if res == "ok" {
            Ok(())
        } else {
            Err(SqlViewError::DbFailedIntegrityCheck {}.into())
        }
    }

    pub async fn get_latest(&mut self) -> Result<Option<Sequence>, SqlViewError> {
        let res: Option<i64> = query("SELECT MAX(log_seq) FROM msgs")
            .map(|row: SqliteRow| row.get(0))
            .fetch_optional(&mut self.connection)
            .await?;

        trace!("got latest seq from db: {:?}", res);

        Ok(res.map(|v| v as Sequence))
    }
}

fn attempt_decryption(mut msg: Msg<Value>, secret_keys: &[Keypair]) -> (bool, Msg<Value>) {
    let mut is_decrypted = false;

    if let Value::String(ref content) = msg.value.content {
        let string = content.trim_end_matches(".box");

        let decoded = b64.decode(string);
        if let Ok(bytes) = decoded {
            for secret_key in secret_keys {
                if let Some(decrypted) = private_box::decrypt(&bytes, secret_key) {
                    is_decrypted = true;
                    if let Ok(new_content) = serde_json::from_slice(&decrypted) {
                        msg.value.content = new_content;
                    }
                    break;
                }
            }
        }
    };

    (is_decrypted, msg)
}

async fn append_item(
    connection: &mut SqliteConnection,
    secret_keys: &[Keypair],
    log_seq: &Sequence,
    item: &[u8],
) -> Result<(), SqlViewError> {
    let msg: Msg<Value> = serde_json::from_slice(item).unwrap();

    let (is_decrypted, msg) = attempt_decryption(msg, secret_keys);

    let msg_ref_id = find_or_create_msg_ref(connection, &msg.key).await?;

    if !msg.value.content.is_object() {
        // early return if content is not object
        // eprintln!("No content: {:?}", msg.value.content);
        return Ok(());
    }

    let content_result: Result<MsgContent, JsonError> = from_value(msg.value.content.clone());

    let content = match content_result {
        Ok(content) => content,
        Err(error) => {
            // early return if content is misformatted
            // eprintln!("Error: {}", error);
            // eprintln!("-> Content: {:?}", msg.value.content);
            // return Err(error.into());
            return Ok(());
        }
    };

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
            if let Some(branch) = post.branch {
                insert_branches(connection, branch.as_slice(), msg_ref_id).await?;
            }
            insert_msg(connection, &msg, log_seq, msg_ref_id, is_decrypted).await?;
        }
        MsgContent::Contact(contact) => {
            insert_or_update_contacts(connection, &msg, &contact, msg_ref_id, is_decrypted).await?;
            insert_msg(connection, &msg, log_seq, msg_ref_id, is_decrypted).await?;
        }
        MsgContent::Vote(vote) => {
            insert_or_update_votes(connection, &msg, &vote).await?;
            insert_msg(connection, &msg, log_seq, msg_ref_id, is_decrypted).await?;
        }
        MsgContent::About(about) => {
            insert_abouts(connection, &msg, &about).await?;
            insert_msg(connection, &msg, log_seq, msg_ref_id, is_decrypted).await?;
        }
        MsgContent::Unknown => {
            // println!("Unknown content: {:?}", msg.value.content);
        }
    }

    Ok(())
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
