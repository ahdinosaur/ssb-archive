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
use ssb_core::{BlobLink, IdError, Link};
use std::str::FromStr;
use thiserror::Error as ThisError;

pub use ssb_core::{Msg, MsgContent, MsgValue};

mod abouts;
mod authors;
mod blob_links;
mod blobs;
mod branches;
mod contacts;
mod keys;
mod links;
mod mentions;
mod messages;
mod migrations;
mod queries;
mod votes;
use self::abouts::*;
use self::authors::*;
use self::blob_links::*;
use self::blobs::*;
use self::branches::*;
use self::contacts::*;
use self::keys::*;
use self::links::*;
use self::mentions::*;
use self::messages::*;
use self::migrations::*;
pub use self::queries::SelectAllMessagesByFeedOptions;
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
    #[error("Id format error: {0}")]
    IdFormat(#[from] IdError),
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
    pub async fn new(
        path: &str,
        secret_keys: Vec<Keypair>,
        pub_key: &str,
    ) -> Result<SqlView, SqlViewError> {
        let mut connection = create_connection(path).await?;

        if let Ok(false) = is_db_up_to_date(&mut connection).await {
            info!("sqlite db is out of date. Deleting db and it will be rebuilt.");
            std::fs::remove_file(path).unwrap();

            connection = create_connection(path).await?;

            create_tables(&mut connection).await?;
            create_indices(&mut connection).await?;
            create_views(&mut connection).await?;

            set_db_version(&mut connection).await?;
        }

        set_pragmas(&mut connection).await?;

        Ok(SqlView {
            connection,
            secret_keys,
        })
    }

    pub async fn get_seq_by_key(&mut self, key: &str) -> Result<i64, SqlViewError> {
        let result: i64 = query("SELECT flume_seq FROM messages_raw JOIN keys ON messages_raw.key_id=keys.id WHERE keys.key=?1")
            .bind(key)
            .map(|row: SqliteRow| row.get(0))
            .fetch_one(&mut self.connection).await?;

        Ok(result)
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
        let res: Option<i64> = query("SELECT MAX(flume_seq) FROM messages_raw")
            .map(|row: SqliteRow| row.get(0))
            .fetch_optional(&mut self.connection)
            .await?;

        trace!("got latest seq from db: {:?}", res);

        Ok(res.map(|v| v as Sequence))
    }
}

fn attempt_decryption(mut message: Msg<Value>, secret_keys: &[Keypair]) -> (bool, Msg<Value>) {
    let mut is_decrypted = false;

    if let Value::String(ref content) = message.value.content {
        let string = content.trim_end_matches(".box");

        let decoded = b64.decode(string);
        if let Ok(bytes) = decoded {
            for secret_key in secret_keys {
                if let Some(decrypted) = private_box::decrypt(&bytes, secret_key) {
                    is_decrypted = true;
                    if let Ok(new_content) = serde_json::from_slice(&decrypted) {
                        message.value.content = new_content;
                    }
                    break;
                }
            }
        }
    };

    (is_decrypted, message)
}

async fn append_item(
    connection: &mut SqliteConnection,
    secret_keys: &[Keypair],
    seq: &Sequence,
    item: &[u8],
) -> Result<(), SqlViewError> {
    let msg: Msg<Value> = serde_json::from_slice(item).unwrap();

    let (is_decrypted, msg) = attempt_decryption(msg, secret_keys);

    let msg_key_id = find_or_create_key(connection, &msg.key).await.unwrap();

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
            return Ok(());
        }
    };

    match content {
        MsgContent::Post(post) => {
            if let Some(mentions) = post.mentions {
                let mut msg_keys = Vec::new();
                let mut feed_keys = Vec::new();
                let mut blob_keys = Vec::new();
                let mut hashtag_keys = Vec::new();
                for link in mentions.iter() {
                    match link {
                        Link::Msg { link, .. } => msg_keys.push(link),
                        Link::Feed { link, .. } => feed_keys.push(link),
                        Link::Blob(BlobLink { link, .. }) => blob_keys.push(link),
                        Link::Hashtag { link } => hashtag_keys.push(link),
                    }
                }
                insert_links(connection, msg_keys.as_slice(), msg_key_id).await?;
                insert_mentions(connection, feed_keys.as_slice(), msg_key_id).await?;
                insert_blob_links(connection, blob_keys.as_slice(), msg_key_id).await?;
            }

            let root_key_id = if let Some(root) = post.root {
                trace!("get root key id");
                Some(find_or_create_key(connection, &root).await?)
            } else {
                None
            };
            let fork_key_id = if let Some(fork) = post.fork {
                trace!("get fork key id");
                Some(find_or_create_key(connection, &fork).await?)
            } else {
                None
            };
            insert_message(
                connection,
                &msg,
                *seq as i64,
                msg_key_id,
                root_key_id,
                fork_key_id,
                is_decrypted,
            )
            .await?;
            if let Some(branch) = post.branch {
                insert_branches(connection, branch.as_slice(), msg_key_id).await?;
            }
        }
        MsgContent::Contact(contact) => {
            insert_or_update_contacts(connection, &msg, &contact, msg_key_id, is_decrypted).await?;
            insert_message(
                connection,
                &msg,
                *seq as i64,
                msg_key_id,
                None,
                None,
                is_decrypted,
            )
            .await?;
        }
        MsgContent::Vote(vote) => {
            insert_or_update_votes(connection, &msg, &vote).await?;
            insert_message(
                connection,
                &msg,
                *seq as i64,
                msg_key_id,
                None,
                None,
                is_decrypted,
            )
            .await?;
        }
        MsgContent::About(about) => {
            insert_abouts(connection, &msg, &about, msg_key_id).await?;
            insert_message(
                connection,
                &msg,
                *seq as i64,
                msg_key_id,
                None,
                None,
                is_decrypted,
            )
            .await?;
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
    create_messages_tables(connection).await?;
    create_authors_tables(connection).await?;
    create_keys_tables(connection).await?;
    create_links_tables(connection).await?;
    create_contacts_tables(connection).await?;
    create_branches_tables(connection).await?;
    create_mentions_tables(connection).await?;
    create_abouts_tables(connection).await?;
    create_blobs_tables(connection).await?;
    create_blob_links_tables(connection).await?;
    create_votes_tables(connection).await?;

    Ok(())
}

async fn create_views(connection: &mut SqliteConnection) -> Result<(), SqlError> {
    create_messages_views(connection).await?;
    create_links_views(connection).await?;
    create_blob_links_views(connection).await?;
    create_abouts_views(connection).await?;
    create_mentions_views(connection).await?;
    create_votes_indices(connection).await?;
    Ok(())
}

async fn create_indices(connection: &mut SqliteConnection) -> Result<(), SqlError> {
    create_messages_indices(connection).await?;
    create_links_indices(connection).await?;
    create_blob_links_indices(connection).await?;
    create_contacts_indices(connection).await?;
    create_keys_indices(connection).await?;
    create_branches_indices(connection).await?;
    create_authors_indices(connection).await?;
    create_abouts_indices(connection).await?;
    create_mentions_indices(connection).await?;
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
    "author": "@QlCTpvY7p9ty2yOFrv1WU1AE88aoQc4Y7wYal7PFc+w=.ed25519",
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
      "mentions": []
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
