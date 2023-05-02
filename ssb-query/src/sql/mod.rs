use base64::engine::{general_purpose::STANDARD as b64, Engine};
use flumedb::flume_view::{FlumeView, Sequence};
use log::{info, trace};
use private_box::Keypair;
use serde_derive::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{
    query,
    sqlite::{SqliteConnection, SqliteRow},
    Connection, Error as SqlError, Row,
};
use thiserror::Error as ThisError;

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

#[derive(Serialize, Deserialize, Debug)]
pub struct SsbValue {
    pub author: String,
    pub sequence: u32,
    pub timestamp: f64,
    pub content: Value,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SsbMessage {
    pub key: String,
    pub value: SsbValue,
    pub timestamp: f64,
}

#[derive(Debug, ThisError)]
pub enum SqlViewError {
    #[error("Db failed integrity check")]
    DbFailedIntegrityCheck {},
    #[error("Sql error")]
    Sql(#[from] SqlError),
}

pub struct SqlView {
    pub connection: SqliteConnection,
    secret_keys: Vec<Keypair>,
}

async fn create_connection(path: &str) -> Result<SqliteConnection, SqlError> {
    SqliteConnection::connect(path).await
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

            create_tables(&mut connection)?;
            create_indices(&mut connection)?;
            create_views(&mut connection)?;

            set_db_version(&mut connection)?;
            set_author_that_is_me(&mut connection, pub_key)?;
        }

        set_pragmas(&mut connection);

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

        let secret_keys = &self.secret_keys;
        self.connection
            .transaction::<_, _, SqlError>(|mut conn| {
                Box::pin(async move {
                    for item in items {
                        append_item(&mut conn, secret_keys, &item.0, &item.1).await?;
                    }
                    Ok(())
                })
            })
            .await;

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

    pub fn get_latest(&self) -> Result<Option<Sequence>, SqlViewError> {
        let mut stmt = self
            .connection
            .prepare_cached("SELECT MAX(flume_seq) FROM messages_raw")?;

        Ok(stmt.query_row((), |row| {
            let res: Option<i64> = row.get(0).ok();
            trace!("got latest seq from db: {:?}", res);
            Ok(res.map(|v| v as Sequence))
        })?)
    }
}

fn find_values_in_object_by_key<'a>(
    obj: &'a serde_json::Value,
    key: &str,
    values: &mut Vec<&'a serde_json::Value>,
) {
    if let Some(val) = obj.get(key) {
        values.push(val)
    }

    match obj {
        Value::Array(arr) => {
            for val in arr {
                find_values_in_object_by_key(val, key, values);
            }
        }
        Value::Object(kv) => {
            for val in kv.values() {
                match val {
                    Value::Object(_) => find_values_in_object_by_key(val, key, values),
                    Value::Array(_) => find_values_in_object_by_key(val, key, values),
                    _ => (),
                }
            }
        }
        _ => (),
    }
}

fn attempt_decryption(mut message: SsbMessage, secret_keys: &[Keypair]) -> (bool, SsbMessage) {
    let mut is_decrypted = false;

    message = match message.value.content["type"] {
        Value::Null => {
            let content = message.value.content.clone();
            let string = &content.as_str().unwrap();

            let string = string.trim_end_matches(".box");

            let decoded = b64.decode(string);
            if let Ok(bytes) = decoded {
                for secret_key in secret_keys {
                    message.value.content = private_box::decrypt(&bytes, secret_key)
                        .and_then(|data| {
                            is_decrypted = true;
                            serde_json::from_slice(&data).ok()
                        })
                        .unwrap_or(Value::Null); //If we can't decrypt it, throw it away.

                    if is_decrypted {
                        break;
                    }
                }
            }

            message
        }
        _ => message,
    };

    (is_decrypted, message)
}

async fn append_item(
    connection: &mut SqliteConnection,
    secret_keys: &[Keypair],
    seq: &Sequence,
    item: &[u8],
) -> Result<(), SqlError> {
    let message: SsbMessage = serde_json::from_slice(item).unwrap();

    let (is_decrypted, message) = attempt_decryption(message, secret_keys);

    let message_key_id = find_or_create_key(&mut SqliteConnection, &message.key).unwrap();

    // votes are a kind of backlink, but we want to put them in their own table.
    match &message.value.content["type"] {
        Value::String(type_string) if type_string == "vote" => {
            insert_or_update_votes(connection, &message);
        }
        _ => {
            let mut links = Vec::new();
            find_values_in_object_by_key(&message.value.content, "link", &mut links);
            insert_links(connection, links.as_slice(), message_key_id);
            insert_mentions(connection, links.as_slice(), message_key_id);
            insert_blob_links(connection, links.as_slice(), message_key_id);
        }
    }

    insert_branches(connection, &message, message_key_id);
    insert_message(
        connection,
        &message,
        seq as i64,
        message_key_id,
        is_decrypted,
    )?;
    insert_or_update_contacts(connection, &message, message_key_id, is_decrypted);
    insert_abouts(connection, &message, message_key_id);

    Ok(())
}

fn set_pragmas(connection: &mut SqliteConnection) {
    connection.execute("PRAGMA synchronous = OFF", ()).unwrap();
    connection.execute("PRAGMA page_size = 4096", ()).unwrap();
}

fn create_tables(connection: &mut SqliteConnection) -> Result<(), SqlError> {
    create_migrations_tables(connection)?;
    create_messages_tables(connection)?;
    create_authors_tables(connection)?;
    create_keys_tables(connection)?;
    create_links_tables(connection)?;
    create_contacts_tables(connection)?;
    create_branches_tables(connection)?;
    create_mentions_tables(connection)?;
    create_abouts_tables(connection)?;
    create_blobs_tables(connection)?;
    create_blob_links_tables(connection)?;
    create_votes_tables(connection)?;

    Ok(())
}

fn create_views(connection: &mut SqliteConnection) -> Result<(), SqlError> {
    create_messages_views(connection)?;
    create_links_views(connection)?;
    create_blob_links_views(connection)?;
    create_abouts_views(connection)?;
    create_mentions_views(connection)?;
    create_votes_indices(connection)?;
    Ok(())
}

fn create_indices(connection: &mut SqliteConnection) -> Result<(), SqlError> {
    create_messages_indices(connection)?;
    create_links_indices(connection)?;
    create_blob_links_indices(connection)?;
    create_contacts_indices(connection)?;
    create_keys_indices(connection)?;
    create_branches_indices(connection)?;
    create_authors_indices(connection)?;
    create_abouts_indices(connection)?;
    create_mentions_indices(connection)?;
    Ok(())
}

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
        SqlView::new(filename, keys, "").unwrap();
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
