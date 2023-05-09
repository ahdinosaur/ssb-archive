use base64::engine::{general_purpose::STANDARD as b64, Engine};
use flumedb::FlumeOffsetLogError;
use flumedb::{FlumeLog, IterAtOffset, OffsetLog, Sequence};
use itertools::Itertools;
use log::{info, trace};
use private_box::Keypair;
use serde_json::{from_value, Error as JsonError, Value};
use sqlx::{Connection, SqliteConnection};
use ssb_msg::{Msg, MsgContent};
use ssb_ref::{FeedRef, MsgRef};
use std::{fs::OpenOptions, io};
use thiserror::Error as ThisError;

pub mod sql;
pub use sql::SelectAllMsgsByFeedOptions;
use sql::*;

pub struct Database {
    sql: SqliteConnection,
    log: OffsetLog<u32>,
    keys: Vec<Keypair>,
}

#[derive(Debug, ThisError)]
pub enum Error {
    #[error("Failed to remove file, cause: {0}")]
    RemoveFile(#[source] io::Error),
    #[error("Failed to open file, cause: {0}")]
    OpenFile(#[source] io::Error),
    #[error("Failed to create log from file, cause: {0}")]
    LogFromFile(#[source] FlumeOffsetLogError),
    #[error("Failed to get from log, cause: {0}")]
    LogGet(#[source] FlumeOffsetLogError),
    #[error("Json error, cause: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Sql error, cause: {0}")]
    Sql(#[from] sqlx::Error),
    #[error("Sql database failed integrity check")]
    SqlIntegrityCheckFailure {},
}

impl Database {
    pub async fn new(
        log_path: String,
        sql_path: String,
        keys: Vec<Keypair>,
    ) -> Result<Self, Error> {
        let log_file = OpenOptions::new()
            .read(true)
            .write(false)
            .create(false)
            .open(&log_path)
            .map_err(Error::OpenFile)?;
        let log = OffsetLog::<u32>::from_file(log_file).map_err(Error::LogFromFile)?;

        let mut sql = create_connection(&sql_path).await?;

        if let Ok(false) = is_db_up_to_date(&mut sql).await {
            info!("sqlite db is out of date. Deleting db and it will be rebuilt.");
            std::fs::remove_file(&sql_path).map_err(Error::RemoveFile)?;

            sql = create_connection(&sql_path).await?;
            setup_new_db(&mut sql).await?;
        }
        setup_db(&mut sql).await?;

        Ok(Self { sql, log, keys })
    }

    pub async fn get_log_latest(&self) -> Option<Sequence> {
        self.log.latest()
    }

    pub async fn get_sql_latest(&mut self) -> Result<Option<Sequence>, Error> {
        Ok(get_latest(&mut self.sql).await?)
    }

    pub async fn process(&mut self, chunk_size: u64) -> Result<(), Error> {
        let latest = self.get_sql_latest().await?;

        //If the latest is 0, we haven't got anything in the db. Don't skip the very first
        //element in the offset log. I know this isn't super nice. It could be refactored later.
        let num_to_skip: u64 = match latest {
            None => 0,
            Some(_) => 1,
        };

        for chunk in self
            .log
            .iter_at_offset(latest.unwrap_or(0))
            .skip(num_to_skip as usize)
            .take(chunk_size as usize)
            .map(|data| (data.offset, data.data)) //TODO log_latest might not be the right thing
            .chunks(1000)
            .into_iter()
        {
            let vec = chunk.collect_vec();
            append_batch(&mut self.sql, &self.keys, &vec).await?;
        }

        Ok(())
    }

    // queries

    pub async fn get_msg(&mut self, msg_ref: MsgRef) -> Result<Option<Msg<Value>>, Error> {
        let log_seq_opt = get_msg_log_seq(&mut self.sql, &msg_ref).await?;
        if let Some(log_seq) = log_seq_opt {
            let bytes = self.log.get(log_seq).map_err(Error::LogGet)?;
            let msg: Msg<Value> = serde_json::from_slice(bytes.as_slice())?;
            Ok(Some(msg))
        } else {
            Ok(None)
        }
    }

    pub async fn get_all_msgs_by_feed(
        &mut self,
        options: SelectAllMsgsByFeedOptions<'_>,
    ) -> Result<Vec<Msg<Value>>, Error> {
        let log_seqs = select_all_msg_log_seqs_by_feed(&mut self.sql, options).await?;
        let mut msgs: Vec<Msg<Value>> = Vec::new();
        for log_seq in log_seqs {
            println!("log_seq: {}", log_seq);
            let bytes = self.log.get(log_seq).map_err(Error::LogGet)?;
            let msg: Msg<Value> = serde_json::from_slice(bytes.as_slice())?;
            msgs.push(msg)
        }
        Ok(msgs)
    }

    pub async fn get_max_seq_by_feed(&mut self, feed_ref: &FeedRef) -> Result<i64, Error> {
        Ok(select_max_seq_by_feed(&mut self.sql, feed_ref).await?)
    }
}

async fn append_batch(
    sql: &mut SqliteConnection,
    secret_keys: &[Keypair],
    items: &[(Sequence, Vec<u8>)],
) -> Result<(), Error> {
    trace!("Start batch append");

    let secret_keys = secret_keys.to_owned();
    let items_cloned = items.to_owned();
    sql.transaction::<'_, _, _, Error>(move |mut conn| {
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

async fn append_item(
    sql: &mut SqliteConnection,
    secret_keys: &[Keypair],
    log_seq: &Sequence,
    item: &[u8],
) -> Result<(), Error> {
    let msg: Msg<Value> = serde_json::from_slice(item)?;

    let is_encrypted = !msg.value.content.is_object();
    let (is_decrypted, msg) = attempt_decryption(msg, secret_keys);

    let msg_ref_id = find_or_create_msg_ref(sql, &msg.key).await?;
    insert_msg(sql, &msg, log_seq, msg_ref_id, is_encrypted, is_decrypted).await?;

    if is_encrypted && !is_decrypted {
        // early return if content is encrypted and not decrypted
        // eprintln!("No content: {:?}", msg.value.content);
        return Ok(());
    }

    let content_result: Result<MsgContent, JsonError> = from_value(msg.value.content.clone());

    let content = match content_result {
        Ok(content) => content,
        Err(_error) => {
            // early return if content is misformatted
            // eprintln!("Error: {}", error);
            // eprintln!("-> Content: {:?}", msg.value.content);
            // return Err(error.into());
            return Ok(());
        }
    };

    insert_content(sql, &msg, &content, msg_ref_id, is_decrypted).await?;

    Ok(())
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
