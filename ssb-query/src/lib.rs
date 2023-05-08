use flumedb::FlumeOffsetLogError;
use flumedb::{FlumeLog, IterAtOffset, OffsetLog, Sequence};
use itertools::Itertools;
use private_box::Keypair;
use serde_json::Value;
use ssb_msg::Msg;
use ssb_ref::{FeedRef, MsgRef};
use std::fs::OpenOptions;
use thiserror::Error as ThisError;

pub mod sql;
pub use sql::SelectAllMsgsByFeedOptions;
use sql::{
    get_msg_log_seq, select_all_msg_log_seqs_by_feed, select_max_seq_by_feed, SqlView, SqlViewError,
};

pub struct SsbQuery {
    view: SqlView,
    log: OffsetLog<u32>,
}

#[derive(Debug, ThisError)]
pub enum QueryError {
    #[error("Log error: {0}")]
    Log(#[from] FlumeOffsetLogError),
    #[error("View error: {0}")]
    View(#[from] SqlViewError),
    #[error("Json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Sql error: {0}")]
    Sql(#[from] sqlx::Error),
}

impl SsbQuery {
    pub async fn new(
        log_path: String,
        view_path: String,
        keys: Vec<Keypair>,
    ) -> Result<SsbQuery, QueryError> {
        let log_file = OpenOptions::new()
            .read(true)
            .write(false)
            .create(false)
            .open(&log_path)
            .unwrap();
        let log = OffsetLog::<u32>::from_file(log_file).unwrap();
        let view = SqlView::new(&view_path, keys).await?;

        Ok(SsbQuery { view, log })
    }

    pub async fn get_log_latest(&self) -> Option<Sequence> {
        self.log.latest()
    }

    pub async fn get_view_latest(&mut self) -> Option<Sequence> {
        self.view.get_latest().await.unwrap()
    }

    pub async fn process(&mut self, chunk_size: u64) -> Result<(), QueryError> {
        let latest = self.get_view_latest().await;

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
            self.view.append_batch(&vec).await?;
        }

        Ok(())
    }

    // queries

    pub async fn get_msg(&mut self, msg_ref: MsgRef) -> Result<Option<Msg<Value>>, QueryError> {
        let log_seq_opt = get_msg_log_seq(&mut self.view.connection, &msg_ref).await?;
        if let Some(log_seq) = log_seq_opt {
            let bytes = self.log.get(log_seq)?;
            let msg: Msg<Value> = serde_json::from_slice(bytes.as_slice())?;
            Ok(Some(msg))
        } else {
            Ok(None)
        }
    }

    pub async fn get_all_msgs_by_feed(
        &mut self,
        options: SelectAllMsgsByFeedOptions<'_>,
    ) -> Result<Vec<Msg<Value>>, QueryError> {
        let log_seqs = select_all_msg_log_seqs_by_feed(&mut self.view.connection, options).await?;
        let mut msgs: Vec<Msg<Value>> = Vec::new();
        for log_seq in log_seqs {
            println!("log_seq: {}", log_seq);
            let bytes = self.log.get(log_seq)?;
            let msg: Msg<Value> = serde_json::from_slice(bytes.as_slice())?;
            msgs.push(msg)
        }
        Ok(msgs)
    }

    pub async fn get_max_seq_by_feed(&mut self, feed_ref: &FeedRef) -> Result<i64, QueryError> {
        Ok(select_max_seq_by_feed(&mut self.view.connection, feed_ref).await?)
    }
}
