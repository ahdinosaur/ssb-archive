use std::fs::OpenOptions;

use flumedb::{FlumeLog, IterAtOffset, OffsetLog, Sequence};

use itertools::Itertools;
use private_box::Keypair;

pub mod sql;
pub use sql::SelectAllMessagesByFeedOptions;
use sql::SqlViewError;
use sql::{select_all_messages_by_feed, select_max_seq_by_feed};
pub use sql::{SqlView, SsbMessage, SsbValue};

pub struct SsbQuery {
    view: SqlView,
    log: OffsetLog<u32>,
}

impl SsbQuery {
    pub fn new(
        log_path: String,
        view_path: String,
        keys: Vec<Keypair>,
        pub_key: &str,
    ) -> Result<SsbQuery, SqlViewError> {
        let log_file = OpenOptions::new()
            .read(true)
            .write(false)
            .create(false)
            .open(&log_path)
            .unwrap();
        let log = OffsetLog::<u32>::from_file(log_file).unwrap();
        let view = SqlView::new(&view_path, keys, pub_key)?;

        Ok(SsbQuery { view, log })
    }

    pub fn get_log_latest(&self) -> Option<Sequence> {
        self.log.latest()
    }

    pub fn get_view_latest(&self) -> Option<Sequence> {
        self.view.get_latest().unwrap()
    }

    pub fn process(&mut self, chunk_size: u64) {
        let latest = self.get_view_latest();

        //If the latest is 0, we haven't got anything in the db. Don't skip the very first
        //element in the offset log. I know this isn't super nice. It could be refactored later.
        let num_to_skip: u64 = match latest {
            None => 0,
            Some(_) => 1,
        };

        self.log
            .iter_at_offset(latest.unwrap_or(0))
            .skip(num_to_skip as usize)
            .take(chunk_size as usize)
            .map(|data| (data.offset, data.data)) //TODO log_latest might not be the right thing
            .chunks(1000)
            .into_iter()
            .for_each(|chunk| {
                let vec = chunk.collect_vec();
                self.view.append_batch(&vec);
            })
    }

    // queries

    pub fn select_all_messages_by_feed(
        &self,
        options: SelectAllMessagesByFeedOptions,
    ) -> Result<Vec<SsbMessage>, SqlViewError> {
        Ok(select_all_messages_by_feed(&self.view.connection, options)?)
    }

    pub fn select_max_seq_by_feed(&self, feed_id: &str) -> Result<i64, SqlViewError> {
        Ok(select_max_seq_by_feed(&self.view.connection, feed_id)?)
    }
}
