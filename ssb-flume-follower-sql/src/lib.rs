use std::fs::OpenOptions;

use flumedb::{BidirIterator, FlumeLog, IterAtOffset, OffsetLog, OffsetLogIter, Sequence};

use itertools::Itertools;
use private_box::Keypair;

pub mod sql;
pub use sql::FlumeViewSql;
use sql::FlumeViewSqlError;

pub struct SsbQuery {
    view: FlumeViewSql,
    log: OffsetLog<u32>,
}

impl SsbQuery {
    pub fn new(
        log_path: String,
        view_path: String,
        keys: Vec<Keypair>,
        pub_key: &str,
    ) -> Result<SsbQuery, FlumeViewSqlError> {
        let log_file = OpenOptions::new()
            .read(true)
            .write(false)
            .create(false)
            .open(&log_path)
            .unwrap();
        let log = OffsetLog::<u32>::from_file(log_file).unwrap();
        let view = FlumeViewSql::new(&view_path, keys, pub_key)?;

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
}
