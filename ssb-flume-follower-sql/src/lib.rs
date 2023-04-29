use std::fs::OpenOptions;

use flumedb::{BidirIterator, FlumeLog, OffsetLog, OffsetLogIter, Sequence};

use itertools::Itertools;
use private_box::Keypair;

pub mod sql;
pub use sql::FlumeViewSql;
use sql::FlumeViewSqlError;

pub struct SsbQuery {
    view: FlumeViewSql,
    log_path: String,
}

impl SsbQuery {
    pub fn new(
        log_path: String,
        view_path: String,
        keys: Vec<Keypair>,
        pub_key: &str,
    ) -> Result<SsbQuery, FlumeViewSqlError> {
        let view = FlumeViewSql::new(&view_path, keys, pub_key)?;

        Ok(SsbQuery { view, log_path })
    }

    pub fn get_log_latest(&self) -> Option<Sequence> {
        let log_file = OpenOptions::new()
            .read(true)
            .write(false)
            .create(false)
            .open(&self.log_path)
            .unwrap();
        let log = OffsetLog::<u32>::from_file(log_file).unwrap();
        log.latest()
    }

    pub fn get_view_latest(&self) -> Sequence {
        self.view.get_latest().unwrap()
    }

    pub fn process(&mut self, num_items: i64) {
        let latest = self.get_view_latest();

        //If the latest is 0, we haven't got anything in the db. Don't skip the very first
        //element in the offset log. I know this isn't super nice. It could be refactored later.
        let num_to_skip = match latest {
            0 => 0,
            _ => 1,
        };
        let log_file = OpenOptions::new()
            .read(true)
            .write(false)
            .create(false)
            .open(&self.log_path)
            .unwrap();

        let items_to_take = match num_items {
            -1 => std::usize::MAX,
            n => n as usize,
        };

        OffsetLogIter::<u32>::with_starting_offset(log_file, latest)
            .forward()
            .skip(num_to_skip)
            .take(items_to_take)
            .map(|data| (data.offset + latest, data.data)) //TODO log_latest might not be the right thing
            .chunks(1000)
            .into_iter()
            .for_each(|chunk| {
                let vec = chunk.collect_vec();
                println!("chunk: {:?}", &vec);
                self.view.append_batch(&vec);
            })
    }
}
