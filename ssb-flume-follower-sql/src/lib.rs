extern crate failure;
extern crate failure_derive;

extern crate log;

extern crate itertools;

extern crate serde;
extern crate serde_derive;
extern crate serde_json;

extern crate base64;
extern crate flumedb;
extern crate private_box;
extern crate rusqlite;

use failure::Error;

use flumedb::BidirIterator;
use flumedb::OffsetLogIter;
use flumedb::Sequence;

use itertools::Itertools;
use private_box::SecretKey;

pub mod sql;
pub use sql::FlumeViewSql;

pub struct SsbQuery {
    view: FlumeViewSql,
    log_path: String,
}

impl SsbQuery {
    pub fn new(
        log_path: String,
        view_path: String,
        keys: Vec<SecretKey>,
        pub_key: &str,
    ) -> Result<SsbQuery, Error> {
        let view = FlumeViewSql::new(&view_path, keys, pub_key)?;

        Ok(SsbQuery { view, log_path })
    }

    pub fn get_latest(&self) -> Sequence {
        self.view.get_latest().unwrap()
    }

    pub fn process(&mut self, num_items: i64) {
        let latest = self.get_latest();

        //If the latest is 0, we haven't got anything in the db. Don't skip the very first
        //element in the offset log. I know this isn't super nice. It could be refactored later.
        let num_to_skip = match latest {
            0 => 0,
            _ => 1,
        };
        let log_path = self.log_path.clone();
        let file = std::fs::File::open(log_path.clone()).unwrap();

        let items_to_take = match num_items {
            -1 => std::usize::MAX,
            n => n as usize,
        };

        OffsetLogIter::<u32>::with_starting_offset(file, latest)
            .forward()
            .skip(num_to_skip)
            .take(items_to_take)
            .map(|data| (data.offset + latest, data.data)) //TODO log_latest might not be the right thing
            .chunks(1000)
            .into_iter()
            .for_each(|chunk| {
                self.view.append_batch(&chunk.collect_vec());
            })
    }
}
