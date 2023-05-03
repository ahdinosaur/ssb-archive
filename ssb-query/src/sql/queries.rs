use crate::sql::*;
use crate::SsbMessage;
use sqlx::{query, Error, Row, SqliteConnection};

pub async fn select_max_seq_by_feed<'a>(
    connection: &mut SqliteConnection,
    feed_id: &'a str,
) -> Result<i64, Error> {
    let max_seq: i64 = query(
        "
        SELECT
          MAX(seq)
        FROM messages_raw
        JOIN authors ON authors.id = messages_raw.author_id
        WHERE
            authors.author = ?
        LIMIT 1
        ",
    )
    .bind(feed_id)
    .fetch_one(connection)
    .await?
    .get(0);

    Ok(max_seq)
}

pub struct SelectAllMessagesByFeedOptions<'a> {
    pub feed_id: &'a str,
    pub content_type: &'a str,
    pub page_size: i64,
    pub less_than_seq: i64,
    pub is_decrypted: bool,
}

pub async fn select_all_messages_by_feed<'a>(
    connection: &mut SqliteConnection,
    options: SelectAllMessagesByFeedOptions<'a>,
) -> Result<Vec<SsbMessage>, Error> {
    let rows = query(
        "
        SELECT
            seq,
            keys.key as key,
            authors.author as author,
            received_time,
            asserted_time,
            content,
            is_decrypted
        FROM messages_raw
        JOIN keys ON keys.id = messages_raw.key_id
        JOIN authors ON authors.id = messages_raw.author_id
        WHERE
            authors.author = ?
            AND content_type = ?
            AND seq < ?
            AND is_decrypted = ?
        ORDER BY seq DESC
        LIMIT ?
        ",
    )
    .bind(options.feed_id)
    .bind(options.content_type)
    .bind(options.less_than_seq)
    .bind(options.is_decrypted)
    .bind(options.page_size)
    .fetch_all(connection)
    .await?;

    let messages = rows
        .into_iter()
        .map(|row| {
            Ok(SsbMessage {
                key: row.get(1),
                value: SsbValue {
                    author: row.get(2),
                    sequence: row.get(0),
                    timestamp: row.get(4),
                    content: row.get(5),
                },
                timestamp: row.get(3),
            })
        })
        .collect::<Result<Vec<SsbMessage>, Error>>()?;

    Ok(messages)
}
// select all posts by a user
//   - greater than seq
//   - limit 10
/*
SELECT
  seq,
  keys.key as key,
  authors.author as author,
  asserted_time,
  content_type,
  content,
  is_decrypted,
  root_keys.key as root,
  fork_keys.key as fork
FROM messages_raw
JOIN keys ON keys.id=messages_raw.key_id
LEFT JOIN keys AS root_keys ON root_keys.id=messages_raw.root_id
LEFT JOIN keys AS fork_keys ON fork_keys.id=messages_raw.fork_id
JOIN authors ON authors.id=messages_raw.author_id
WHERE
        authors.author = '@6ilZq3kN0F+dXFHAPjAwMm87JEb/VdB+LC9eIMW3sa0=.ed25519'
        AND content_type = 'post'
        AND seq > 10
LIMIT 10
*/

#[derive(Serialize, Deserialize, Debug)]
pub enum Link {
    Out {
        id: String,
        author: String,
        timestamp: f64,
    },
    Back {
        id: String,
        author: String,
        timestamp: f64,
    },
}

pub async fn select_out_links_by_message(
    connection: &mut SqliteConnection,
    id: &str,
) -> Result<Vec<Link>, Error> {
    /*
        SELECT
            links.link_from_key as id,
            messages.author as author,
            messages.received_time as timestamp
        FROM links
        JOIN messages ON messages.key = links.link_from_key
        WHERE link_to_key = ?
        AND NOT root = ?
        AND NOT content_type = 'about'
        AND NOT content_type = 'vote'
        AND NOT content_type = 'tag'
    */
    let rows = query(
        "
        SELECT
                links.link_to_key as id,
                authors.author as author,
                messages_raw.asserted_time as timestamp
        FROM links
        JOIN keys ON keys.key = links.link_to_key
        JOIN messages_raw ON messages_raw.key_id = keys.id
        JOIN authors ON authors.id = messages_raw.author_id
        LEFT JOIN keys AS root_keys ON root_keys.id = messages_raw.root_id
        WHERE link_from_key = ?1
        AND root_keys.key = ?2
        AND content_type = ?3
",
    )
    .bind(id)
    .bind(id)
    .bind("post")
    .fetch_all(connection)
    .await?;

    let out_links = rows
        .into_iter()
        .map(|row| {
            Ok(Link::Back {
                id: row.get(0),
                author: row.get(1),
                timestamp: row.get(2),
            })
        })
        .collect::<Result<Vec<Link>, Error>>()?;

    Ok(out_links)
}

pub async fn select_back_links_by_message(
    connection: &mut SqliteConnection,
    id: &str,
) -> Result<Vec<Link>, Error> {
    /*
        SELECT
            links.link_from_key as id,
            messages.author as author,
            messages.received_time as timestamp
        FROM links
        JOIN messages ON messages.key = links.link_from_key
        WHERE link_to_key = ?
        AND NOT root = ?
        AND NOT content_type = 'about'
        AND NOT content_type = 'vote'
        AND NOT content_type = 'tag'
    */
    let rows = query(
        "
        SELECT
                links.link_from_key as id,
                authors.author as author,
                messages_raw.asserted_time as timestamp
        FROM links
        JOIN keys ON keys.key = links.link_from_key
        JOIN messages_raw ON messages_raw.key_id = keys.id
        JOIN authors ON authors.id = messages_raw.author_id
        LEFT JOIN keys AS root_keys ON root_keys.id = messages_raw.root_id
        WHERE link_to_key = ?1
        AND root_keys.key = ?2
        AND content_type = ?3
",
    )
    .bind(id)
    .bind(id)
    .bind("post")
    .fetch_all(connection)
    .await?;

    let back_links = rows
        .into_iter()
        .map(|row| {
            Ok(Link::Back {
                id: row.get(0),
                author: row.get(1),
                timestamp: row.get(2),
            })
        })
        .collect::<Result<Vec<Link>, Error>>()?;

    Ok(back_links)
}

/*
pub fn how_many_friends_follow_id() {}
pub fn who_is_friends_with_id() {}
pub fn who_does_id_follow_one_way() {}
pub fn who_does_follows_id_one_way() {}

pub fn friends_two_hops(connection: Connection) {
    //"
    //SELECT
    //author as id
    //FROM
    //authors
    //WHERE authors.id IN (
    //SELECT
    //contact_author_id
    //FROM contacts_raw
    //WHERE author_id == 1 AND state == 1
    //UNION
    //SELECT
    //friend_contacts_raw.contact_author_id
    //FROM contacts_raw
    //join contacts_raw AS friend_contacts_raw ON friend_contacts_raw.author_id == contacts_raw.contact_author_id
    //WHERE contacts_raw.author_id == 1
    //AND contacts_raw.state == 1
    //AND friend_contacts_raw.state == 1
    //EXCEPT
    //SELECT
    //contact_author_id
    //FROM contacts_raw
    //WHERE author_id == 1
    //AND state == -1)"
}
#[cfg(test)]
mod test {
    use crate::sql::queries::back_link_references;
    use crate::*;
    use flumedb::offset_log::OffsetLogIter;
    use flumedb::BidirIterator;
    use itertools::Itertools;

    #[test]
    fn find_backlinks_refs() {
        let view = create_test_db(
            5000,
            "/home/piet/.ssb/flume/log.offset",
            "/tmp/backlinks.sqlite3",
        );
        let connection = &view.connection;
        let links = back_link_references(
            connection,
            "%ZEuQdC7OBxDgRg2Vv/VgjArRIpE5YwIMo6ufXqaWaGg=.sha256",
            0.0,
        );
        assert_eq!(links.unwrap().len(), 1);
    }
    fn create_test_db(num_entries: usize, offset_filename: &str, db_filename: &str) -> SqlView {
        let keys = Vec::new();
        std::fs::remove_file(db_filename).unwrap_or(());
        let mut view = SqlView::new(db_filename, keys, "").unwrap();

        let file = std::fs::File::open(offset_filename.to_string()).unwrap();

        OffsetLogIter::<u32>::new(file)
            .forward()
            .take(num_entries)
            .map(|data| (data.offset, data.data))
            .chunks(1000 as usize)
            .into_iter()
            .for_each(|chunk| {
                view.append_batch(&chunk.collect_vec());
            });

        view
    }
}
*/
