use base64::{
    engine::{general_purpose::STANDARD as b64, Engine},
    DecodeError,
};
use lazy_static::lazy_static;
use regex::Regex;

pub fn link_id_multi_regex() -> &'static Regex {
    lazy_static! {
        static ref RE: Regex = combine_regexes(vec![
            message_id_multi_regex(),
            feed_id_multi_regex(),
            blob_id_multi_regex(),
        ]);
    }
    &*RE
}

pub fn message_id_single_regex() -> &'static Regex {
    lazy_static! {
        static ref RE: Regex = canonical_base64("%", ".sha256", 32, true);
    }
    &*RE
}

pub fn message_id_multi_regex() -> &'static Regex {
    lazy_static! {
        static ref RE: Regex = canonical_base64("%", ".sha256", 32, false);
    }
    &*RE
}

pub fn is_message_id(string: &str) -> bool {
    let regex = message_id_single_regex();
    regex.is_match(string)
}

pub fn parse_message_id_data(id: &str) -> Result<Vec<u8>, DecodeError> {
    let base64_data = &id[1..id.len() - 7];
    b64.decode(base64_data)
}

pub fn feed_id_single_regex() -> &'static Regex {
    lazy_static! {
        static ref RE: Regex = canonical_base64("@", ".ed25519", 32, true);
    }
    &*RE
}

pub fn feed_id_multi_regex() -> &'static Regex {
    lazy_static! {
        static ref RE: Regex = canonical_base64("@", ".ed25519", 32, false);
    }
    &*RE
}

pub fn is_feed_id(string: &str) -> bool {
    let regex = feed_id_single_regex();
    regex.is_match(string)
}

pub fn parse_feed_id_data(id: &str) -> Result<Vec<u8>, DecodeError> {
    let base64_data = &id[1..id.len() - 8];
    b64.decode(base64_data)
}

pub fn blob_id_single_regex() -> &'static Regex {
    lazy_static! {
        static ref RE: Regex = canonical_base64("&", ".sha256", 32, true);
    }
    &*RE
}

pub fn blob_id_multi_regex() -> &'static Regex {
    lazy_static! {
        static ref RE: Regex = canonical_base64("&", ".sha256", 32, false);
    }
    &*RE
}

pub fn is_blob_id(string: &str) -> bool {
    let regex = blob_id_single_regex();
    regex.is_match(string)
}

pub fn parse_blob_id_data(id: &str) -> Result<Vec<u8>, DecodeError> {
    let base64_data = &id[1..id.len() - 7];
    b64.decode(base64_data)
}

// https://github.com/dominictarr/is-canonical-base64/blob/master/index.js
fn canonical_base64(prefix: &str, suffix: &str, length: u32, include_start_and_end: bool) -> Regex {
    let char = "[a-zA-Z0-9/+]";
    let trail2 = "[AQgw]==";
    let trail4 = "[AEIMQUYcgkosw048]=";

    let mut re = String::new();
    if include_start_and_end {
        re.push_str("^");
    }
    re.push_str(prefix);
    re.push_str(char);
    re.push_str("{");
    re.push_str(&(!!((length * 8) / 6)).to_string());
    re.push_str("}");

    let pad = length % 3;
    re.push_str(if pad == 0 {
        ""
    } else if pad == 1 {
        trail2
    } else {
        trail4
    });

    re.push_str(suffix);
    if include_start_and_end {
        re.push_str("$");
    }

    Regex::new(&re).unwrap()
}

fn combine_regexes(regexes: Vec<&Regex>) -> Regex {
    let mut string = String::new();
    string.push_str("(");
    string.push_str(
        regexes
            .into_iter()
            .map(|regex| regex.as_str())
            .collect::<Vec<&str>>()
            .join("|")
            .as_str(),
    );
    string.push_str(")");
    Regex::new(&string).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_message_id() {
        assert!(is_message_id(
            "%pGzeEydYdHjKW1iIchR0Yumydsr3QSp8+FuYcwVwi8Q=.sha256"
        ));
        assert!(is_message_id(
            "%09abcdefghyq9KH6dYMc/g17L04jDbl1py8arGQmL1I=.sha256"
        ));
    }

    #[test]
    fn test_parse_message_id_data() {
        assert_eq!(
            parse_message_id_data("%pGzeEydYdHjKW1iIchR0Yumydsr3QSp8+FuYcwVwi8Q=.sha256").unwrap(),
            b64.decode("pGzeEydYdHjKW1iIchR0Yumydsr3QSp8+FuYcwVwi8Q=")
                .unwrap()
        );
    }

    #[test]
    fn test_is_feed_id() {
        assert!(is_feed_id(
            "@jEA8WSl0URsB/g/XYG5zCGBkMOyTeBZfGtbw3RJMIuk=.ed25519"
        ));
    }

    #[test]
    fn test_parse_feed_id_data() {
        assert_eq!(
            parse_feed_id_data("@jEA8WSl0URsB/g/XYG5zCGBkMOyTeBZfGtbw3RJMIuk=.ed25519").unwrap(),
            b64.decode("jEA8WSl0URsB/g/XYG5zCGBkMOyTeBZfGtbw3RJMIuk=")
                .unwrap()
        );
    }

    #[test]
    fn test_is_blob_id() {
        assert!(is_blob_id(
            "&abcdefg6bIh5dmyss7QH7uMrQxz3LKvgjer68we30aQ=.sha256"
        ));
        assert!(is_blob_id(
            "&51ZXxNYIvTDCoNTE9R94NiEg3JAZAxWtKn4h4SmBwyY=.sha256"
        ));
    }

    #[test]
    fn test_parse_blob_id_data() {
        assert_eq!(
            parse_blob_id_data("&abcdefg6bIh5dmyss7QH7uMrQxz3LKvgjer68we30aQ=.sha256").unwrap(),
            b64.decode("abcdefg6bIh5dmyss7QH7uMrQxz3LKvgjer68we30aQ=")
                .unwrap()
        );
    }
}
