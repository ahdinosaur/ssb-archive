use base64::{
    engine::{general_purpose::STANDARD as b64, Engine},
    DecodeError,
};
use lazy_static::lazy_static;
use regex::Regex;

pub fn message_id_regex() -> &'static Regex {
    lazy_static! {
        static ref RE: Regex = canonical_base64("%", ".sha256", 32);
    }
    &*RE
}

pub fn is_message_id(string: &str) -> bool {
    let regex = message_id_regex();
    regex.is_match(string)
}

pub fn parse_message_id_data(id: &str) -> Result<Vec<u8>, DecodeError> {
    let base64_data = &id[1..id.len() - 7];
    b64.decode(base64_data)
}

pub fn feed_id_regex() -> &'static Regex {
    lazy_static! {
        static ref RE: Regex = canonical_base64("@", ".ed25519", 32);
    }
    &*RE
}

pub fn is_feed_id(string: &str) -> bool {
    let regex = feed_id_regex();
    regex.is_match(string)
}

pub fn parse_feed_id_data(id: &str) -> Result<Vec<u8>, DecodeError> {
    let base64_data = &id[1..id.len() - 8];
    b64.decode(base64_data)
}

pub fn blob_id_regex() -> &'static Regex {
    lazy_static! {
        static ref RE: Regex = canonical_base64("&", ".sha256", 32);
    }
    &*RE
}

pub fn is_blob_id(string: &str) -> bool {
    let regex = blob_id_regex();
    regex.is_match(string)
}

pub fn parse_blob_id_data(id: &str) -> Result<Vec<u8>, DecodeError> {
    let base64_data = &id[1..id.len() - 7];
    b64.decode(base64_data)
}

// https://github.com/dominictarr/is-canonical-base64/blob/master/index.js
fn canonical_base64(prefix: &str, suffix: &str, length: u32) -> Regex {
    let char = "[a-zA-Z0-9/+]";
    let trail2 = "[AQgw]==";
    let trail4 = "[AEIMQUYcgkosw048]=";

    let mut re = String::new();
    re.push_str("^");
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
    re.push_str("$");

    Regex::new(&re).unwrap()
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
