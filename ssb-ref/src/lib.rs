use base64::{
    engine::{
        general_purpose::STANDARD as b64, general_purpose::URL_SAFE_NO_PAD as b64url, Engine,
    },
    DecodeError,
};
use hashtag_regex::HASHTAG_RE_STRING;
use lazy_static::lazy_static;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use thiserror::Error as ThisError;
use urlencoding::encode;

#[derive(Clone, Debug, ThisError)]
pub enum RefError {
    #[error("Does not match as {ref_type}: {input}")]
    BadFormat {
        ref_type: &'static str,
        input: String,
    },
    #[error("Failed to decode base64: {0}")]
    DecodeError(#[from] DecodeError),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(try_from = "String")]
pub struct FeedRef(Vec<u8>);

impl FeedRef {
    // From string that starts with @
    pub fn from_string(string: String) -> Result<Self, RefError> {
        if !Self::is_match(string.as_str()) {
            Err(RefError::BadFormat {
                ref_type: "Feed",
                input: string,
            })
        } else {
            Ok(Self(Self::parse_data(string.as_str())?))
        }
    }

    pub fn to_string(&self) -> String {
        format!("@{}.ed25519", self.string_data())
    }

    pub fn single_regex() -> &'static Regex {
        lazy_static! {
            static ref RE: Regex = canonical_base64("@", ".ed25519", 32, true);
        }
        &*RE
    }

    pub fn multi_regex() -> &'static Regex {
        lazy_static! {
            static ref RE: Regex = canonical_base64("@", ".ed25519", 32, false);
        }
        &*RE
    }

    pub fn is_match(string: &str) -> bool {
        let regex = Self::single_regex();
        regex.is_match(string)
    }

    pub fn to_page_url(&self) -> String {
        format!("/feed/{}", self.urlsafe_data())
    }

    fn string_data(&self) -> String {
        b64.encode(self.0.clone())
    }

    fn urlsafe_data(&self) -> String {
        b64url.encode(self.0.clone())
    }

    fn parse_data(key: &str) -> Result<Vec<u8>, RefError> {
        let base64_data = &key[1..key.len() - 8];
        Ok(b64.decode(base64_data)?)
    }
}

impl TryFrom<String> for FeedRef {
    type Error = RefError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        FeedRef::from_string(value)
    }
}

impl From<&FeedRef> for String {
    fn from(value: &FeedRef) -> String {
        value.to_string()
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(try_from = "String")]
pub struct MsgRef(Vec<u8>);

impl MsgRef {
    // From string that starts with %
    pub fn from_string(string: String) -> Result<Self, RefError> {
        if !Self::is_match(string.as_str()) {
            Err(RefError::BadFormat {
                ref_type: "Msg",
                input: string,
            })
        } else {
            Ok(Self(Self::parse_data(string.as_str())?))
        }
    }

    pub fn to_string(&self) -> String {
        format!("%{}.sha256", self.string_data())
    }

    pub fn single_regex() -> &'static Regex {
        lazy_static! {
            static ref RE: Regex = canonical_base64("%", ".sha256", 32, true);
        }
        &*RE
    }

    pub fn multi_regex() -> &'static Regex {
        lazy_static! {
            static ref RE: Regex = canonical_base64("%", ".sha256", 32, false);
        }
        &*RE
    }

    pub fn is_match(string: &str) -> bool {
        let regex = Self::single_regex();
        regex.is_match(string)
    }

    pub fn to_page_url(&self) -> String {
        format!("/message/{}", self.urlsafe_data())
    }

    fn string_data(&self) -> String {
        b64.encode(self.0.clone())
    }

    fn urlsafe_data(&self) -> String {
        b64url.encode(self.0.clone())
    }

    fn parse_data(key: &str) -> Result<Vec<u8>, RefError> {
        let base64_data = &key[1..key.len() - 7];
        Ok(b64.decode(base64_data)?)
    }
}

impl TryFrom<String> for MsgRef {
    type Error = RefError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        MsgRef::from_string(value)
    }
}

impl From<&MsgRef> for String {
    fn from(value: &MsgRef) -> String {
        value.to_string()
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(try_from = "String")]
pub struct BlobRef(Vec<u8>);

impl BlobRef {
    // From string that starts with &
    pub fn from_string(string: String) -> Result<Self, RefError> {
        if !Self::is_match(string.as_str()) {
            Err(RefError::BadFormat {
                ref_type: "Blob",
                input: string,
            })
        } else {
            Ok(Self(Self::parse_data(string.as_str())?))
        }
    }

    pub fn to_string(&self) -> String {
        format!("&{}.sha256", self.string_data())
    }

    pub fn single_regex() -> &'static Regex {
        lazy_static! {
            static ref RE: Regex = canonical_base64("&", ".sha256", 32, true);
        }
        &*RE
    }

    pub fn multi_regex() -> &'static Regex {
        lazy_static! {
            static ref RE: Regex = canonical_base64("&", ".sha256", 32, false);
        }
        &*RE
    }

    pub fn is_match(string: &str) -> bool {
        let regex = Self::single_regex();
        regex.is_match(string)
    }

    pub fn to_page_url(&self) -> String {
        format!("/blob/{}", self.urlsafe_data())
    }

    fn string_data(&self) -> String {
        b64.encode(self.0.clone())
    }

    pub fn urlsafe_data(&self) -> String {
        b64url.encode(self.0.clone())
    }

    fn parse_data(key: &str) -> Result<Vec<u8>, RefError> {
        let base64_data = &key[1..key.len() - 7];
        Ok(b64.decode(base64_data)?)
    }
}

impl TryFrom<String> for BlobRef {
    type Error = RefError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        BlobRef::from_string(value)
    }
}

impl From<&BlobRef> for String {
    fn from(value: &BlobRef) -> String {
        value.to_string()
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(try_from = "String")]
pub struct HashtagRef(String);

impl HashtagRef {
    // From string that starts with &
    pub fn from_string(string: String) -> Result<Self, RefError> {
        if !Self::is_match(string.as_str()) {
            Err(RefError::BadFormat {
                ref_type: "Hashtag",
                input: string,
            })
        } else {
            Ok(Self(string))
        }
    }

    pub fn to_string(&self) -> String {
        let mut string = String::new();
        string.push_str("#");
        string.push_str(self.0.as_str());
        string
    }

    pub fn single_regex() -> &'static Regex {
        lazy_static! {
            static ref RE: Regex =
                Regex::new(format!("^{}$", HASHTAG_RE_STRING.to_string()).as_str()).unwrap();
        }
        &*RE
    }

    pub fn multi_regex() -> &'static Regex {
        lazy_static! {
            static ref RE: Regex = Regex::new(&HASHTAG_RE_STRING).unwrap();
        }
        &*RE
    }

    pub fn is_match(string: &str) -> bool {
        let regex = Self::single_regex();
        regex.is_match(string)
    }

    pub fn to_page_url(&self) -> String {
        let tag = self.parse_tag();
        let urlsafe_tag = encode(tag.as_str());
        format!("/hashtag/{}", urlsafe_tag)
    }

    fn parse_tag(&self) -> String {
        let regex = Self::single_regex();
        let caps = regex.captures(self.0.as_str()).unwrap();
        caps.name("tag").unwrap().as_str().to_string()
    }
}

impl TryFrom<String> for HashtagRef {
    type Error = RefError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        HashtagRef::from_string(value)
    }
}

impl From<&HashtagRef> for String {
    fn from(value: &HashtagRef) -> String {
        value.to_string()
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(try_from = "String")]
pub enum LinkRef {
    Feed(FeedRef),
    Msg(MsgRef),
    Blob(BlobRef),
    Hashtag(HashtagRef),
}

impl LinkRef {
    pub fn from_string(value: String) -> Result<Self, RefError> {
        let v = value.as_str();
        if FeedRef::is_match(v) {
            Ok(LinkRef::Feed(value.try_into()?))
        } else if MsgRef::is_match(v) {
            Ok(LinkRef::Msg(value.try_into()?))
        } else if BlobRef::is_match(v) {
            Ok(LinkRef::Blob(value.try_into()?))
        } else if HashtagRef::is_match(v) {
            Ok(LinkRef::Hashtag(value.try_into()?))
        } else {
            Err(RefError::BadFormat {
                ref_type: "Link",
                input: value,
            })
        }
    }

    pub fn to_string(&self) -> String {
        match self {
            LinkRef::Feed(id) => id.into(),
            LinkRef::Msg(id) => id.into(),
            LinkRef::Blob(id) => id.into(),
            LinkRef::Hashtag(id) => id.into(),
        }
    }

    pub fn single_regex() -> &'static Regex {
        lazy_static! {
            static ref RE: Regex = combine_regexes(vec![
                MsgRef::single_regex(),
                FeedRef::single_regex(),
                BlobRef::single_regex(),
                HashtagRef::single_regex(),
            ]);
        }
        &*RE
    }

    pub fn multi_regex() -> &'static Regex {
        lazy_static! {
            static ref RE: Regex = combine_regexes(vec![
                MsgRef::multi_regex(),
                FeedRef::multi_regex(),
                BlobRef::multi_regex(),
                HashtagRef::multi_regex(),
            ]);
        }
        &*RE
    }

    pub fn is_match(string: &str) -> bool {
        let regex = Self::single_regex();
        regex.is_match(string)
    }

    pub fn to_page_url(&self) -> String {
        match self {
            LinkRef::Feed(feed_ref) => feed_ref.to_page_url(),
            LinkRef::Msg(msg_ref) => msg_ref.to_page_url(),
            LinkRef::Blob(blob_ref) => blob_ref.to_page_url(),
            LinkRef::Hashtag(hashtag_ref) => hashtag_ref.to_page_url(),
        }
    }
}

impl TryFrom<String> for LinkRef {
    type Error = RefError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        LinkRef::from_string(value)
    }
}

impl From<&LinkRef> for String {
    fn from(value: &LinkRef) -> String {
        value.to_string()
    }
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
        assert!(MsgRef::is_match(
            "%pGzeEydYdHjKW1iIchR0Yumydsr3QSp8+FuYcwVwi8Q=.sha256"
        ));
        assert!(MsgRef::is_match(
            "%09abcdefghyq9KH6dYMc/g17L04jDbl1py8arGQmL1I=.sha256"
        ));
    }

    #[test]
    fn test_parse_message_id_data() {
        assert_eq!(
            MsgRef::parse_data("%pGzeEydYdHjKW1iIchR0Yumydsr3QSp8+FuYcwVwi8Q=.sha256").unwrap(),
            b64.decode("pGzeEydYdHjKW1iIchR0Yumydsr3QSp8+FuYcwVwi8Q=")
                .unwrap()
        );
    }

    #[test]
    fn test_is_feed_id() {
        assert!(FeedRef::is_match(
            "@jEA8WSl0URsB/g/XYG5zCGBkMOyTeBZfGtbw3RJMIuk=.ed25519"
        ));
    }

    #[test]
    fn test_parse_feed_id_data() {
        assert_eq!(
            FeedRef::parse_data("@jEA8WSl0URsB/g/XYG5zCGBkMOyTeBZfGtbw3RJMIuk=.ed25519").unwrap(),
            b64.decode("jEA8WSl0URsB/g/XYG5zCGBkMOyTeBZfGtbw3RJMIuk=")
                .unwrap()
        );
    }

    #[test]
    fn test_is_blob_id() {
        assert!(BlobRef::is_match(
            "&abcdefg6bIh5dmyss7QH7uMrQxz3LKvgjer68we30aQ=.sha256"
        ));
        assert!(BlobRef::is_match(
            "&51ZXxNYIvTDCoNTE9R94NiEg3JAZAxWtKn4h4SmBwyY=.sha256"
        ));
    }

    #[test]
    fn test_parse_blob_id_data() {
        assert_eq!(
            BlobRef::parse_data("&abcdefg6bIh5dmyss7QH7uMrQxz3LKvgjer68we30aQ=.sha256").unwrap(),
            b64.decode("abcdefg6bIh5dmyss7QH7uMrQxz3LKvgjer68we30aQ=")
                .unwrap()
        );
    }
}
