// https://github.com/ssbc/ssb-typescript

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;
use serde_with::serde_as;
use std::{convert::TryFrom, marker::PhantomData};
use thiserror::Error as ThisError;

#[derive(Copy, Clone, Debug, ThisError)]
pub enum IdError {
    #[error("{id_type} must start with {sigil}.")]
    MissingSigil {
        id_type: &'static str,
        sigil: &'static str,
    },
}

/*
#[error("Missing {field} field in {contentType} content.")]
MissingField {
    contentType: &'static str,
    field: &'static str,
},
*/

/**
 * Starts with @
 */
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(try_from = "String")]
pub struct FeedId(String);

impl TryFrom<String> for FeedId {
    type Error = IdError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.starts_with('@') {
            Ok(FeedId(value))
        } else {
            Err(IdError::MissingSigil {
                id_type: "FeedId",
                sigil: "'@'",
            })
        }
    }
}

impl From<FeedId> for String {
    fn from(value: FeedId) -> String {
        value.0
    }
}

/**
 * Starts with %
 */
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(try_from = "String")]
pub struct MsgId(String);

impl TryFrom<String> for MsgId {
    type Error = IdError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.starts_with('%') {
            Ok(MsgId(value))
        } else {
            Err(IdError::MissingSigil {
                id_type: "MsgId",
                sigil: "'%'",
            })
        }
    }
}

impl From<MsgId> for String {
    fn from(value: MsgId) -> String {
        value.0
    }
}

/**
 * Starts with &
 */
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(try_from = "String")]
pub struct BlobId(String);

impl TryFrom<String> for BlobId {
    type Error = IdError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.starts_with('&') {
            Ok(BlobId(value))
        } else {
            Err(IdError::MissingSigil {
                id_type: "BlobId",
                sigil: "'&'",
            })
        }
    }
}

impl From<BlobId> for String {
    fn from(value: BlobId) -> String {
        value.0
    }
}

/**
 * Starts with #
 */
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(try_from = "String")]
pub struct HashtagId(String);

impl TryFrom<String> for HashtagId {
    type Error = IdError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.starts_with('#') {
            Ok(HashtagId(value))
        } else {
            Err(IdError::MissingSigil {
                id_type: "HashtagId",
                sigil: "'#'",
            })
        }
    }
}

impl From<HashtagId> for String {
    fn from(value: HashtagId) -> String {
        value.0
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(try_from = "String")]
pub enum LinkId {
    Feed(FeedId),
    Msg(MsgId),
    Blob(BlobId),
    Hashtag(HashtagId),
}

impl TryFrom<String> for LinkId {
    type Error = IdError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.starts_with('@') {
            Ok(LinkId::Feed(FeedId(value)))
        } else if value.starts_with('%') {
            Ok(LinkId::Msg(MsgId(value)))
        } else if value.starts_with('&') {
            Ok(LinkId::Blob(BlobId(value)))
        } else if value.starts_with('#') {
            Ok(LinkId::Hashtag(HashtagId(value)))
        } else {
            Err(IdError::MissingSigil {
                id_type: "LinkId",
                sigil: "either '@', '%', '&', or '#'",
            })
        }
    }
}

impl From<LinkId> for String {
    fn from(value: LinkId) -> String {
        match value {
            LinkId::Feed(id) => id.into(),
            LinkId::Msg(id) => id.into(),
            LinkId::Blob(id) => id.into(),
            LinkId::Hashtag(id) => id.into(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Msg {
    pub key: MsgId,
    pub value: MsgValue,
    pub timestamp_received: i64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MsgValue {
    pub previous: MsgId,
    pub author: FeedId,
    pub sequence: u64,
    pub timestamp_asserted: i64,
    #[serde(default = "MsgValue::default_hash")]
    pub hash: String,
    pub content: MsgContent,
    // pub signature: String,
}

impl MsgValue {
    pub fn default_hash() -> String {
        "sha256".to_string()
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum MsgContent {
    Typed(MsgContentTyped),
    Unknown(Value),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type")]
pub enum MsgContentTyped {
    Post(PostContent),
    /*
    Contact(ContactContent),
    Vote(VoteContent),
    About(AboutContent),
    Blog(BlogContent),
    Alias(AliasContent),
    Gathering(GatheringContent),
    GatheringUpdate(GatheringUpdateContent),
    Attendee(AttendeeContent),
    */
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PostContent {
    pub text: String,
    pub channel: Option<String>,
    pub mentions: Vec<Mention>,
    pub root: Option<MsgId>,
    #[serde_as(as = "serde_with::OneOrMany<_>")]
    pub branch: Vec<MsgId>,
    pub fork: Option<MsgId>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Mention {
    pub link: LinkId,
    pub name: Option<String>,
}

/*
pub struct ContactContent {
    contact: Option<FeedId>,
    following: Option<bool>,
    blocking: Option<bool>,
}

pub struct VoteContent {
    vote: Vote,
}

pub struct Vote {
    link: MsgId,
    value: i32,
    expression: String,
}

pub struct AboutContent {
    about: FeedId,
    name: Option<String>,
    description: Option<String>,
    image: Option<Image>,
}

pub struct BlogContent {
    title: String,
    summary: String,
    channel: Option<String>,
    thumbnail: Option<String>,
    blog: String,
    mentions: Option<Vec<any>>,
    root: Option<MsgId>,
    branch: Option<Vec<MsgId>>,
    fork: Option<MsgId>,
}

pub struct AliasContent {
    action: Option<String>,
    alias: Option<String>,
    alias_url: Option<String>,
    room: Option<FeedId>,
}

pub struct GatheringContent {
    progenitor: Option<MsgId>,
    mentions: Option<Vec<FeedId>>,
}

pub struct GatheringUpdateContent {
    about: MsgId,
    title: Option<String>,
    description: Option<String>,
    location: Option<String>,
    start_date_time: Option<DateTime>,
    image: Option<Image>,
}

pub struct DateTime {
    epoch: Option<i64>,
    tz: Option<String>,
    bias: Option<i32>,
    silent: Option<bool>,
}

pub struct Image {
    link: BlobId,
    name: Option<String>,
    size: Option<u64>,
    type: Option<String>,
}

pub struct AttendeeContent {
    about: MsgId,
    attendee: Attendee,
}

pub struct Attendee {
    link: FeedId,
    remove: Option<bool>,
}
*/

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
