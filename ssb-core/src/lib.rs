// https://github.com/ssbc/ssb-typescript

use serde::{
    de::{self, MapAccess, Visitor},
    Deserialize, Deserializer, Serialize,
};
use serde_with::{serde_as, DefaultOnError, OneOrMany};
use std::{convert::TryFrom, fmt, str::FromStr};
use thiserror::Error as ThisError;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Msg<Content> {
    pub key: MsgKey,
    pub value: MsgValue<Content>,
    #[serde(alias = "timestamp")]
    pub timestamp_received: f64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MsgValue<Content> {
    // pub previous: MsgKey,
    pub author: FeedKey,
    pub sequence: u64,
    #[serde(alias = "timestamp")]
    pub timestamp_asserted: f64,
    // #[serde(default = "MsgValue::<Content>::default_hash")]
    // pub hash: String,
    pub content: Content,
    // pub signature: String,
}

impl<Content> MsgValue<Content> {
    pub fn default_hash() -> String {
        "sha256".to_string()
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type")]
pub enum MsgContent {
    #[serde(alias = "post")]
    Post(PostContent),
    #[serde(alias = "contact")]
    Contact(ContactContent),
    #[serde(alias = "vote")]
    Vote(VoteContent),
    #[serde(alias = "about")]
    About(AboutContent),
    /*
    Blog(BlogContent),
    Alias(AliasContent),
    Gathering(GatheringContent),
    GatheringUpdate(GatheringUpdateContent),
    Attendee(AttendeeContent),
    */
    #[serde(other)]
    Unknown,
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum Link {
    Feed {
        link: FeedKey,
        #[serde_as(deserialize_as = "DefaultOnError")]
        #[serde(default)]
        name: Option<String>,
    },
    Msg {
        link: MsgKey,
        #[serde_as(deserialize_as = "DefaultOnError")]
        #[serde(default)]
        name: Option<String>,
    },
    Blob(BlobLink),
    Hashtag {
        link: HashtagKey,
    },
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BlobLink {
    pub link: BlobKey,
    #[serde_as(deserialize_as = "DefaultOnError")]
    #[serde(default)]
    pub name: Option<String>,
    #[serde_as(deserialize_as = "DefaultOnError")]
    #[serde(default)]
    pub width: Option<u64>,
    #[serde_as(deserialize_as = "DefaultOnError")]
    #[serde(default)]
    pub height: Option<u64>,
    #[serde_as(deserialize_as = "DefaultOnError")]
    #[serde(default)]
    pub size: Option<u64>,
    #[serde_as(deserialize_as = "DefaultOnError")]
    #[serde(alias = "type")]
    #[serde(default)]
    pub mime_type: Option<String>,
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PostContent {
    pub text: String,
    #[serde_as(deserialize_as = "DefaultOnError")]
    #[serde(default)]
    pub channel: Option<String>,
    #[serde_as(as = "Option<DefaultOnError<OneOrMany<_>>>")]
    #[serde(default)]
    pub mentions: Option<Vec<Link>>,
    pub root: Option<MsgKey>,
    #[serde_as(as = "Option<DefaultOnError<OneOrMany<_>>>")]
    #[serde(default)]
    pub branch: Option<Vec<MsgKey>>,
    #[serde_as(deserialize_as = "DefaultOnError")]
    #[serde(default)]
    pub fork: Option<MsgKey>,
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ContactContent {
    pub contact: FeedKey,
    #[serde_as(deserialize_as = "DefaultOnError")]
    #[serde(default)]
    pub following: Option<bool>,
    #[serde_as(deserialize_as = "DefaultOnError")]
    #[serde(default)]
    pub blocking: Option<bool>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct VoteContent {
    pub vote: Vote,
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Vote {
    pub link: MsgKey,
    pub value: i32,
    #[serde_as(deserialize_as = "DefaultOnError")]
    #[serde(default)]
    pub expression: Option<String>,
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct AboutContent {
    pub about: LinkKey,
    #[serde_as(deserialize_as = "DefaultOnError")]
    #[serde(default)]
    pub name: Option<String>,
    #[serde_as(deserialize_as = "DefaultOnError")]
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    #[serde(deserialize_with = "deserialize_optional_blob_link")]
    pub image: Option<BlobLink>,
}

/*
pub struct BlogContent {
    title: String,
    summary: String,
    channel: Option<String>,
    thumbnail: Option<String>,
    blog: String,
    mentions: Option<Vec<any>>,
    root: Option<MsgKey>,
    branch: Option<Vec<MsgKey>>,
    fork: Option<MsgKey>,
}

pub struct AliasContent {
    action: Option<String>,
    alias: Option<String>,
    alias_url: Option<String>,
    room: Option<FeedKey>,
}

pub struct GatheringContent {
    progenitor: Option<MsgKey>,
    mentions: Option<Vec<FeedKey>>,
}

pub struct GatheringUpdateContent {
    about: MsgKey,
    title: Option<String>,
    description: Option<String>,
    location: Option<String>,
    start_date_time: Option<DateTime>,
    image: Option<BlobLink>,
}

pub struct DateTime {
    epoch: Option<i64>,
    tz: Option<String>,
    bias: Option<i32>,
    silent: Option<bool>,
}

pub struct AttendeeContent {
    about: MsgKey,
    attendee: Attendee,
}

pub struct Attendee {
    link: FeedKey,
    remove: Option<bool>,
}
*/

// https://serde.rs/string-or-struct.html
// https://users.rust-lang.org/t/solved-serde-deserialize-with-for-option-s/12749/2

impl FromStr for BlobLink {
    type Err = KeyError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(BlobLink {
            link: s.to_string().try_into()?,
            name: None,
            width: None,
            height: None,
            size: None,
            mime_type: None,
        })
    }
}

#[derive(Debug, Deserialize)]
struct WrappedBlobLink(#[serde(deserialize_with = "deserialize_blob_link")] BlobLink);

fn deserialize_optional_blob_link<'de, D>(deserializer: D) -> Result<Option<BlobLink>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<WrappedBlobLink>::deserialize(deserializer).map(
        |opt_wrapped: Option<WrappedBlobLink>| {
            opt_wrapped.map(|wrapped: WrappedBlobLink| wrapped.0)
        },
    )
}

fn deserialize_blob_link<'de, D>(deserializer: D) -> Result<BlobLink, D::Error>
where
    D: Deserializer<'de>,
{
    struct DeserializeBlobLink;

    impl<'de> Visitor<'de> for DeserializeBlobLink {
        type Value = BlobLink;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("string or map")
        }

        fn visit_str<E>(self, value: &str) -> Result<BlobLink, E>
        where
            E: de::Error,
        {
            let image = FromStr::from_str(value).map_err(|err| E::custom(format!("{}", err)))?;
            Ok(image)
        }

        fn visit_map<M>(self, map: M) -> Result<BlobLink, M::Error>
        where
            M: MapAccess<'de>,
        {
            Deserialize::deserialize(de::value::MapAccessDeserializer::new(map))
        }
    }

    deserializer.deserialize_any(DeserializeBlobLink)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
