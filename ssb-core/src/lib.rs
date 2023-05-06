// https://github.com/ssbc/ssb-typescript

use serde::{
    de::{self, MapAccess, Visitor},
    Deserialize, Deserializer, Serialize,
};
use serde_json::Value;
use serde_with::serde_as;
use std::{
    convert::{Infallible, TryFrom},
    fmt::{self, Display},
    marker::PhantomData,
    str::FromStr,
};
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
pub struct FeedId(pub String);

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

impl From<&FeedId> for String {
    fn from(value: &FeedId) -> String {
        value.0.clone()
    }
}

/**
 * Starts with %
 */
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(try_from = "String")]
pub struct MsgId(pub String);

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

impl From<&MsgId> for String {
    fn from(value: &MsgId) -> String {
        value.0.clone()
    }
}

/**
 * Starts with &
 */
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(try_from = "String")]
pub struct BlobId(pub String);

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

impl From<&BlobId> for String {
    fn from(value: &BlobId) -> String {
        value.0.clone()
    }
}

/**
 * Starts with #
 */
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(try_from = "String")]
pub struct HashtagId(pub String);

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

impl From<&HashtagId> for String {
    fn from(value: &HashtagId) -> String {
        value.0.clone()
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

impl From<&LinkId> for String {
    fn from(value: &LinkId) -> String {
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
    pub signature: String,
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
    Contact(ContactContent),
    Vote(VoteContent),
    About(AboutContent),
    /*
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

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ContactContent {
    pub contact: Option<FeedId>,
    pub following: Option<bool>,
    pub blocking: Option<bool>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct VoteContent {
    pub vote: Vote,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Vote {
    pub link: MsgId,
    pub value: i32,
    pub expression: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct AboutContent {
    pub about: LinkId,
    pub name: Option<String>,
    pub description: Option<String>,
    #[serde(deserialize_with = "deserialize_optional_image")]
    pub image: Option<Image>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Image {
    pub link: BlobId,
    pub name: Option<String>,
    pub size: Option<u64>,
    pub image_type: Option<String>,
}

impl FromStr for Image {
    type Err = IdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Image {
            link: s.to_string().try_into()?,
            name: None,
            size: None,
            image_type: None,
        })
    }
}

/*
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

pub struct AttendeeContent {
    about: MsgId,
    attendee: Attendee,
}

pub struct Attendee {
    link: FeedId,
    remove: Option<bool>,
}
*/

// https://serde.rs/string-or-struct.html
// https://users.rust-lang.org/t/solved-serde-deserialize-with-for-option-s/12749/2

#[derive(Debug, Deserialize)]
struct WrappedImage(#[serde(deserialize_with = "deserialize_image")] Image);

fn deserialize_optional_image<'de, D>(deserializer: D) -> Result<Option<Image>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<WrappedImage>::deserialize(deserializer)
        .map(|opt_wrapped: Option<WrappedImage>| opt_wrapped.map(|wrapped: WrappedImage| wrapped.0))
}

fn deserialize_image<'de, D>(deserializer: D) -> Result<Image, D::Error>
where
    D: Deserializer<'de>,
{
    struct DeserializeImage;

    impl<'de> Visitor<'de> for DeserializeImage {
        type Value = Image;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("string or map")
        }

        fn visit_str<E>(self, value: &str) -> Result<Image, E>
        where
            E: de::Error,
        {
            let image = FromStr::from_str(value).map_err(|err| E::custom(format!("{}", err)))?;
            Ok(image)
        }

        fn visit_map<M>(self, map: M) -> Result<Image, M::Error>
        where
            M: MapAccess<'de>,
        {
            Deserialize::deserialize(de::value::MapAccessDeserializer::new(map))
        }
    }

    deserializer.deserialize_any(DeserializeImage)
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
