// https://github.com/ssbc/ssb-typescript

use serde_json::Value;
use std::convert::TryFrom;
use thiserror::Error as ThisError;

#[derive(ThisError)]
pub enum Error {
    #[error("{type} must start with {sigil}.")]
    IdMissingSigil {
        idType: &'static str,
        sigil: &'static str,
    },
    #[error("Missing {field} field in {contentType} content.")]
    MissingField {
        contentType: &'static str,
        field: &'static str,
    },
}

/**
 * Starts with @
 */
pub struct FeedId(String);

impl TryFrom<String> for FeedId {
    type Error = &'static str;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.starts_with('@') {
            Ok(FeedId(value))
        } else {
            Err(Error::IdMissingSigil {
                idType: "FeedId",
                sigil: "'@'",
            })
        }
    }
}

/**
 * Starts with %
 */
pub struct MsgId(String);

impl TryFrom<String> for MsgId {
    type Error = &'static str;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.starts_with('%') {
            Ok(MsgId(value))
        } else {
            Err(Error::IdMissingSigil {
                idType: "MsgId",
                sigil: "'%'",
            })
        }
    }
}

/**
 * Starts with &
 */
pub struct BlobId(String);

impl TryFrom<String> for BlobId {
    type Error = &'static str;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.starts_with('&') {
            Ok(BlobId(value))
        } else {
            Err(Error::IdMissingSigil {
                idType: "BlobId",
                sigil: "'&'",
            })
        }
    }
}

pub enum LinkId {
    Feed(FeedId),
    Msg(MsgId),
    Blob(BlobId),
}

impl TryFrom<String> for BlobId {
    type Error = &'static str;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.starts_with('@') {
            Ok(LinkId::Feed(FeedId(value)))
        } else if value.starts_with('%') {
            Ok(LinkId::Msg(MsgId(value)))
        } else if value.starts_with('&') {
            Ok(LinkId::Blob(BlobId(value)))
        } else {
            Err(Error::IdMissingSigil {
                idType: "LinkId",
                sigil: "either '@', '%', or '&'",
            })
        }
    }
}

pub struct Msg {
    key: MsgId,
    value: MsgValue,
    timestamp: i64,
}

pub struct MsgValue {
    previous: MsgId,
    author: FeedId,
    sequence: u64,
    timestamp: i64,
    // hash: &'static str,
    content: MsgContent,
    signature: String,
}

pub enum Content {
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
    Other,
}

impl TryFrom<Value> for Content {
    type Error = &'static str;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        let content_type = value
            .get("type")
            .and_then(|v| v.as_str())
            .ok_or("Missing 'type' field in SSB message content.")?;

        match content_type {
            "post" => Ok(Content::Post(PostContent::try_from(value)?)),
            /*
            "contact" => Ok(Content::Contact(ContactContent::try_from(value)?)),
            "vote" => Ok(Content::Vote(VoteContent::try_from(value)?)),
            "about" => Ok(Content::About(AboutContent::try_from(value)?)),
            "blog" => Ok(Content::Blog(BlogContent::try_from(value)?)),
            "room/alias" => Ok(Content::Alias(AliasContent::try_from(value)?)),
            "gathering" => Ok(Content::Gathering(GatheringContent::try_from(value)?)),
            "gathering-update" => Ok(Content::GatheringUpdate(GatheringUpdateContent::try_from(
                value,
            )?)),
            "attendee" => Ok(Content::Attendee(AttendeeContent::try_from(value)?)),
            */
            _ => Ok(Content::Other),
        }
    }
}

pub struct PostContent {
    text: String,
    channel: Option<String>,
    mentions: Option<Vec<Mention>>,
    root: Option<MsgId>,
    branch: Option<Vec<MsgId>>,
    fork: Option<MsgId>,
}

impl TryFrom<Value> for PostContent {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        let text = value.get("text").map(String::from);
        let channel = value.get("channel").map(String::from);
        let mentions = value.get("mentions").map(TryInto::try_into)?;
        let root = value.get("root").map(String::from).map(TryInto::into)?;
        let branch = value.get("branch").map(|v| {
            if v.is_array() {
                Some(v.as_array().unwrap().iter().map(|b| b.into()).collect())
            } else if v.is_string() {
                Some(vec![v.to_string().into()])
            } else {
                None
            }
        })?;
        let fork = value.get("fork").map(String::from).map(TryInto::into)?;

        Ok(PostContent {
            text,
            channel,
            mentions,
            root,
            branch,
            fork,
        })
    }
}

pub struct Mention {
    pub link: LinkId,
    pub name: Option<String>,
}

impl TryFrom<Value> for Mention {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        if value.is_array() {
            value
                .as_array()
                .unwrap()
                .iter()
                .map(|item| {
                    let link: LinkId =
                        item.get("link")
                            .map(TryFrom::into)
                            .ok_or(Err(Error::MissingField {
                                contentType: "Mention",
                                field: "link",
                            }));
                    let name = item.get("name").map(String::from);
                    Mention { link, name }
                })
                .collect()
        } else {
            let link: LinkId =
                item.get("link")
                    .map(TryFrom::into)
                    .ok_or(Err(Error::MissingField {
                        contentType: "Mention",
                        field: "link",
                    }));
            let name = item.get("name").map(String::from);
            Mention { link, name }
        }
    }
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
