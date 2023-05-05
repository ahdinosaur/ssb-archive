// https://github.com/ssbc/ssb-typescript

use std::convert::TryFrom;

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
            Err("FeedId must start with '@'")
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
            Err("MsgId must start with '%'")
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
            Err("BlobId must start with '&'")
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
    Contact(ContactContent),
    Vote(VoteContent),
    About(AboutContent),
    Blog(BlogContent),
    Alias(AliasContent),
    Gathering(GatheringContent),
    GatheringUpdate(GatheringUpdateContent),
    Attendee(AttendeeContent),
    Other,
}

pub struct PostContent {
    text: String,
    channel: Option<String>,
    mentions: Option<Vec<any>>,
    root: Option<MsgId>,
    branch: Option<Vec<MsgId>>,
    fork: Option<MsgId>,
}

pub struct AboutContent {
    about: FeedId,
    name: Option<String>,
    description: Option<String>,
    image: Option<String>,
}

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
    type_: Option<String>,
}

pub struct AttendeeContent {
    about: MsgId,
    attendee: Attendee,
}

pub struct Attendee {
    link: FeedId,
    remove: Option<bool>,
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
