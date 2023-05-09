use std::collections::HashMap;

use ssb_msg::{Link, PostContent};
use ssb_ref::{BlobRef, FeedRef, MsgRef};

pub struct Post {
    pub post_msg_ref: MsgRef,
    pub author_feed_ref: FeedRef,
    pub content: PostContent,
    pub reactions: Vec<Reaction>,
    pub fork_msg_refs: Vec<MsgRef>,
    pub mentions: Vec<Link>,
    pub back_mention_msg_refs: Vec<MsgRef>,
}

pub struct Reaction {
    pub feed_ref: FeedRef,
    pub expression: String,
}

pub struct Author {
    pub feed_ref: FeedRef,
    pub name: String,
    pub image: BlobRef,
    pub description: String,
}

pub struct Thread {
    pub root_msg_ref: MsgRef,
    pub reply_msg_refs: Vec<MsgRef>,
}

pub struct ThreadBundle {
    pub thread: Thread,
    pub authors: HashMap<FeedRef, Author>,
    pub posts: HashMap<MsgRef, Post>,
}
