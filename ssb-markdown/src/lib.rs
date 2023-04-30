use lazy_static::lazy_static;
use pulldown_cmark::{html, CowStr, Event, LinkType, Options, Parser, Tag};
use regex::{Captures, Regex};
use ssb_ref::{is_message_id, message_id_regex, parse_feed_id_data};
use std::{
    borrow::{Borrow, Cow},
    iter::Map,
};

pub fn parse(text: &str) -> impl Iterator<Item = Event> {
    let mut parser_opts = Options::empty();
    parser_opts.insert(Options::ENABLE_TABLES);
    parser_opts.insert(Options::ENABLE_STRIKETHROUGH);
    parser_opts.insert(Options::ENABLE_SMART_PUNCTUATION);
    let parser = Parser::new_ext(text, parser_opts).map(move |event| {
        match &event {
            Event::Start(tag) => {
                println!("Start: {:?}", tag);
                match tag {
                    Tag::Heading(heading_level, fragment_identifier, class_list) => println!(
                        "Heading heading_level: {} fragment identifier: {:?} classes: {:?}",
                        heading_level, fragment_identifier, class_list
                    ),
                    Tag::Paragraph => println!("Paragraph"),
                    Tag::List(ordered_list_first_item_number) => println!(
                        "List ordered_list_first_item_number: {:?}",
                        ordered_list_first_item_number
                    ),
                    Tag::Item => println!("Item (this is a list item)"),
                    Tag::Emphasis => println!("Emphasis (this is a span tag)"),
                    Tag::Strong => println!("Strong (this is a span tag)"),
                    Tag::Strikethrough => println!("Strikethrough (this is a span tag)"),
                    Tag::BlockQuote => println!("BlockQuote"),
                    Tag::CodeBlock(code_block_kind) => {
                        println!("CodeBlock code_block_kind: {:?}", code_block_kind)
                    }
                    Tag::Link(link_type, url, title) => println!(
                        "Link link_type: {:?} url: {} title: {}",
                        link_type, url, title
                    ),
                    Tag::Image(link_type, url, title) => println!(
                        "Image link_type: {:?} url: {} title: {}",
                        link_type, url, title
                    ),
                    Tag::Table(column_text_alignment_list) => println!(
                        "Table column_text_alignment_list: {:?}",
                        column_text_alignment_list
                    ),
                    Tag::TableHead => println!("TableHead (contains TableRow tags"),
                    Tag::TableRow => println!("TableRow (contains TableCell tags)"),
                    Tag::TableCell => println!("TableCell (contains inline tags)"),
                    Tag::FootnoteDefinition(label) => {
                        println!("FootnoteDefinition label: {}", label)
                    }
                }
            }
            Event::End(tag) => {
                println!("End: {:?}", tag);
            }
            Event::Html(s) => {
                println!("Html: {:?}", s);
            }
            Event::Text(s) => {
                println!("Text: {:?}", s)
            }
            Event::Code(s) => {
                println!("Code: {:?}", s);
            }
            Event::FootnoteReference(s) => {
                println!("FootnoteReference: {:?}", s)
            }
            Event::TaskListMarker(b) => println!("TaskListMarker: {:?}", b),
            Event::SoftBreak => println!("SoftBreak"),
            Event::HardBreak => println!("HardBreak"),
            Event::Rule => println!("Rule"),
        };
        event
    });
    parser
}

pub fn to_html(parser: Map<Parser, impl FnMut(Event) -> Event>) -> String {
    let mut html_buf = String::new();
    html::push_html(&mut html_buf, parser);
    html_buf
}

pub fn linkify(text: &str) -> impl Iterator<Item = Event> {
    let mut parents: Vec<Tag> = Vec::new();
    Parser::new(text).flat_map(move |event| match event {
        Event::Start(tag) => {
            println!("Start: {:?} (Parents: {:?})", tag, parents);
            parents.push(tag.clone());
            vec![Event::Start(tag)].into_iter()
        }
        Event::End(tag) => {
            println!("End: {:?} (Parents: {:?})", tag, parents);
            parents.pop();
            vec![Event::End(tag)].into_iter()
        }
        Event::Text(text) => {
            println!("Text: {:?}", text);
            println!("Parent: {:?}", parents.last());

            if let Some(Tag::Link(..)) = parents.last() {
                return vec![Event::Text(text)].into_iter();
            };

            let mut events: Vec<Event> = Vec::new();

            // message ids
            let mut last_match_end = 0;
            for mat in message_id_many_regex().find_iter(text.borrow()) {
                let range = mat.range();
                let match_start = range.start;

                // push previous text
                events.push(Event::Text(
                    text[last_match_end..match_start].to_string().into(),
                ));

                // push new link
                let link_tag =
                    Tag::Link(LinkType::Inline, mat.as_str().to_string().into(), "".into());
                events.push(Event::Start(link_tag.clone()));
                events.push(Event::Text(mat.as_str().to_string().into()));
                events.push(Event::End(link_tag.clone()));

                last_match_end = range.end;
            }
            // push last text
            if last_match_end < text.len() - 1 {
                events.push(Event::Text(
                    text[last_match_end..text.len()].to_string().into(),
                ));
            }

            events.into_iter()
        }
        _ => vec![event].into_iter(),
    })
}

pub fn message_id_many_regex() -> &'static Regex {
    lazy_static! {
        static ref OG_RE: &'static str = ssb_ref::message_id_regex().as_str();
        static ref RE: Regex = Regex::new(&OG_RE[1..OG_RE.len() - 1]).unwrap();
    }
    &*RE
}

#[cfg(test)]
mod tests {
    use super::*;
    use pulldown_cmark_to_cmark::cmark;

    #[test]
    fn test_message_ids_unlinked() {
        let text = r###"
* %SABuw7mOMKT5E8g6vp7ZZl8cqJfsIPPF44QpFE6p6sA=.sha256
* %huSc8wPvcd6CE6p5Zwo7/geQyK1i4AZE4zr/8Ov94xI=.sha256
"###;

        let mut actual = String::new();
        cmark(linkify(text), &mut actual).unwrap();

        let expected = r###"
* [%SABuw7mOMKT5E8g6vp7ZZl8cqJfsIPPF44QpFE6p6sA=.sha256](%SABuw7mOMKT5E8g6vp7ZZl8cqJfsIPPF44QpFE6p6sA=.sha256)
* [%huSc8wPvcd6CE6p5Zwo7/geQyK1i4AZE4zr/8Ov94xI=.sha256](%huSc8wPvcd6CE6p5Zwo7/geQyK1i4AZE4zr/8Ov94xI=.sha256)
"###.trim_start().trim_end();
        assert_eq!(actual, expected);
    }

    /*
        #[test]
        fn test_message_id_linked() {
            let text = r###"
    - ["TEST"](%SABuw7mOMKT5E8g6vp7ZZl8cqJfsIPPF44QpFE6p6sA=.sha256)
    - ["IT WORKS"](%huSc8wPvcd6CE6p5Zwo7/geQyK1i4AZE4zr/8Ov94xI=.sha256)
        "###;
            let (html, links) = render(text);
            println!("{}", html);
            assert_eq!(html, "".to_string());
            assert_eq!(links, vec![]);
        }
        */
}
