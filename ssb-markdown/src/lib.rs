use pulldown_cmark::{html, CowStr, Event, LinkType, Options, Parser, Tag};
use ssb_ref::{
    blob_id_data_urlsafe, feed_id_data_urlsafe, is_blob_id, is_feed_id, is_link_id, is_message_id,
    link_id_multi_regex, message_id_data_urlsafe,
};
use std::{borrow::Borrow, cell::RefCell, sync::Arc};

pub fn render(text: &str) -> (String, Vec<String>) {
    let mut parser_opts = Options::empty();
    parser_opts.insert(Options::ENABLE_TABLES);
    parser_opts.insert(Options::ENABLE_STRIKETHROUGH);
    parser_opts.insert(Options::ENABLE_SMART_PUNCTUATION);

    let events = Parser::new_ext(text, parser_opts);
    let events_2 = linkify(events);

    let (events_3, links) = render_links(events_2);
    let html = to_html(events_3.into_iter());

    (html, (*links).clone().into_inner())
}

pub fn to_html<'a>(events: impl Iterator<Item = Event<'a>>) -> String {
    let mut html_buf = String::new();
    html::push_html(&mut html_buf, events);
    html_buf
}

pub fn render_links<'a>(
    events: impl Iterator<Item = Event<'a>>,
) -> (impl Iterator<Item = Event<'a>>, Arc<RefCell<Vec<String>>>) {
    let links = Arc::new(RefCell::new(Vec::<String>::new()));
    let links_ret = links.clone();

    let next_events = events.map(move |event| match &event {
        Event::Start(tag) => match tag {
            Tag::Link(link_type, url, title) => {
                if !is_link_id(url) {
                    return event;
                }

                links.borrow_mut().push(url.to_string());

                let next_url = render_link_url(url);
                Event::Start(Tag::Link(*link_type, next_url.into(), title.clone()))
            }
            Tag::Image(link_type, url, title) => {
                if !is_link_id(url) {
                    return event;
                }

                links.borrow_mut().push(url.to_string());

                let next_url = render_link_url(url);
                Event::Start(Tag::Link(*link_type, next_url.into(), title.clone()))
            }
            _ => event,
        },
        Event::End(tag) => match tag {
            Tag::Link(link_type, url, title) => {
                if !is_link_id(url) {
                    return event;
                }

                let next_url = render_link_url(url);
                Event::End(Tag::Link(*link_type, next_url.into(), title.clone()))
            }
            Tag::Image(link_type, url, title) => {
                if !is_link_id(url) {
                    return event;
                }
                let next_url = render_link_url(url);
                Event::End(Tag::Image(*link_type, next_url.into(), title.clone()))
            }
            _ => event,
        },
        _ => event,
    });

    (next_events, links_ret)
}

fn render_link_url(url: &str) -> String {
    if is_message_id(url) {
        format!("/message/{}", message_id_data_urlsafe(url))
    } else if is_feed_id(url) {
        format!("/feed/{}", feed_id_data_urlsafe(url))
    } else if is_blob_id(url) {
        format!("/blob/{}", blob_id_data_urlsafe(url))
    } else {
        url.to_string()
    }
}

pub fn linkify<'a>(events: impl Iterator<Item = Event<'a>>) -> impl Iterator<Item = Event<'a>> {
    let mut parents: Vec<Tag> = Vec::new();
    events.flat_map(move |event| match event {
        Event::Start(tag) => {
            // println!("Start: {:?} (Parents: {:?})", tag, parents);
            parents.push(tag.clone());
            vec![Event::Start(tag)].into_iter()
        }
        Event::End(tag) => {
            // println!("End: {:?} (Parents: {:?})", tag, parents);
            parents.pop();
            vec![Event::End(tag)].into_iter()
        }
        Event::Text(text) => {
            // println!("Text: {:?}", text);
            // println!("Parent: {:?}", parents.last());

            if let Some(Tag::Link(..)) = parents.last() {
                return vec![Event::Text(text)].into_iter();
            };

            // message ids
            let mut events: Vec<Event> = Vec::new();
            events.append(&mut linkify_text(text.clone()));
            events.into_iter()
        }
        _ => vec![event].into_iter(),
    })
}

fn linkify_text<'a>(text: CowStr<'a>) -> Vec<Event<'a>> {
    let mut events: Vec<Event> = Vec::new();

    // message ids
    let mut last_match_end = 0;
    for mat in link_id_multi_regex().find_iter(text.borrow()) {
        let range = mat.range();
        let match_start = range.start;

        // push previous text
        events.push(Event::Text(
            text[last_match_end..match_start].to_string().into(),
        ));

        // push new link
        let link_tag = Tag::Link(LinkType::Inline, mat.as_str().to_string().into(), "".into());
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

    events
}

#[cfg(test)]
mod tests {
    use super::*;
    use pulldown_cmark_to_cmark::cmark;

    #[test]
    fn test_linkify_message_ids_unlinked() {
        let text = r###"
* %SABuw7mOMKT5E8g6vp7ZZl8cqJfsIPPF44QpFE6p6sA=.sha256
* %huSc8wPvcd6CE6p5Zwo7/geQyK1i4AZE4zr/8Ov94xI=.sha256
"###;

        let mut actual = String::new();
        cmark(linkify(Parser::new(text)), &mut actual).unwrap();

        let expected = r###"
* [%SABuw7mOMKT5E8g6vp7ZZl8cqJfsIPPF44QpFE6p6sA=.sha256](%SABuw7mOMKT5E8g6vp7ZZl8cqJfsIPPF44QpFE6p6sA=.sha256)
* [%huSc8wPvcd6CE6p5Zwo7/geQyK1i4AZE4zr/8Ov94xI=.sha256](%huSc8wPvcd6CE6p5Zwo7/geQyK1i4AZE4zr/8Ov94xI=.sha256)
"###.trim();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_linkify_message_id_linked_with_id_label() {
        let text = r###"
* [%SABuw7mOMKT5E8g6vp7ZZl8cqJfsIPPF44QpFE6p6sA=.sha256](%SABuw7mOMKT5E8g6vp7ZZl8cqJfsIPPF44QpFE6p6sA=.sha256)
* [%huSc8wPvcd6CE6p5Zwo7/geQyK1i4AZE4zr/8Ov94xI=.sha256](%huSc8wPvcd6CE6p5Zwo7/geQyK1i4AZE4zr/8Ov94xI=.sha256)
        "###
        .trim();

        let mut actual = String::new();
        cmark(linkify(Parser::new(text)), &mut actual).unwrap();

        assert_eq!(actual, text);
    }

    #[test]
    fn test_linkify_message_id_linked_with_name_label() {
        let text = r###"
* ["TEST"](%SABuw7mOMKT5E8g6vp7ZZl8cqJfsIPPF44QpFE6p6sA=.sha256)
* ["IT WORKS"](%huSc8wPvcd6CE6p5Zwo7/geQyK1i4AZE4zr/8Ov94xI=.sha256)
        "###
        .trim();

        let mut actual = String::new();
        cmark(linkify(Parser::new(text)), &mut actual).unwrap();

        assert_eq!(actual, text);
    }

    #[test]
    fn test_render_links() {
        let text = r###"
- ["TEST"](%SABuw7mOMKT5E8g6vp7ZZl8cqJfsIPPF44QpFE6p6sA=.sha256)
- ["IT WORKS"](%huSc8wPvcd6CE6p5Zwo7/geQyK1i4AZE4zr/8Ov94xI=.sha256)
        "###
        .trim();

        let expected_html = r###"
<ul>
<li><a href="/message/SABuw7mOMKT5E8g6vp7ZZl8cqJfsIPPF44QpFE6p6sA">“TEST”</a></li>
<li><a href="/message/huSc8wPvcd6CE6p5Zwo7_geQyK1i4AZE4zr_8Ov94xI">“IT WORKS”</a></li>
</ul>
"###
        .trim_start();
        let expected_links: Vec<String> = vec![
            "%SABuw7mOMKT5E8g6vp7ZZl8cqJfsIPPF44QpFE6p6sA=.sha256".into(),
            "%huSc8wPvcd6CE6p5Zwo7/geQyK1i4AZE4zr/8Ov94xI=.sha256".into(),
        ];
        let (actual_html, actual_links) = render(text);
        assert_eq!(actual_html, expected_html);
        assert_eq!(actual_links, expected_links);
    }
}
