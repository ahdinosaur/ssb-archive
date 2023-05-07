use pulldown_cmark::{html, CowStr, Event, LinkType, Options, Parser, Tag};
use ssb_ref::LinkRef;
use std::borrow::Borrow;

/*
struct Link<'a> {
    url: &'a str,
    label: &'a str,
}
*/

pub fn render(text: &str) -> String {
    let mut parser_opts = Options::empty();
    parser_opts.insert(Options::ENABLE_TABLES);
    parser_opts.insert(Options::ENABLE_STRIKETHROUGH);
    parser_opts.insert(Options::ENABLE_SMART_PUNCTUATION);

    let events = Parser::new_ext(text, parser_opts);
    let events_2 = linkify(events);

    let events_3 = render_links(events_2);
    let html = to_html(events_3.into_iter());

    html
}

pub fn to_html<'a>(events: impl Iterator<Item = Event<'a>>) -> String {
    let mut html_buf = String::new();
    html::push_html(&mut html_buf, events);
    html_buf
}

fn render_links<'a>(events: impl Iterator<Item = Event<'a>>) -> impl Iterator<Item = Event<'a>> {
    events.map(move |event| match &event {
        Event::Start(tag) => match tag {
            Tag::Link(link_type, url, title) => {
                if !LinkRef::is_match(url) {
                    return event;
                }
                let next_url = render_link_url(url);
                Event::Start(Tag::Link(*link_type, next_url.into(), title.clone()))
            }
            Tag::Image(link_type, url, title) => {
                if !LinkRef::is_match(url) {
                    return event;
                }
                let next_url = render_link_url(url);
                Event::Start(Tag::Link(*link_type, next_url.into(), title.clone()))
            }
            _ => event,
        },
        Event::End(tag) => match tag {
            Tag::Link(link_type, url, title) => {
                if !LinkRef::is_match(url) {
                    return event;
                }
                let next_url = render_link_url(url);
                Event::End(Tag::Link(*link_type, next_url.into(), title.clone()))
            }
            Tag::Image(link_type, url, title) => {
                if !LinkRef::is_match(url) {
                    return event;
                }
                let next_url = render_link_url(url);
                Event::End(Tag::Image(*link_type, next_url.into(), title.clone()))
            }
            _ => event,
        },
        _ => event,
    })
}

fn render_link_url(url: &str) -> String {
    LinkRef::from_string(url.to_string()).unwrap().to_page_url()
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
    for mat in LinkRef::multi_regex().find_iter(text.borrow()) {
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

pub fn collect_links(text: &str) -> Vec<String> {
    let events = linkify(Parser::new(text));

    let mut links = Vec::<String>::new();

    for event in events {
        match &event {
            Event::Start(tag) => match tag {
                Tag::Link(_link_type, url, _title) => {
                    if LinkRef::is_match(url) {
                        links.push(url.to_string());
                    }
                }
                Tag::Image(_link_type, url, _title) => {
                    if LinkRef::is_match(url) {
                        links.push(url.to_string());
                    }
                }
                _ => {}
            },
            _ => {}
        }
    }

    links
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
        let actual_html = render(text);
        assert_eq!(actual_html, expected_html);
    }

    #[test]
    fn test_collect_links() {
        let text = r###"
- %SABuw7mOMKT5E8g6vp7ZZl8cqJfsIPPF44QpFE6p6sA=.sha256
- ["IT WORKS"](%huSc8wPvcd6CE6p5Zwo7/geQyK1i4AZE4zr/8Ov94xI=.sha256)
        "###;

        let expected_links: Vec<String> = vec![
            "%SABuw7mOMKT5E8g6vp7ZZl8cqJfsIPPF44QpFE6p6sA=.sha256".into(),
            "%huSc8wPvcd6CE6p5Zwo7/geQyK1i4AZE4zr/8Ov94xI=.sha256".into(),
        ];
        let actual_links = collect_links(text);
        assert_eq!(actual_links, expected_links);
    }
}
