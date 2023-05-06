use axohtml::{dom::DOMTree, html, unsafe_text};
use ssb_markdown::render;
use ssb_query::Msg;

pub enum PageError {
    BadContent,
}

pub fn render_post(message: Msg) -> Result<DOMTree<String>, PageError> {
    let value = message.value;
    let content = value.content;
    let content_type = content["type"].as_str().ok_or(PageError::BadContent)?;
    assert_eq!(content_type, "post");
    let content_text = content["text"].as_str().ok_or(PageError::BadContent)?;
    let content_root = content["root"].as_str().ok_or(PageError::BadContent)?;
    let content_fork = content["fork"].as_str().ok_or(PageError::BadContent)?;

    let content_html = render(content_text);

    let post_html = html!(
        <div id=message.key.as_str() class=content_type>
            <header>
            </header>
            <article class="content">
                {unsafe_text!(content_html)}
            </article>
        </div>
    );

    Ok(post_html)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {}
}
