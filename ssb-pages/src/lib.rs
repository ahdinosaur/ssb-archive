use axohtml::{dom::DOMTree, html, unsafe_text};
use serde_json::Value;
use ssb_core::{Msg, PostContent};
use ssb_markdown::render;

pub enum PageError {}

pub fn render_post(msg: Msg<Value>, content: PostContent) -> Result<DOMTree<String>, PageError> {
    let msg_key = Into::<String>::into(&msg.key);
    let content_html = render(content.text.as_str());

    let post_html = html!(
        <div id=msg_key.as_str() class="post">
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
