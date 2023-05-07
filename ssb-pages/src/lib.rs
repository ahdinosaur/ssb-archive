use axohtml::{dom::DOMTree, html, unsafe_text};
use serde_json::Value;
use ssb_markdown::render;
use ssb_msg::{Msg, PostContent};

pub enum PageError {}

pub fn render_post(msg: Msg<Value>, content: PostContent) -> Result<DOMTree<String>, PageError> {
    let msg_ref = Into::<String>::into(&msg.key);
    let content_html = render(content.text.as_str());

    let post_html = html!(
        <div id=msg_ref.as_str() class="post">
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
