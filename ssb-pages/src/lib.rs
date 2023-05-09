use axohtml::{dom::DOMTree, html, unsafe_text};
use serde_json::{to_string_pretty, Value};
use ssb_markdown::render;
use ssb_msg::{Msg, MsgContent, PostContent};
use std::{io, path::PathBuf};
use tokio::fs::write;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Failed to write to file: {0}")]
    WriteFile(#[source] io::Error),
    #[error("Failed to serialize JSON to string: {0}")]
    JsonToString(#[source] serde_json::Error),
}

pub struct Config {
    base_dir: PathBuf,
}

pub async fn write_thread_html(
    config: &Config,
    thread: &Thread<Value>,
    content: &MsgContent,
) -> Result<(), Error> {
    let Config { base_dir } = config;
    let page_path = base_dir.join(msg.key.to_page_path()).with_extension("html");
    let page_html = render_msg(msg, content);
    let page_bytes = page_html.to_string();

    write(page_path, page_bytes)
        .await
        .map_err(Error::WriteFile)?;

    Ok(())
}

pub async fn write_msg_json(config: &Config, msg: &Msg<Value>) -> Result<(), Error> {
    let Config { base_dir } = config;
    let json_path = base_dir.join(msg.key.to_page_path()).with_extension("json");
    let msg_json = to_string_pretty(&msg).map_err(Error::JsonToString)?;

    write(json_path, msg_json).await.map_err(Error::WriteFile)?;

    Ok(())
}

pub fn render_thread(msg: &Msg<Value>, content: &PostContent) -> DOMTree<String> {

pub fn render_post(msg: &Msg<Value>, content: &PostContent) -> DOMTree<String> {
    let msg_ref = msg.key.to_string();

    let action: DOMTree<String> = match content {
        MsgContent::Post(post) => {
            html!(
                <span class="action">
                </span>
            )
        }
        _ => {
            html!(
                <span class="action">
                </span>
            )
        }
    };

    let content: DOMTree<String> = match content {
        MsgContent::Post(post) => {
            let text_html = render(post.text.as_str());

            html!(
            )
        }
        _ => {
            html!(
                <article class="content">
                </article>
            )
        }
    };

    html!(
        <div id=msg_ref.as_str() class="post">
            <header>
            </header>
            <article class="content">
                {unsafe_text!(text_html)}
            </article>
        </div>
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {}
}
