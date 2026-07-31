use hyper::{Request, Response};
use tokio::sync::mpsc::UnboundedSender;
use util_layer::cc_util::CommandCenterEvent;

fn is_sensitive(name: &hyper::header::HeaderName) -> bool {
    matches!(
        name.as_str().to_ascii_lowercase().as_str(),
        "authorization" | "proxy-authorization" | "cookie" | "set-cookie" | "x-api-key"
    )
}

pub fn log_request_headers<B>(req: &Request<B>, cc_tx: UnboundedSender<CommandCenterEvent>) {
    let headers_str = req.headers().iter()
        .map(|(name, value)| {
            if is_sensitive(name) {
                format!("  {}: <redacted>", name)
            } else {
                format!("  {}: {}", name, value.to_str().unwrap_or("<non-utf8>"))
            }
        })
        .collect::<Vec<_>>()
        .join("\n");

    let log_msg = format!(
        "outgoing request\nmethod: {}\nuri: {}\nheaders:\n{}",
        req.method(),
        req.uri(),
        headers_str
    );

    cc_tx.send(CommandCenterEvent::Log(log_msg)).expect("Failed to send log event to Command Center");
}

pub fn log_response_headers<B>(resp: &Response<B>, cc_tx: UnboundedSender<CommandCenterEvent>) {
    let headers_str = resp.headers().iter()
        .map(|(name, value)| {
            if is_sensitive(name) {
                format!("  {}: <redacted>", name)
            } else {
                format!("  {}: {}", name, value.to_str().unwrap_or("<non-utf8>"))
            }
        })
        .collect::<Vec<_>>()
        .join("\n");

    let log_msg = format!(
        "incoming response\nstatus: {}\nheaders:\n{}",
        resp.status(),
        headers_str
    );

    cc_tx.send(CommandCenterEvent::Log(log_msg)).expect("Failed to send log event to Command Center");
}