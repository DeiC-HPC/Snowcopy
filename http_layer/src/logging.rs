use hyper::{Request, Response};
use tracing::debug;

fn is_sensitive(name: &hyper::header::HeaderName) -> bool {
    matches!(
        name.as_str().to_ascii_lowercase().as_str(),
        "authorization" | "proxy-authorization" | "cookie" | "set-cookie" | "x-api-key"
    )
}

pub fn log_request_headers<B>(req: &Request<B>) {
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

    debug!(
        method = %req.method(), 
        uri = %req.uri(), 
        headers = %format!("\n{}", headers_str),
        "outgoing request"
    );
}

pub fn log_response_headers<B>(resp: &Response<B>) {
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

    debug!(
        status = %resp.status(), 
        headers = %format!("\n{}", headers_str),
        "incoming response"
    );
}