use bytes::Bytes;
use http::{Method, Request};
use http_body_util::{BodyExt, Full};
use serde::Serialize;
use util_layer::StreamingBody;
use util_layer::traits::HyperClient;

pub struct ProxyHandler {
    proxy_url: String,
    hyper_client: HyperClient
}

#[derive(Serialize)]
struct CorruptRequest {
    target_key: String,
    method: String,
    count: usize,
}

impl ProxyHandler {
    pub fn new(proxy_url: String) -> Self {
        Self {
            proxy_url,
            hyper_client: http_layer::build_https_client(),
        }
    }

    pub fn get_proxy_url(&self) -> &str {
        &self.proxy_url
    }

    pub async fn corrupt_the_body_of_request_x_times(
        &self,
        request_type: &str,
        target_key: &str,
        times: usize,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let payload = CorruptRequest {
            target_key: target_key.to_string(),
            method: request_type.to_string(),
            count: times,
        };

        let body_bytes = serde_json::to_vec(&payload)?;

        let body: StreamingBody = Full::new(Bytes::from(body_bytes))
            .map_err(|never| match never {})
            .map_err(std::io::Error::other)
            .boxed();

        let req = Request::builder()
            .method(Method::POST)
            .uri(format!("{}/configure", self.proxy_url))
            .header("content-type", "application/json")
            .body(body)?;

        let resp = self.hyper_client.request(req).await?;

        println!(
            "Configured proxy corruptor, status={}",
            resp.status()
        );

        Ok(())
    }
}