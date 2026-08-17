use bytes::Bytes;
use http::{Method, Request};
use http_body_util::{BodyExt, Empty, Full};
use http_layer::{HyperClient, StreamingBody};
use serde::Serialize;

pub struct ProxyHandler {
    proxy_url: String,
    hyper_client: HyperClient,
}

#[derive(Serialize)]
struct CorruptRequest {
    target_key: String,
    method: String,
    count: usize,
}

#[derive(Serialize)]
struct CutConnectionRequest {
    target_key: String,
    method: String,
    count: usize,
}

#[derive(Serialize)]
struct TruncateResponseRequest {
    target_key: String,
    method: String,
    count: usize,
    bytes: usize,
}

#[derive(Serialize)]
struct FakeResponseRequest {
    target_key: String,
    method: String,
    count: usize,
    status_code: u16,
    headers: std::collections::HashMap<String, String>,
    body: String,
}

#[derive(Debug, serde::Deserialize, PartialEq, Eq)]
pub struct RequestCounts {
    pub GET: usize,
    pub PUT: usize,
    pub POST: usize,
    pub HEAD: usize,
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

    async fn post_json<T: Serialize>(
        &self,
        endpoint: &str,
        payload: &T,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let body_bytes = serde_json::to_vec(payload)?;

        let body: StreamingBody = Full::new(Bytes::from(body_bytes))
            .map_err(|never| match never {})
            .map_err(std::io::Error::other)
            .boxed();

        let req = Request::builder()
            .method(Method::POST)
            .uri(format!("{}/{}", self.proxy_url, endpoint))
            .header("content-type", "application/json")
            .body(body)?;

        let resp = self.hyper_client.request(req).await?;

        println!(
            "Configured proxy endpoint={}, status={}",
            endpoint,
            resp.status()
        );

        Ok(())
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

        self.post_json("configure-corrupt", &payload).await
    }

    pub async fn cut_connection_x_times(
        &self,
        request_type: &str,
        target_key: &str,
        times: usize,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let payload = CutConnectionRequest {
            target_key: target_key.to_string(),
            method: request_type.to_string(),
            count: times,
        };

        self.post_json("configure-cut-connection", &payload)
            .await
    }

    pub async fn truncate_response_x_times(
        &self,
        request_type: &str,
        target_key: &str,
        times: usize,
        bytes: usize,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let payload = TruncateResponseRequest {
            target_key: target_key.to_string(),
            method: request_type.to_string(),
            count: times,
            bytes,
        };

        self.post_json("configure-truncate-response", &payload)
            .await
    }

    pub async fn fake_response_x_times(
        &self,
        request_type: &str,
        target_key: &str,
        times: usize,
        status_code: u16,
        headers: std::collections::HashMap<String, String>,
        body: String,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let payload = FakeResponseRequest {
            target_key: target_key.to_string(),
            method: request_type.to_string(),
            count: times,
            status_code,
            headers,
            body,
        };

        self.post_json("configure-fake-response", &payload)
            .await
    }

    pub async fn reset_counts(
        &self,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let payload = serde_json::json!({});

        self.post_json("reset-counts", &payload).await
    }

    pub async fn get_counts(
        &self,
    ) -> Result<RequestCounts, Box<dyn std::error::Error + Send + Sync>> {
        let body: StreamingBody = Empty::<Bytes>::new()
            .map_err(std::io::Error::other)
            .boxed();

        let req = Request::builder()
            .method(Method::GET)
            .uri(format!("{}/counts", self.proxy_url))
            .body(body)?;

        let resp = self.hyper_client.request(req).await?;

        let body = resp.into_body().collect().await?.to_bytes();

        Ok(serde_json::from_slice(&body)?)
    }
}