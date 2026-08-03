pub enum S3EndpointType {
    HTTPS,
    HTTP,
    Invalid,
}
pub fn validate_s3_endpoint(endpoint: &str) -> S3EndpointType {
    let Ok(url) = url::Url::parse(endpoint) else {
        return S3EndpointType::Invalid;
    };

    match url.scheme() {
        "https" => S3EndpointType::HTTPS,
        "http" => S3EndpointType::HTTP,
        _ => S3EndpointType::Invalid,
    }
}