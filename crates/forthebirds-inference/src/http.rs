//! Bounded HTTP responses and validated service URLs.

use crate::{Error, Result};
use reqwest::Url;

pub fn base_url(value: &str) -> Result<Url> {
    let url = Url::parse(value).map_err(|_| Error::new("config", "invalid service URL"))?;
    if !["http", "https"].contains(&url.scheme())
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || !url.path().ends_with('/')
    {
        return Err(Error::new(
            "config",
            "service URLs must end in / and have no credentials, query or fragment",
        ));
    }
    Ok(url)
}

pub async fn body(mut response: reqwest::Response, limit: usize) -> Result<Vec<u8>> {
    if !response.status().is_success() {
        return Err(Error::new(
            "upstream",
            format!("HTTP {}", response.status().as_u16()),
        ));
    }
    if response.content_length().is_some_and(|n| n > limit as u64) {
        return Err(Error::new("limit", "response too large"));
    }
    let mut result = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| Error::new("upstream", "response stream failed"))?
    {
        if chunk.len() > limit.saturating_sub(result.len()) {
            return Err(Error::new("limit", "response too large"));
        }
        result.extend_from_slice(&chunk);
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base_url_accepts_http_and_https_service_roots() {
        let local = "http://127.0.0.1:8080/";
        assert_eq!(base_url(local).unwrap().as_str(), local);
        assert!(base_url("https://example.com/inference/").is_ok());
    }

    #[test]
    fn base_url_rejects_queries_credentials_and_missing_path_slashes() {
        for value in [
            "http://127.0.0.1:8080/?query=1",
            "http://user@127.0.0.1:8080/",
            "http://user:password@127.0.0.1:8080/",
            "http://127.0.0.1:8080/inference",
            "http://127.0.0.1:8080/#fragment",
            "ftp://127.0.0.1:8080/",
            "http:///",
        ] {
            let error = base_url(value).unwrap_err();
            assert_eq!(error.code, "config", "{value}");
        }
    }
}
