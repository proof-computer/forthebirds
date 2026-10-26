//! Local embedding transport. Configuration labels are not model attestation.
use crate::{
    Error, InferenceConfig, Result,
    http::{base_url, body},
};
use serde_json::{Value, json};
#[derive(Clone)]
pub struct Embedder {
    client: reqwest::Client,
    config: InferenceConfig,
}
impl Embedder {
    pub fn new(config: &InferenceConfig) -> Result<Self> {
        Ok(Self {
            client: config.client()?,
            config: config.clone(),
        })
    }
    async fn post(&self, path: &str, input: Value) -> Result<Value> {
        let base = self.config.embedding_url.as_deref().ok_or_else(|| {
            Error::new(
                "embedding_unavailable",
                "local embedding endpoint is disabled",
            )
        })?;
        let url = base_url(base)?
            .join(path)
            .map_err(|_| Error::new("config", "invalid embedding route"))?;
        let response = self
            .client
            .post(url)
            .json(&input)
            .timeout(std::time::Duration::from_secs(10))
            .send()
            .await
            .map_err(|_| {
                Error::new(
                    "embedding_unavailable",
                    "local embedding request failed or timed out",
                )
            })?;
        serde_json::from_slice(&body(response, 128 * 1024).await?)
            .map_err(|_| Error::new("embedding_protocol", "invalid embedding JSON"))
    }
    pub async fn prepare(&self, text: &str, query: bool) -> Result<(Vec<i32>, bool)> {
        let text = if query {
            format!("Represent this sentence for searching relevant passages: {text}")
        } else {
            text.into()
        };
        let v = self
            .post(
                "tokenize",
                json!({"content":text,"add_special":true,"parse_special":false}),
            )
            .await?;
        let mut tokens: Vec<i32> = serde_json::from_value(v["tokens"].clone())
            .map_err(|_| Error::new("embedding_protocol", "missing embedding token IDs"))?;
        if tokens.is_empty() || tokens.len() > 16384 || tokens.iter().any(|n| *n < 0) {
            return Err(Error::new("embedding_protocol", "invalid embedding tokens"));
        }
        let truncated = tokens.len() > 512;
        if truncated {
            let last = *tokens.last().expect("nonempty");
            tokens.truncate(511);
            tokens.push(last);
        }
        Ok((tokens, truncated))
    }
    pub async fn embed(&self, tokens: Vec<i32>) -> Result<(Vec<f32>, u64)> {
        let maximum = tokens.len() as u64;
        let v = self
            .post(
                "v1/embeddings",
                json!({"input":tokens,"encoding_format":"float"}),
            )
            .await?;
        if v["data"].as_array().map(Vec::len) != Some(1) {
            return Err(Error::new(
                "embedding_protocol",
                "expected one pooled embedding",
            ));
        }
        let mut vector: Vec<f32> = serde_json::from_value(v["data"][0]["embedding"].clone())
            .map_err(|_| Error::new("embedding_protocol", "invalid embedding vector"))?;
        let actual = v["usage"]["prompt_tokens"]
            .as_u64()
            .ok_or_else(|| Error::new("embedding_protocol", "missing embedding usage"))?;
        if actual > maximum || vector.len() != 384 || vector.iter().any(|n| !n.is_finite()) {
            return Err(Error::new(
                "embedding_protocol",
                "invalid vector dimension/value/usage",
            ));
        }
        let norm = vector
            .iter()
            .map(|n| (*n as f64).powi(2))
            .sum::<f64>()
            .sqrt();
        if !norm.is_finite() || norm == 0.0 {
            return Err(Error::new(
                "embedding_protocol",
                "zero/nonfinite embedding norm",
            ));
        }
        for n in &mut vector {
            *n = (*n as f64 / norm) as f32;
        }
        Ok((vector, actual))
    }
}
