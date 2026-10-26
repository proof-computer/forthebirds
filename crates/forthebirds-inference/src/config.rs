use crate::{Error, Result};
use std::time::Duration;

/// Fresh-render budget when `render_tokens` is unset.
pub const DEFAULT_RENDER_TOKENS: usize = 4096;

/// Consumer-supplied endpoints, token budgets and HTTP client settings.
#[derive(Debug, Clone)]
pub struct InferenceConfig {
    pub model_url: String,
    pub embedding_url: Option<String>,
    pub context_tokens: usize,
    pub output_tokens: u32,
    pub render_tokens: Option<usize>,
    pub inference_timeout_seconds: u64,
    pub request_timeout_seconds: u64,
    pub user_agent: String,
}

impl InferenceConfig {
    /// Prompt-plus-output budget of a fresh render.
    pub fn render_budget(&self) -> usize {
        self.render_tokens
            .unwrap_or(self.context_tokens.min(DEFAULT_RENDER_TOKENS))
    }

    /// Build a direct HTTP client that does not follow redirects.
    pub fn client(&self) -> Result<reqwest::Client> {
        reqwest::Client::builder()
            .user_agent(&self.user_agent)
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(self.request_timeout_seconds))
            .build()
            .map_err(|_| Error::new("config", "cannot build HTTP client"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_budget_uses_explicit_budget_or_caps_the_context() {
        let mut config = InferenceConfig {
            model_url: "http://127.0.0.1:8080/".into(),
            embedding_url: None,
            context_tokens: DEFAULT_RENDER_TOKENS * 2,
            output_tokens: 256,
            render_tokens: None,
            inference_timeout_seconds: 60,
            request_timeout_seconds: 30,
            user_agent: "forthebirds-inference/test".into(),
        };
        assert_eq!(config.render_budget(), DEFAULT_RENDER_TOKENS);
        config.context_tokens = 1024;
        assert_eq!(config.render_budget(), 1024);
        config.render_tokens = Some(2048);
        assert_eq!(config.render_budget(), 2048);
        config.render_tokens = Some(0);
        assert_eq!(config.render_budget(), 0);
        config.render_tokens = None;
        config.context_tokens = 0;
        assert_eq!(config.render_budget(), 0);
    }
}
