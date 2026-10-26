use crate::{
    Error, InferenceConfig, Result,
    http::{base_url, body},
};
use serde::Serialize;
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use tokio::sync::Mutex;

pub struct Prepared {
    pub tokens: Vec<i32>,
}
/// One completion: the decoded action, the native generated-token count the
/// ledger settles, and the generated token ids the server's slot now holds.
/// `tokens` is empty when the server did not report ids; continuation then
/// falls back to re-rendering.
pub struct Completion {
    pub action: Value,
    pub generated: u64,
    pub tokens: Vec<i32>,
}
/// Exact chat-template text between turns, derived once from the model's own
/// template so a research step can extend the previous prompt token-for-token
/// instead of re-rendering it (ADR-0227 decision 1).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TurnDelimiters {
    /// Closes the previous turn and opens a user turn, e.g.
    /// `<|im_end|>\n<|im_start|>user\n`.
    pub user_open: String,
    /// Closes the user turn and opens generation, e.g.
    /// `<|im_end|>\n<|im_start|>assistant\n<think>\n\n</think>\n\n`.
    pub generation_tail: String,
}
#[derive(Clone)]
pub struct Model {
    client: reqwest::Client,
    config: InferenceConfig,
    delimiters: Arc<Mutex<Option<Option<TurnDelimiters>>>>,
}
impl Model {
    pub fn new(config: &InferenceConfig) -> Result<Self> {
        Ok(Self {
            client: config.client()?,
            config: config.clone(),
            delimiters: Arc::new(Mutex::new(None)),
        })
    }
    async fn post(&self, path: &str, value: impl Serialize) -> Result<Value> {
        let url = base_url(&self.config.model_url)?
            .join(path)
            .map_err(|_| Error::new("config", "invalid model route"))?;
        let response = self
            .client
            .post(url)
            .json(&value)
            .timeout(Duration::from_secs(self.config.inference_timeout_seconds))
            .send()
            .await
            .map_err(|_| Error::new("upstream", "local model request failed or timed out"))?;
        serde_json::from_slice(&body(response, 512 * 1024).await?)
            .map_err(|_| Error::new("model_protocol", "model returned invalid JSON"))
    }
    pub async fn tokenize(&self, content: &str) -> Result<Vec<i32>> {
        let value = self.post("tokenize", json!({"content": content, "add_special": false, "parse_special": true, "with_pieces": false})).await?;
        let tokens: Vec<i32> =
            serde_json::from_value(value.get("tokens").cloned().unwrap_or(Value::Null))
                .map_err(|_| Error::new("model_protocol", "tokenizer response has no token IDs"))?;
        if tokens.is_empty() || tokens.len() > 131072 || tokens.iter().any(|n| *n < 0) {
            return Err(Error::new(
                "model_protocol",
                "invalid or oversized tokenizer response",
            ));
        }
        Ok(tokens)
    }
    async fn render(&self, messages: &[Value]) -> Result<String> {
        let rendered = self
            .post(
                "apply-template",
                json!({"messages":messages,"chat_template_kwargs":{"enable_thinking":false}}),
            )
            .await?;
        rendered
            .get("prompt")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| Error::new("model_protocol", "template response has no prompt"))
    }
    pub async fn prepare(&self, messages: &[Value]) -> Result<Prepared> {
        self.prepare_within(messages, self.config.context_tokens)
            .await
    }
    /// A fresh research render, bounded by `render_tokens` so a full
    /// re-evaluation costs no more than before the context grew. Only exact
    /// continuation extends a prompt past it, up to `context_tokens`.
    pub async fn prepare_render(&self, messages: &[Value]) -> Result<Prepared> {
        self.prepare_within(messages, self.config.render_budget())
            .await
    }
    async fn prepare_within(&self, messages: &[Value], budget: usize) -> Result<Prepared> {
        let prompt = self.render(messages).await?;
        let tokens = self.tokenize(&prompt).await?;
        self.fits(&tokens, budget)?;
        Ok(Prepared { tokens })
    }
    fn fits(&self, tokens: &[i32], budget: usize) -> Result<()> {
        if tokens.len() + self.config.output_tokens as usize > budget {
            return Err(Error::new(
                "limit",
                "prompt plus output exceeds configured context",
            ));
        }
        Ok(())
    }
    /// The turn delimiters of the loaded template, probed once per process.
    /// `None` means the template could not be split reliably; callers then
    /// re-render every step as before. Never an error: continuation is an
    /// optimisation, not a capability.
    pub async fn turn_delimiters(&self) -> Option<TurnDelimiters> {
        let mut cached = self.delimiters.lock().await;
        if let Some(known) = cached.as_ref() {
            return known.clone();
        }
        let derived = self.derive_delimiters().await.ok().flatten();
        *cached = Some(derived.clone());
        derived
    }
    async fn derive_delimiters(&self) -> Result<Option<TurnDelimiters>> {
        const ONE: &str = "planetarium-turn-probe-one";
        const TWO: &str = "planetarium-turn-probe-two";
        let system = json!({"role":"system","content":"planetarium-turn-probe-system"});
        let one = self
            .render(&[system.clone(), json!({"role":"user","content":ONE})])
            .await?;
        let two = self
            .render(&[
                system,
                json!({"role":"user","content":ONE}),
                json!({"role":"user","content":TWO}),
            ])
            .await?;
        Ok(split_turns(&one, &two, ONE, TWO))
    }
    /// Extend the exact token sequence the server's slot holds — the previous
    /// prompt plus what it generated — with one more user turn. The result is
    /// a strict prefix extension, so the hybrid cache is reused in full.
    pub async fn continue_prompt(
        &self,
        previous: &[i32],
        generated: &[i32],
        delimiters: &TurnDelimiters,
        content: &str,
    ) -> Result<Prepared> {
        if previous.is_empty() || generated.is_empty() {
            return Err(Error::new(
                "continuation",
                "no exact previous prompt to continue",
            ));
        }
        let open = self.tokenize(&delimiters.user_open).await?;
        let mut suffix = self
            .tokenize(&format!(
                "{}{content}{}",
                delimiters.user_open, delimiters.generation_tail
            ))
            .await?;
        // The sampled stop token is already in the slot; do not send it twice.
        if generated.last() == open.first() && suffix.first() == open.first() {
            suffix.remove(0);
        }
        let mut tokens = Vec::with_capacity(previous.len() + generated.len() + suffix.len());
        tokens.extend_from_slice(previous);
        tokens.extend_from_slice(generated);
        tokens.extend(suffix);
        self.fits(&tokens, self.config.context_tokens)?;
        Ok(Prepared { tokens })
    }
    /// Generate one tool call under `schema` — the grammar of the activation's
    /// tool set (ADR-0227 §5: a follower in a mode is offered only that
    /// mode's tools).
    pub async fn complete<S: Serialize>(
        &self,
        prepared: Prepared,
        schema: &S,
        delimiters: Option<&TurnDelimiters>,
    ) -> Result<Completion> {
        self.complete_with_schema(prepared, schema, delimiters)
            .await
    }
    pub async fn complete_summary(&self, prepared: Prepared) -> Result<(Value, u64)> {
        // Serialize properties in generation order; Value maps sort keys alphabetically.
        #[derive(Serialize)]
        struct Properties {
            summary: Value,
            headline: Value,
        }
        #[derive(Serialize)]
        struct Schema {
            #[serde(rename = "type")]
            kind: &'static str,
            properties: Properties,
            required: [&'static str; 2],
            #[serde(rename = "additionalProperties")]
            additional_properties: bool,
        }
        let schema = Schema {
            kind: "object",
            properties: Properties {
                summary: json!({"type":"string","minLength":1,"maxLength":1600}),
                headline: json!({"type":"string","minLength":1,"maxLength":96}),
            },
            required: ["summary", "headline"],
            additional_properties: false,
        };
        let c = self.complete_with_schema(prepared, &schema, None).await?;
        Ok((c.action, c.generated))
    }
    async fn complete_with_schema<S: Serialize>(
        &self,
        prepared: Prepared,
        schema: &S,
        delimiters: Option<&TurnDelimiters>,
    ) -> Result<Completion> {
        // Naming the user delimiter lets the server checkpoint the hybrid
        // state where each user message starts, so the next activation reuses
        // the system prompt instead of re-evaluating it.
        let message_delimiters = delimiters
            .map(|d| vec![json!({"role":"user","delimiter":d.user_open})])
            .unwrap_or_default();
        let value = self
            .post(
                "completion",
                CompletionRequest {
                    prompt: prepared.tokens,
                    n_predict: self.config.output_tokens,
                    stream: false,
                    cache_prompt: true,
                    return_tokens: true,
                    temperature: 0.3,
                    json_schema: schema,
                    message_delimiters,
                },
            )
            .await?;
        if value.get("truncated").and_then(Value::as_bool) != Some(false) {
            return Err(Error::new(
                "model_protocol",
                "model context truncation is true or unknown",
            ));
        }
        let generated = value
            .pointer("/timings/predicted_n")
            .and_then(Value::as_u64)
            .or_else(|| value.get("tokens_predicted").and_then(Value::as_u64))
            .ok_or_else(|| Error::new("model_protocol", "model did not report generated tokens"))?;
        if generated > self.config.output_tokens as u64 {
            return Err(Error::new(
                "model_protocol",
                "model exceeded requested output allowance",
            ));
        }
        let content = value
            .get("content")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::new("model_protocol", "missing completion content"))?;
        let action = serde_json::from_str(content)
            .map_err(|_| Error::new("analysis", "completion is not a JSON object"))?;
        let tokens: Vec<i32> = value
            .get("tokens")
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .filter(|t: &Vec<i32>| {
                !t.is_empty()
                    && t.len() <= self.config.output_tokens as usize + 1
                    && t.iter().all(|n| *n >= 0)
            })
            .unwrap_or_default();
        Ok(Completion {
            action,
            generated,
            tokens,
        })
    }
}

/// Split two renders of the same system/user prefix — one with a single user
/// message, one with a second user message appended — into the text that opens
/// a user turn and the text that opens generation. Returns `None` whenever the
/// template does not render both probes verbatim in the expected order.
pub fn split_turns(
    one: &str,
    two: &str,
    probe_one: &str,
    probe_two: &str,
) -> Option<TurnDelimiters> {
    let end_one = one.rfind(probe_one)? + probe_one.len();
    let generation_tail = &one[end_one..];
    if !two.starts_with(&one[..end_one]) {
        return None;
    }
    let start_two = two.rfind(probe_two)?;
    if start_two < end_one {
        return None;
    }
    let user_open = &two[end_one..start_two];
    if !two[start_two + probe_two.len()..].ends_with(generation_tail)
        || user_open.is_empty()
        || generation_tail.is_empty()
        || user_open.len() > 256
        || generation_tail.len() > 256
    {
        return None;
    }
    Some(TurnDelimiters {
        user_open: user_open.to_owned(),
        generation_tail: generation_tail.to_owned(),
    })
}

#[derive(Serialize)]
struct CompletionRequest<'a, S> {
    prompt: Vec<i32>,
    n_predict: u32,
    stream: bool,
    cache_prompt: bool,
    return_tokens: bool,
    temperature: f64,
    json_schema: &'a S,
    message_delimiters: Vec<Value>,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn qwen_shaped_template_splits_into_user_open_and_generation_tail() {
        let one = "<|im_start|>system\nS<|im_end|>\n<|im_start|>user\nONE<|im_end|>\n<|im_start|>assistant\n<think>\n\n</think>\n\n";
        let two = "<|im_start|>system\nS<|im_end|>\n<|im_start|>user\nONE<|im_end|>\n<|im_start|>user\nTWO<|im_end|>\n<|im_start|>assistant\n<think>\n\n</think>\n\n";
        let d = split_turns(one, two, "ONE", "TWO").unwrap();
        assert_eq!(d.user_open, "<|im_end|>\n<|im_start|>user\n");
        assert_eq!(
            d.generation_tail,
            "<|im_end|>\n<|im_start|>assistant\n<think>\n\n</think>\n\n"
        );
    }
    #[test]
    fn a_template_that_reorders_or_rewrites_probes_yields_no_delimiters() {
        // Probe missing.
        assert!(split_turns("a ONE b", "a b", "ONE", "TWO").is_none());
        // Second render does not extend the first.
        assert!(split_turns("x ONE tail", "y ONE open TWO tail", "ONE", "TWO").is_none());
        // Tail differs after the second probe.
        assert!(split_turns("x ONE tail", "x ONE open TWO other", "ONE", "TWO").is_none());
        // Second probe before the first, or nothing between them.
        assert!(split_turns("x ONE tail", "x ONETWO tail", "ONE", "TWO").is_none());
    }
}
