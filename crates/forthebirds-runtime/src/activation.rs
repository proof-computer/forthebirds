use forthebirds_inference::{
    Error as InferenceError,
    model::{Model, Prepared},
};
use serde::Serialize;
use serde_json::{Value, json};

pub const ACTIVATION_INTERVAL_MS: u64 = 60_000;

pub fn activation_due(
    now_ms: u64,
    last_start: Option<u64>,
    deadline: u64,
    stopped: bool,
    trigger_available: bool,
) -> bool {
    !stopped
        && trigger_available
        && now_ms < deadline
        && last_start.is_none_or(|last| now_ms.saturating_sub(last) >= ACTIVATION_INTERVAL_MS)
}

#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub max_steps: usize,
    pub output_tokens: u32,
    pub continuation: bool,
}

pub enum Step<T> {
    Finish(T),
    Continue(String),
}

pub trait Activation: Send {
    type Output: Send;
    type Error: From<InferenceError> + Send;

    fn begin(
        &mut self,
        activation: &str,
        context: &mut Value,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
    fn before_turn(
        &mut self,
        turn: usize,
        context: &mut Value,
    ) -> impl Future<Output = Result<Option<String>, Self::Error>> + Send;
    /// Choose this turn's grammar after `before_turn` updates the context.
    /// `None` keeps the fixed schema passed to `run`; a returned schema's
    /// property order and the validation of generated actions belong to the host.
    fn schema_for_turn(
        &mut self,
        _turn: usize,
        _context: &Value,
    ) -> impl Future<Output = Result<Option<Value>, Self::Error>> + Send {
        std::future::ready(Ok(None))
    }
    fn prepare(
        &mut self,
        messages: &mut Vec<Value>,
        context: &Value,
    ) -> impl Future<Output = Result<Prepared, Self::Error>> + Send;
    fn reserve(
        &mut self,
        operation: &str,
        amount: u64,
        continued: bool,
        messages: &[Value],
    ) -> impl Future<Output = Result<String, Self::Error>> + Send;
    fn settle(
        &mut self,
        reservation: &str,
        actual: u64,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
    fn act(
        &mut self,
        activation: &str,
        operation: &str,
        call: &Value,
        input_tokens: u64,
        continued: bool,
        context: &mut Value,
    ) -> impl Future<Output = Result<Step<Self::Output>, Self::Error>> + Send;
}

pub async fn run<A: Activation, S: Serialize + Sync>(
    model: &Model,
    schema: &S,
    limits: Limits,
    host: &mut A,
    messages: &mut Vec<Value>,
    activation: &str,
) -> Result<A::Output, A::Error> {
    let mut context: Value = serde_json::from_str(
        messages[1]["content"]
            .as_str()
            .ok_or_else(|| InferenceError::new("invalid", "activation context missing"))?,
    )
    .map_err(|_| InferenceError::new("invalid", "activation context is not JSON"))?;
    host.begin(activation, &mut context).await?;
    // Exact-token continuation (ADR-0227 §1): after the first step the
    // prompt is the previous prompt, what the model generated and one
    // more user turn, so the server reuses its whole cache. `slot` is the
    // (prompt, generated) pair the server now holds; `pending_turn` is the
    // tool result not yet sent.
    let delimiters = if limits.continuation {
        model.turn_delimiters().await
    } else {
        None
    };
    let mut slot: Option<(Vec<i32>, Vec<i32>)> = None;
    let mut pending_turn: Option<String> = None;
    for turn in 0..limits.max_steps {
        let suffix = host.before_turn(turn, &mut context).await?;
        let turn_schema = host.schema_for_turn(turn, &context).await?;
        let mut continued = None;
        if let (Some((previous, generated)), Some(turn_text), Some(d)) =
            (&slot, &pending_turn, &delimiters)
        {
            let mut content = turn_text.clone();
            if let Some(text) = suffix {
                content.push_str(&text);
            }
            match model
                .continue_prompt(previous, generated, d, &content)
                .await
            {
                Ok(prepared) => continued = Some(prepared),
                // Over the context: fall back once to the trimmed
                // re-render, then continue from that prompt.
                Err(e) if e.code == "limit" || e.code == "continuation" => {}
                Err(e) => return Err(e.into()),
            }
        }
        let is_continuation = continued.is_some();
        let prepared = match continued {
            Some(prepared) => prepared,
            None => host.prepare(messages, &context).await?,
        };
        let input = prepared.tokens.len() as u64;
        let operation = format!("{activation}-{turn}");
        let reservation = host
            .reserve(
                &operation,
                input + limits.output_tokens as u64,
                is_continuation,
                messages,
            )
            .await?;
        // No automatic retry. If generation/decoding fails, the durable
        // reservation remains consumed, including across process restart.
        let prompt_tokens = prepared.tokens.clone();
        let completion = match turn_schema {
            Some(value) => {
                model
                    .complete(prepared, &value, delimiters.as_ref())
                    .await?
            }
            None => {
                model
                    .complete(prepared, schema, delimiters.as_ref())
                    .await?
            }
        };
        let (value, output) = (completion.action, completion.generated);
        slot = if completion.tokens.is_empty() {
            None
        } else {
            Some((prompt_tokens, completion.tokens))
        };
        host.settle(&reservation, input + output).await?;
        match host
            .act(
                activation,
                &operation,
                &value,
                input,
                is_continuation,
                &mut context,
            )
            .await?
        {
            Step::Finish(out) => return Ok(out),
            Step::Continue(reply) => {
                let turn_text = format!("Tool result: {reply}");
                messages.push(json!({"role":"assistant","content":value.to_string()}));
                messages.push(json!({"role":"user","content":turn_text.clone()}));
                pending_turn = Some(turn_text);
            }
        }
    }
    Err(InferenceError::new(
        "tool_limit",
        "activation reached its tool-step limit; committed actions remain available",
    )
    .into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Json, Router, extract::State as HttpState, routing::post};
    use forthebirds_inference::InferenceConfig;
    use std::sync::{Arc, Mutex};

    fn limits() -> Limits {
        Limits {
            max_steps: 3,
            output_tokens: 64,
            continuation: false,
        }
    }

    fn model(url: &str) -> Model {
        Model::new(&InferenceConfig {
            model_url: url.to_owned(),
            embedding_url: None,
            context_tokens: 4096,
            output_tokens: 64,
            render_tokens: None,
            inference_timeout_seconds: 30,
            request_timeout_seconds: 30,
            user_agent: "planetarium-activation-test".into(),
        })
        .unwrap()
    }

    fn completion(tool: &str) -> Value {
        json!({
            "content": json!({"tool": tool, "arguments": {}}).to_string(),
            "truncated": false,
            "timings": {"predicted_n": 3}
        })
    }

    #[derive(Clone)]
    struct Script {
        calls: Arc<Mutex<Vec<Value>>>,
        requests: Arc<Mutex<Vec<Value>>>,
    }

    async fn serve(calls: Vec<Value>) -> (String, tokio::task::JoinHandle<()>, Script) {
        let script = Script {
            calls: Arc::new(Mutex::new(calls)),
            requests: Arc::new(Mutex::new(Vec::new())),
        };
        let router = Router::new()
            .route(
                "/completion",
                post(
                    |HttpState(script): HttpState<Script>, Json(request): Json<Value>| async move {
                        script.requests.lock().unwrap().push(request);
                        Json(script.calls.lock().unwrap().remove(0))
                    },
                ),
            )
            .with_state(script.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        (url, task, script)
    }

    struct Rec {
        log: Vec<String>,
        fail_before: bool,
    }

    impl Activation for Rec {
        type Output = String;
        type Error = InferenceError;

        async fn begin(
            &mut self,
            activation: &str,
            _context: &mut Value,
        ) -> Result<(), Self::Error> {
            self.log.push(format!("begin {activation}"));
            Ok(())
        }
        async fn before_turn(
            &mut self,
            turn: usize,
            _context: &mut Value,
        ) -> Result<Option<String>, Self::Error> {
            if self.fail_before && turn == 0 {
                return Err(InferenceError::new("stopped", "before turn zero"));
            }
            Ok(None)
        }
        async fn prepare(
            &mut self,
            _messages: &mut Vec<Value>,
            _context: &Value,
        ) -> Result<Prepared, Self::Error> {
            Ok(Prepared {
                tokens: vec![1; 10],
            })
        }
        async fn reserve(
            &mut self,
            operation: &str,
            amount: u64,
            continued: bool,
            _messages: &[Value],
        ) -> Result<String, Self::Error> {
            self.log
                .push(format!("reserve {operation} {amount} {continued}"));
            Ok(format!("r-{operation}"))
        }
        async fn settle(&mut self, reservation: &str, actual: u64) -> Result<(), Self::Error> {
            self.log.push(format!("settle {reservation} {actual}"));
            Ok(())
        }
        async fn act(
            &mut self,
            _activation: &str,
            operation: &str,
            call: &Value,
            input_tokens: u64,
            _continued: bool,
            _context: &mut Value,
        ) -> Result<Step<String>, Self::Error> {
            let tool = call["tool"].as_str().unwrap();
            self.log
                .push(format!("act {operation} \"{tool}\" {input_tokens}"));
            if tool == "finish" {
                Ok(Step::Finish("done".into()))
            } else {
                Ok(Step::Continue("{\"ok\":true}".into()))
            }
        }
    }

    fn messages() -> Vec<Value> {
        vec![
            json!({"role":"system","content":"s"}),
            json!({"role":"user","content":"{}"}),
        ]
    }

    #[tokio::test]
    async fn scripted_look_then_finish_logs_the_reserve_settle_act_order() {
        let (url, task, _) = serve(vec![completion("look"), completion("finish")]).await;
        let model = model(&url);
        let mut host = Rec {
            log: Vec::new(),
            fail_before: false,
        };
        let mut messages = messages();
        let output = run(&model, &json!({}), limits(), &mut host, &mut messages, "a")
            .await
            .unwrap();
        task.abort();
        assert_eq!(output, "done");
        assert_eq!(
            host.log,
            vec![
                "begin a".to_string(),
                "reserve a-0 74 false".to_string(),
                "settle r-a-0 13".to_string(),
                "act a-0 \"look\" 10".to_string(),
                "reserve a-1 74 false".to_string(),
                "settle r-a-1 13".to_string(),
                "act a-1 \"finish\" 10".to_string(),
            ]
        );
        assert_eq!(messages[3]["content"], "Tool result: {\"ok\":true}");
    }

    #[tokio::test]
    async fn three_non_finishing_calls_end_at_tool_limit_after_three_reservations() {
        let (url, task, _) = serve(vec![
            completion("look"),
            completion("look"),
            completion("look"),
        ])
        .await;
        let model = model(&url);
        let mut host = Rec {
            log: Vec::new(),
            fail_before: false,
        };
        let mut messages = messages();
        let error = run(&model, &json!({}), limits(), &mut host, &mut messages, "a")
            .await
            .unwrap_err();
        task.abort();
        assert_eq!(error.code, "tool_limit");
        assert_eq!(
            error.message,
            "activation reached its tool-step limit; committed actions remain available"
        );
        assert_eq!(
            host.log
                .iter()
                .filter(|line| line.starts_with("reserve"))
                .count(),
            3
        );
    }

    #[tokio::test]
    async fn before_turn_failure_at_turn_zero_reserves_nothing() {
        let (url, task, _) = serve(vec![]).await;
        let model = model(&url);
        let mut host = Rec {
            log: Vec::new(),
            fail_before: true,
        };
        let mut messages = messages();
        let error = run(&model, &json!({}), limits(), &mut host, &mut messages, "a")
            .await
            .unwrap_err();
        task.abort();
        assert_eq!(error.code, "stopped");
        assert_eq!(error.message, "before turn zero");
        assert!(host.log.iter().all(|line| !line.starts_with("reserve")));
    }

    struct SchemaRec {
        rec: Rec,
        narrowed: Value,
        fail_schema: bool,
        turns: Vec<usize>,
    }

    impl Activation for SchemaRec {
        type Output = String;
        type Error = InferenceError;

        async fn begin(
            &mut self,
            activation: &str,
            context: &mut Value,
        ) -> Result<(), Self::Error> {
            self.rec.begin(activation, context).await
        }
        async fn before_turn(
            &mut self,
            turn: usize,
            context: &mut Value,
        ) -> Result<Option<String>, Self::Error> {
            context["turn"] = json!(turn);
            self.rec.before_turn(turn, context).await
        }
        async fn schema_for_turn(
            &mut self,
            turn: usize,
            context: &Value,
        ) -> Result<Option<Value>, Self::Error> {
            assert_eq!(context["turn"], json!(turn));
            self.turns.push(turn);
            if self.fail_schema {
                return Err(InferenceError::new("schema", "schema refused"));
            }
            Ok((turn == 0).then(|| self.narrowed.clone()))
        }
        async fn prepare(
            &mut self,
            messages: &mut Vec<Value>,
            context: &Value,
        ) -> Result<Prepared, Self::Error> {
            assert!(!self.fail_schema, "prepare must follow schema selection");
            self.rec.prepare(messages, context).await
        }
        async fn reserve(
            &mut self,
            operation: &str,
            amount: u64,
            continued: bool,
            messages: &[Value],
        ) -> Result<String, Self::Error> {
            self.rec
                .reserve(operation, amount, continued, messages)
                .await
        }
        async fn settle(&mut self, reservation: &str, actual: u64) -> Result<(), Self::Error> {
            self.rec.settle(reservation, actual).await
        }
        async fn act(
            &mut self,
            activation: &str,
            operation: &str,
            call: &Value,
            input_tokens: u64,
            continued: bool,
            context: &mut Value,
        ) -> Result<Step<String>, Self::Error> {
            self.rec
                .act(
                    activation,
                    operation,
                    call,
                    input_tokens,
                    continued,
                    context,
                )
                .await
        }
    }

    #[tokio::test]
    async fn per_turn_schema_overrides_then_falls_back_without_changing_action_order() {
        let (url, task, script) = serve(vec![completion("look"), completion("finish")]).await;
        let model = model(&url);
        let fixed = json!({"type":"object", "properties":{"tool":{"enum":["look", "finish"]}}});
        let narrowed = json!({"type":"object", "properties":{"tool":{"const":"look"}}});
        let mut host = SchemaRec {
            rec: Rec {
                log: Vec::new(),
                fail_before: false,
            },
            narrowed: narrowed.clone(),
            fail_schema: false,
            turns: Vec::new(),
        };
        let output = run(&model, &fixed, limits(), &mut host, &mut messages(), "a")
            .await
            .unwrap();
        task.abort();
        assert_eq!(output, "done");
        assert_eq!(host.turns, vec![0, 1]);
        let requests = script.requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0]["json_schema"], narrowed);
        assert_eq!(requests[1]["json_schema"], fixed);
        assert_eq!(
            host.rec.log,
            vec![
                "begin a",
                "reserve a-0 74 false",
                "settle r-a-0 13",
                "act a-0 \"look\" 10",
                "reserve a-1 74 false",
                "settle r-a-1 13",
                "act a-1 \"finish\" 10",
            ]
        );
    }

    #[tokio::test]
    async fn schema_failure_at_turn_zero_sends_no_request_and_reserves_nothing() {
        let (url, task, script) = serve(vec![]).await;
        let model = model(&url);
        let mut host = SchemaRec {
            rec: Rec {
                log: Vec::new(),
                fail_before: false,
            },
            narrowed: json!({}),
            fail_schema: true,
            turns: Vec::new(),
        };
        let error = run(
            &model,
            &json!({}),
            limits(),
            &mut host,
            &mut messages(),
            "a",
        )
        .await
        .unwrap_err();
        task.abort();
        assert_eq!(error.code, "schema");
        assert_eq!(error.message, "schema refused");
        assert_eq!(host.turns, vec![0]);
        assert!(script.requests.lock().unwrap().is_empty());
        assert_eq!(host.rec.log, vec!["begin a"]);
    }

    #[test]
    fn activation_due_keeps_throttle_stop_deadline_and_trigger() {
        assert!(activation_due(100_000, None, 500_000, false, true));
        assert!(!activation_due(
            159_999,
            Some(100_000),
            500_000,
            false,
            true
        ));
        assert!(activation_due(160_000, Some(100_000), 500_000, false, true));
        assert!(!activation_due(
            160_000,
            Some(100_000),
            160_000,
            false,
            true
        ));
        assert!(!activation_due(160_000, Some(100_000), 500_000, true, true));
        assert!(!activation_due(
            160_000,
            Some(100_000),
            500_000,
            false,
            false
        ));
        assert!(!activation_due(
            170_000,
            Some(160_000),
            500_000,
            false,
            true
        ));
        assert!(!activation_due(99_000, Some(100_000), 500_000, false, true));
    }

    #[test]
    fn production_source_names_no_crate_path() {
        let source = include_str!("activation.rs");
        let production = source.split("#[cfg(test)]").next().unwrap();
        assert!(!production.contains("crate::"));
    }
}
