use serde::Serialize;
use serde_json::{Value, json};

// Struct field order is intentional. llama.cpp uses property order when it
// builds its grammar: select the tool before generating its arguments. Keep
// this local to the model request; do not change persisted JSON serialization.
#[derive(Serialize)]
pub struct GenerationSchema {
    #[serde(rename = "oneOf")]
    choices: Vec<ToolSchema>,
}
#[derive(Serialize)]
pub struct ToolSchema {
    #[serde(rename = "type")]
    kind: &'static str,
    properties: ToolProperties,
    required: [&'static str; 2],
    #[serde(rename = "additionalProperties")]
    additional: bool,
}
#[derive(Serialize)]
struct ToolProperties {
    tool: Value,
    arguments: Value,
}

pub fn text(max: usize) -> Value {
    json!({"type":"string","minLength":1,"maxLength":max})
}
pub fn nullable(schema: Value) -> Value {
    json!({"anyOf":[schema,{"type":"null"}]})
}
pub fn tool(name: &str, properties: Value, required: &[&str]) -> ToolSchema {
    ToolSchema {
        kind: "object",
        properties: ToolProperties {
            tool: json!({"const": name}),
            arguments: json!({"type":"object","properties":properties,"required":required,"additionalProperties":false}),
        },
        required: ["tool", "arguments"],
        additional: false,
    }
}

impl GenerationSchema {
    pub fn select(choices: Vec<ToolSchema>, allows: impl Fn(&str) -> bool) -> Self {
        Self {
            choices: choices
                .into_iter()
                .filter(|t| allows(t.properties.tool["const"].as_str().expect("tool name")))
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn select_keeps_allowed_tools_in_order_with_tool_before_arguments() {
        let schema = GenerationSchema::select(
            vec![
                tool("look", json!({"q": text(8)}), &["q"]),
                tool("finish", json!({"summary": text(16)}), &["summary"]),
                tool("skip", json!({}), &[]),
            ],
            |name| name != "skip",
        );
        let value = serde_json::to_value(&schema).unwrap();
        let choices = value["oneOf"].as_array().unwrap();
        assert_eq!(
            choices
                .iter()
                .map(|choice| choice["properties"]["tool"]["const"].as_str().unwrap())
                .collect::<Vec<_>>(),
            vec!["look", "finish"]
        );
        let rendered = serde_json::to_string(&schema).unwrap();
        let mut search = 0;
        let mut seen = 0;
        while let Some(offset) = rendered[search..].find("\"properties\":{\"tool\":") {
            let at = search + offset;
            let arguments = rendered[at..]
                .find("\"arguments\":")
                .expect("arguments follow the tool property");
            assert!(arguments > 0);
            seen += 1;
            search = at + 1;
        }
        assert_eq!(seen, choices.len());
        for choice in choices {
            assert_eq!(choice["required"], json!(["tool", "arguments"]));
            assert_eq!(choice["additionalProperties"], false);
        }
    }

    #[test]
    fn text_and_nullable_produce_the_moved_json() {
        assert_eq!(
            text(12),
            json!({"type":"string","minLength":1,"maxLength":12})
        );
        let inner = json!({"type":"string"});
        assert_eq!(
            nullable(inner.clone()),
            json!({"anyOf":[inner, {"type":"null"}]})
        );
    }

    #[test]
    fn production_source_names_no_crate_path() {
        let source = include_str!("grammar.rs");
        let production = source.split("#[cfg(test)]").next().unwrap();
        assert!(!production.contains("crate::"));
    }
}
