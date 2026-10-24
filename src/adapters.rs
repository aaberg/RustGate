use crate::config::ProviderConfig;
use serde_json::Value;

/// A provider adapter knows how to talk to one class of upstream LLM API.
/// The client-facing protocol is always the OpenAI-compatible shape; adapters
/// translate at the seam if a provider diverges.
pub trait ProviderAdapter: Send + Sync {
    fn id(&self) -> &str;
    fn config(&self) -> &ProviderConfig;
    /// Whether this adapter forwards the given path (after the proxy's own
    /// allowlist check, this is the provider-specific confirmation).
    fn forwards(&self, path: &str) -> bool {
        self.config().allow_paths.iter().any(|p| p == path)
    }
    /// The Authorization header value for a request using `key`.
    fn auth_header(&self, key: &str) -> String {
        format!("Bearer {key}")
    }
    /// Mutate the request body before forwarding (e.g. request stream usage).
    fn transform_request(&self, _path: &str, _body: &mut Value) {}
    /// Mutate the response body (non-streaming) after receiving it.
    fn transform_response(&self, _body: &mut Value) {}
    /// Extract usage from a non-streaming response body.
    fn extract_usage_response(&self, body: &Value) -> Option<Usage> {
        extract_openai_usage(body)
    }
    /// Extract usage from one SSE chunk of a streaming response.
    fn extract_usage_chunk(&self, chunk: &Value) -> Option<Usage> {
        extract_openai_usage(chunk)
    }
    /// Whether the request body is a chat-completions style body with a
    /// messages array (i.e. eligible for system-prompt injection).
    fn is_chat_body(&self, path: &str) -> bool {
        path == "/v1/chat/completions"
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Usage {
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
}

impl Usage {
    pub fn total(&self) -> u64 {
        self.prompt_tokens + self.completion_tokens
    }
}

fn extract_openai_usage(v: &Value) -> Option<Usage> {
    let u = v.get("usage")?;
    Some(Usage {
        prompt_tokens: u.get("prompt_tokens")?.as_u64()?,
        completion_tokens: u
            .get("completion_tokens")
            .and_then(|c| c.as_u64())
            .unwrap_or(0),
    })
}

/// Adapter for OpenAI-compatible providers (Mistral, Groq, vLLM, OpenAI, ...).
/// Nearly a no-op: base URL and key shapes come from config.
pub struct OpenAICompatible {
    id: String,
    cfg: ProviderConfig,
}

impl OpenAICompatible {
    pub fn new(id: String, cfg: ProviderConfig) -> Self {
        OpenAICompatible { id, cfg }
    }
}

impl ProviderAdapter for OpenAICompatible {
    fn id(&self) -> &str {
        &self.id
    }
    fn config(&self) -> &ProviderConfig {
        &self.cfg
    }
    fn transform_request(&self, path: &str, body: &mut Value) {
        // Ask the upstream to include usage in streamed responses so the
        // proxy can account tokens on streams. Only for streaming chats.
        if path == "/v1/chat/completions"
            && self.cfg.request_stream_usage
            && body.get("stream").and_then(|s| s.as_bool()) == Some(true)
        {
            let so = body
                .as_object_mut()
                .map(|o| o.entry("stream_options").or_insert(Value::Null));
            if let Some(Value::Null) = so {
                *so.unwrap() = serde_json::json!({ "include_usage": true });
            } else if let Some(v) = so {
                v["include_usage"] = Value::Bool(true);
            }
        }
    }
}

/// Registry of loaded adapters, keyed by provider id.
pub struct Adapters {
    map: std::collections::HashMap<String, std::sync::Arc<dyn ProviderAdapter>>,
}

impl Adapters {
    pub fn build(providers: &std::collections::HashMap<String, ProviderConfig>) -> Self {
        let mut map = std::collections::HashMap::new();
        for (id, cfg) in providers {
            map.insert(
                id.clone(),
                std::sync::Arc::new(OpenAICompatible::new(id.clone(), cfg.clone()))
                    as std::sync::Arc<dyn ProviderAdapter>,
            );
        }
        Adapters { map }
    }

    pub fn get(&self, provider_id: &str) -> Option<std::sync::Arc<dyn ProviderAdapter>> {
        self.map.get(provider_id).cloned()
    }
}
