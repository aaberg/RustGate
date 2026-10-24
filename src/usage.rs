/// Token accounting for streams that never deliver a usage chunk.
/// ~4 bytes/token is a reasonable upper bound for English+JSON; we add a
/// fixed per-request overhead so the estimate errs high (a cost guard must
/// not under-count).
pub const BYTES_PER_TOKEN: u64 = 4;
pub const PER_REQUEST_OVERHEAD_TOKENS: u64 = 16;

pub fn estimate_tokens(request_bytes: usize, response_bytes: usize) -> u64 {
    let req = (request_bytes as u64) / BYTES_PER_TOKEN;
    let resp = (response_bytes as u64) / BYTES_PER_TOKEN;
    req + resp + PER_REQUEST_OVERHEAD_TOKENS
}

/// Conservative output allowance when the request declares no completion
/// limit: the upstream can still generate a full completion, so the pre-flight
/// gate must not assume zero output tokens.
pub const DEFAULT_COMPLETION_ALLOWANCE: u64 = 4096;

/// Pre-flight estimate from the request body. Honors `max_completion_tokens`
/// (current OpenAI-compatible field) before the legacy `max_tokens`; when
/// neither is declared, applies a conservative completion allowance so a job
/// with a small remaining budget doesn't authorize an unbounded generation.
/// All arithmetic saturates — a declared limit of u64::MAX must not wrap the
/// estimate small enough to pass the quota gate.
/// Count token IDs in an embeddings `input`: a flat array of IDs, a batch
/// (array of arrays), or non-array content (None — byte estimate applies).
fn embedding_input_ids(input: &serde_json::Value) -> Option<u64> {
    match input {
        serde_json::Value::Array(items) => {
            let mut total = 0u64;
            for item in items {
                match item {
                    serde_json::Value::Number(_) => total += 1,
                    serde_json::Value::Array(inner) => total += inner.len() as u64,
                    _ => return None,
                }
            }
            Some(total)
        }
        _ => None,
    }
}

pub fn preflight_estimate(request_bytes: usize, body: &serde_json::Value, path: &str) -> u64 {
    let req_est = (request_bytes as u64) / BYTES_PER_TOKEN;
    if path == "/v1/embeddings" {
        // Embeddings generate no completion tokens: input-only estimate.
        // Token-ID arrays are counted directly: 1,000 one-digit IDs serialize
        // to only ~527 bytes/4 tokens, which would under-reserve vs. the
        // explicit token count the upstream actually bills.
        let id_count = body
            .get("input")
            .and_then(embedding_input_ids)
            .unwrap_or(0);
        return req_est
            .max(id_count)
            .saturating_add(PER_REQUEST_OVERHEAD_TOKENS);
    }
    let declared = body
        .get("max_completion_tokens")
        .and_then(|m| m.as_u64())
        .or_else(|| body.get("max_tokens").and_then(|m| m.as_u64()));
    let completion_per_choice = declared.unwrap_or(DEFAULT_COMPLETION_ALLOWANCE);
    // The completion limit is per choice; `n` multiplies the authorized output.
    let choices = body.get("n").and_then(|m| m.as_u64()).unwrap_or(1).max(1);
    let completion_est = completion_per_choice.saturating_mul(choices);
    req_est.saturating_add(completion_est).saturating_add(PER_REQUEST_OVERHEAD_TOKENS)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn u64_max_does_not_wrap() {
        let body = json!({"max_tokens": u64::MAX});
        assert_eq!(preflight_estimate(100, &body, "/v1/chat/completions"), u64::MAX);
    }

    #[test]
    fn max_completion_tokens_preferred() {
        let body = json!({"max_completion_tokens": 1000});
        let est = preflight_estimate(400, &body, "/v1/chat/completions");
        assert_eq!(est, 100 + 1000 + PER_REQUEST_OVERHEAD_TOKENS);
    }

    #[test]
    fn no_limit_gets_default_allowance() {
        let body = json!({"messages": []});
        let est = preflight_estimate(400, &body, "/v1/chat/completions");
        assert_eq!(est, 100 + DEFAULT_COMPLETION_ALLOWANCE + PER_REQUEST_OVERHEAD_TOKENS);
    }
    #[test]
    fn n_multiplies_completion_estimate() {
        let body = json!({"max_tokens": 1000, "n": 10});
        let est = preflight_estimate(400, &body, "/v1/chat/completions");
        assert_eq!(est, 100 + 10_000 + PER_REQUEST_OVERHEAD_TOKENS);
    }

    #[test]
    fn embedding_token_id_array_counted_directly() {
        // 1000 one-digit IDs: bytes/4 would estimate far fewer than 1000.
        let ids: Vec<u32> = (0..1000).collect();
        let body = json!({"model": "text-embedding", "input": ids});
        let est = preflight_estimate(400, &body, "/v1/embeddings");
        assert!(est >= 1000, "estimate {est} must cover the explicit token count");
    }

    #[test]
    fn embedding_batched_id_arrays_counted() {
        let body = json!({"input": [[1, 2, 3], [4, 5, 6, 7]]});
        let est = preflight_estimate(200, &body, "/v1/embeddings");
        assert!(est >= 7, "estimate {est} must cover the batched token count");
    }

    #[test]
    fn embeddings_get_input_only_estimate() {
        let body = json!({"model": "text-embedding", "input": "hello"});
        let est = preflight_estimate(400, &body, "/v1/embeddings");
        assert_eq!(est, 100 + PER_REQUEST_OVERHEAD_TOKENS);
    }
}
