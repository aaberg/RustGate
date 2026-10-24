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
pub fn preflight_estimate(request_bytes: usize, body: &serde_json::Value) -> u64 {
    let req_est = (request_bytes as u64) / BYTES_PER_TOKEN;
    let declared = body
        .get("max_completion_tokens")
        .and_then(|m| m.as_u64())
        .or_else(|| body.get("max_tokens").and_then(|m| m.as_u64()));
    let completion_est = declared.unwrap_or(DEFAULT_COMPLETION_ALLOWANCE);
    req_est.saturating_add(completion_est).saturating_add(PER_REQUEST_OVERHEAD_TOKENS)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn u64_max_does_not_wrap() {
        let body = json!({"max_tokens": u64::MAX});
        assert_eq!(preflight_estimate(100, &body), u64::MAX);
    }

    #[test]
    fn max_completion_tokens_preferred() {
        let body = json!({"max_completion_tokens": 1000});
        let est = preflight_estimate(400, &body);
        assert_eq!(est, 100 + 1000 + PER_REQUEST_OVERHEAD_TOKENS);
    }

    #[test]
    fn no_limit_gets_default_allowance() {
        let body = json!({"messages": []});
        let est = preflight_estimate(400, &body);
        assert_eq!(est, 100 + DEFAULT_COMPLETION_ALLOWANCE + PER_REQUEST_OVERHEAD_TOKENS);
    }
}
