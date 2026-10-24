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

/// Pre-flight estimate from the request body: prefer the request's declared
/// `max_tokens`, else estimate from body size.
pub fn preflight_estimate(request_bytes: usize, body: &serde_json::Value) -> u64 {
    let req_est = (request_bytes as u64) / BYTES_PER_TOKEN;
    let completion_est = body
        .get("max_tokens")
        .and_then(|m| m.as_u64())
        .unwrap_or(0);
    req_est + completion_est + PER_REQUEST_OVERHEAD_TOKENS
}
