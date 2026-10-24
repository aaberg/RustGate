use crate::adapters::Usage;
use crate::error::ApiError;
use crate::state::SharedState;
use crate::store::LogEntry;
use crate::usage::{estimate_tokens, preflight_estimate};
use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::{HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use chrono::Utc;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::time::Instant;

/// Data-plane entry point for all proxied paths.
/// AuthN → AuthZ/state → path allowlist → body policy → quotas → forward.
pub async fn proxy(
    State(state): State<SharedState>,
    axum::extract::OriginalUri(uri): axum::extract::OriginalUri,
    method: Method,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let started = Instant::now();
    let path = uri.path().to_string();

    // ---- AuthN: hashed lookup ----
    let token = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(|s| s.to_string());
    let Some(token) = token else {
        return err_response(&state, None, &path, body.len(), &e_unauthorized(), started, "no_token");
    };
    let token_hash = hex::encode(Sha256::digest(token.as_bytes()));
    let Some(job) = state.store.job_by_hash(&token_hash) else {
        return err_response(&state, None, &path, body.len(), &e_unauthorized(), started, "unknown_token");
    };

    let job_id = job.job_id.clone();
    let tenant_id = job.tenant_id.clone();
    let request_bytes = body.len();

    // ---- AuthZ / state ----
    state.store.sweep_expired();
    if job.status != "active" {
        let reason = format!("status_{}", job.status);
        // Quota exhaustion is a 429 (design: "clear error"); everything
        // else (revoked/expired/done) is a 401.
        let e = if job.status == "exhausted" {
            ApiError::RateLimited("request quota exhausted".into())
        } else {
            e_unauthorized()
        };
        return err_response(&state, Some(&job), &path, request_bytes, &e, started, &reason);
    }
    if job.expires_at < Utc::now() {
        state.store.mark_status(&job_id, "expired", None);
        return err_response(&state, Some(&job), &path, request_bytes, &e_unauthorized(), started, "expired");
    }

    // ---- Path allowlist (proxy-wide; adapter confirms) ----
    let adapter: Arc<dyn crate::adapters::ProviderAdapter> = match state.adapters.get(&job.provider_id) {
        Some(a) => a,
        None => {
            let e = ApiError::Internal("no adapter".into());
            return err_response(&state, Some(&job), &path, request_bytes, &e, started, "no_adapter");
        }
    };
    if !adapter.forwards(&path) {
        let e = ApiError::Forbidden("path not forwardable".into());
        return err_response(&state, Some(&job), &path, request_bytes, &e, started, "path_not_allowed");
    }

    // ---- Body policy: size + secret scan (blocking), injection markers (advisory) ----
    match state.policy.check_body(&body) {
        crate::policy::BodyVerdict::Blocked(reason) => {
            state.metrics.blocked_total.with_label_values(&[reason]).inc();
            let e = ApiError::Forbidden("blocked by policy".into());
            return err_response(&state, Some(&job), &path, request_bytes, &e, started, reason);
        }
        crate::policy::BodyVerdict::Allowed => {}
    }

    // Parse the body (for injection, quota estimate, usage request).
    let mut json_body: Option<Value> = serde_json::from_slice(&body).ok();
    let mut advisory = false;
    if let Some(jb) = json_body.as_ref() {
        let s = serde_json::to_string(jb).unwrap_or_default();
        advisory = state.policy.scan_injection_markers(&s);
    }
    if advisory {
        tracing::warn!(job_id = %job_id, tenant_id = %tenant_id, path = %path, "prompt-injection marker observed (advisory)");
    }

    // System prompt injection (chat-shaped bodies only).
    if let Some(jb) = json_body.as_mut() {
        if adapter.is_chat_body(&path) {
            if let Some(prompt) = state.store.get_system_prompt(&job_id) {
                state.policy.inject_system_prompt(jb, &prompt);
            }
        }
        adapter.transform_request(&path, jb);
    }

    // ---- Quotas ----
    let est = json_body
        .as_ref()
        .map(|jb| preflight_estimate(request_bytes, jb))
        .unwrap_or_else(|| estimate_tokens(request_bytes, 0));
    if job.tokens_used.saturating_add(est) > job.max_tokens_total {
        state.store.mark_status(&job_id, "exhausted", None);
        let e = ApiError::RateLimited("token quota exhausted".into());
        return err_response(&state, Some(&job), &path, request_bytes, &e, started, "token_cap");
    }
    if state.store.reserve_request(&job_id).is_none() {
        let e = ApiError::RateLimited("request quota exhausted".into());
        return err_response(&state, Some(&job), &path, request_bytes, &e, started, "request_cap");
    }

    // ---- Concurrency bound ----
    let permit = match &state.gate {
        Some(sem) => match sem.clone().acquire_owned().await {
            Ok(p) => Some(p),
            Err(_) => {
                state.store.release_request(&job_id);
                let e = ApiError::Unavailable("saturated".into());
                return err_response(&state, Some(&job), &path, request_bytes, &e, started, "saturated");
            }
        },
        None => None,
    };
    state.metrics.in_flight.inc();

    let result = forward(
        state.clone(),
        job.clone(),
        adapter,
        method,
        headers,
        path.clone(),
        json_body,
        body,
        advisory,
        started,
        permit,
    )
    .await;

    match result {
        Ok(resp) => resp,
        Err(e) => {
            state.metrics.in_flight.dec();
            e
        }
    }
}

fn e_unauthorized() -> ApiError {
    ApiError::Unauthorized
}

async fn forward(
    state: SharedState,
    job: crate::config::JobPolicySnapshot,
    adapter: Arc<dyn crate::adapters::ProviderAdapter>,
    method: Method,
    headers: HeaderMap,
    path: String,
    json_body: Option<Value>,
    raw_body: Bytes,
    advisory: bool,
    started: Instant,
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
) -> Result<Response, Response> {
    let key = state
        .pool
        .get(&job.upstream_key_id)
        .map(|k| k.key.clone())
        .unwrap_or_default();
    let base = adapter.config().base_url.trim_end_matches('/').to_string();
    let url = format!("{base}{path}");

    let mut req = state.upstream.request(method, &url);
    for (name, value) in headers.iter() {
        let n = name.as_str().to_ascii_lowercase();
        if n == "authorization" || n == "host" || n == "content-length" || n == "connection"
            || n == "transfer-encoding" || n == "accept-encoding" {
            continue;
        }
        req = req.header(name.clone(), value.clone());
    }

    let fwd_body: Bytes = match json_body.as_ref() {
        Some(jb) => serde_json::to_vec(jb).unwrap_or_default().into(),
        None => raw_body.clone(),
    };
    let content_type = if json_body.is_some() {
        "application/json".to_string()
    } else {
        headers
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("application/octet-stream")
            .to_string()
    };
    req = req
        .header("authorization", adapter.auth_header(&key))
        .header("content-type", content_type);

    let upstream_resp = match req.body(fwd_body).send().await {
        Ok(r) => r,
        Err(e) => {
            // Failed before forwarding: release the reservation.
            state.store.release_request(&job.job_id);
            let msg = e.to_string();
            tracing::warn!(job_id = %job.job_id, path = %path, "upstream connect failed: {msg}");
            state.store.log_request(&LogEntry {
                job_id: &job.job_id,
                ts: Utc::now(),
                path: &path,
                model: model_of(json_body.as_ref()),
                request_bytes: raw_body.len(),
                response_bytes: 0,
                status_code: None,
                duration_ms: Some(started.elapsed().as_millis() as u64),
                prompt_tokens: None,
                completion_tokens: None,
                tokens_estimated: false,
                policy_verdict: "upstream_connect_failed",
                block_reason: None,
            });
            return Err(ApiError::Upstream(msg).into_response());
        }
    };

    let status = upstream_resp.status();
    if status.is_server_error() {
        state
            .metrics
            .upstream_errors_total
            .with_label_values(&[&format!("{}xx", status.as_u16() / 100)])
            .inc();
    }
    let status_code = status.as_u16();
    let upstream_ct = upstream_resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/json")
        .to_string();
    let is_stream = upstream_ct.contains("text/event-stream");

    let request_bytes = raw_body.len();
    let finalize = {
        let state = state.clone();
        let job_id = job.job_id.clone();
        let tenant_id = job.tenant_id.clone();
        let key_id = job.upstream_key_id.clone();
        let provider_id = job.provider_id.clone();
        let path = path.clone();
        let model = model_of(json_body.as_ref());
        move |response_bytes: u64, usage: Option<Usage>| {
            let (u, estimated) = match usage {
                Some(u) if u.total() > 0 => (u, false),
                _ => (
                    Usage { prompt_tokens: estimate_tokens(request_bytes, response_bytes as usize), completion_tokens: 0 },
                    true,
                ),
            };
            let exhausted = state.store.add_tokens(&job_id, u.total());
            if exhausted {
                tracing::info!(job_id = %job_id, "job marked exhausted (token cap)");
            }
            state
                .metrics
                .tokens_total
                .with_label_values(&[&key_id, &provider_id])
                .inc_by(u.total() as f64);
            state
                .metrics
                .requests_total
                .with_label_values(&[&job_id, &tenant_id, "allowed"])
                .inc();
            state.store.log_request(&LogEntry {
                job_id: &job_id,
                ts: Utc::now(),
                path: &path,
                model,
                request_bytes,
                response_bytes: response_bytes as usize,
                status_code: Some(status_code),
                duration_ms: Some(started.elapsed().as_millis() as u64),
                prompt_tokens: Some(u.prompt_tokens),
                completion_tokens: Some(u.completion_tokens),
                tokens_estimated: estimated,
                policy_verdict: if advisory { "allowed_injection_flag" } else { "allowed" },
                block_reason: None,
            });
        }
    };

    if is_stream {
        let stream = crate::stream_util::usage_stream(
            upstream_resp,
            adapter.clone(),
            finalize,
            state.metrics.clone(),
            permit,
        );
        let resp = Response::builder()
            .status(StatusCode::from_u16(status_code).unwrap())
            .header("content-type", upstream_ct)
            .header("cache-control", "no-cache")
            .header("x-accel-buffering", "no")
            .body(Body::from_stream(stream))
            .unwrap();
        Ok(resp)
    } else {
        drop(permit);
        // Buffered: read whole body, extract usage, finalize, respond.
        let body_bytes = match tokio::time::timeout(
            std::time::Duration::from_secs(600),
            upstream_resp.bytes(),
        )
        .await
        {
            Ok(Ok(b)) => b,
            Ok(Err(e)) => {
                finalize(0, None);
                return Err(ApiError::Upstream(e.to_string()).into_response());
            }
            Err(_) => {
                finalize(0, None);
                return Err(ApiError::Upstream("upstream read timeout".into()).into_response());
            }
        };
        let n = body_bytes.len() as u64;
        let usage = serde_json::from_slice::<Value>(&body_bytes)
            .ok()
            .and_then(|v| adapter.extract_usage_response(&v));
        finalize(n, usage);
        let resp = Response::builder()
            .status(StatusCode::from_u16(status_code).unwrap())
            .header("content-type", upstream_ct)
            .body(Body::from(body_bytes))
            .unwrap();
        Ok(resp)
    }
}

fn model_of(body: Option<&Value>) -> Option<String> {
    body.and_then(|b| b.get("model"))
        .and_then(|m| m.as_str())
        .map(|s| s.to_string())
}

fn err_response(
    state: &SharedState,
    job: Option<&crate::config::JobPolicySnapshot>,
    path: &str,
    request_bytes: usize,
    e: &ApiError,
    started: Instant,
    reason: &str,
) -> Response {
    let reason = reason.to_string();
    let verdict = match e.status() {
        StatusCode::UNAUTHORIZED => "blocked_auth",
        StatusCode::FORBIDDEN => "blocked_policy",
        StatusCode::TOO_MANY_REQUESTS => "blocked_quota",
        StatusCode::SERVICE_UNAVAILABLE => "blocked_saturated",
        _ => "blocked_other",
    };
    if let Some(job) = job {
        state
            .metrics
            .requests_total
            .with_label_values(&[&job.job_id, &job.tenant_id, verdict])
            .inc();
        state.store.log_request(&LogEntry {
            job_id: &job.job_id,
            ts: Utc::now(),
            path,
            model: None,
            request_bytes,
            response_bytes: 0,
            status_code: Some(e.status().as_u16()),
            duration_ms: Some(started.elapsed().as_millis() as u64),
            prompt_tokens: None,
            completion_tokens: None,
            tokens_estimated: false,
            policy_verdict: verdict,
            block_reason: Some(&reason),
        });
    }
    (*e).clone().into_response()
}
