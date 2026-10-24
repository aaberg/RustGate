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
        return err_response(&state, None, &path, body.len(), &e_unauthorized(), started, "no_token").await;
    };
    let token_hash = hex::encode(Sha256::digest(token.as_bytes()));
    let Some(job) = state.store.job_by_hash_async(token_hash).await else {
        return err_response(&state, None, &path, body.len(), &e_unauthorized(), started, "unknown_token").await;
    };

    let job_id = job.job_id.clone();
    let tenant_id = job.tenant_id.clone();
    let request_bytes = body.len();

    // ---- AuthZ / state (per-job lazy expiry; global sweeps are periodic) ----
    if job.status != "active" {
        let reason = format!("status_{}", job.status);
        // Quota exhaustion is a 429 (design: "clear error"); everything
        // else (revoked/expired/done) is a 401.
        let e = if job.status == "exhausted" {
            ApiError::RateLimited("request quota exhausted".into())
        } else {
            e_unauthorized()
        };
        return err_response(&state, Some(&job), &path, request_bytes, &e, started, &reason).await;
    }
    if job.expires_at < Utc::now() {
        state.store.mark_status_async(job_id.clone(), "expired".into(), None).await;
        return err_response(&state, Some(&job), &path, request_bytes, &e_unauthorized(), started, "expired").await;
    }

    // ---- Path allowlist (proxy-wide; adapter confirms) ----
    let adapter: Arc<dyn crate::adapters::ProviderAdapter> = match state.adapters.get(&job.provider_id) {
        Some(a) => a,
        None => {
            let e = ApiError::Internal("no adapter".into());
            return err_response(&state, Some(&job), &path, request_bytes, &e, started, "no_adapter").await;
        }
    };
    if !adapter.forwards(&path) {
        let e = ApiError::Forbidden("path not forwardable".into());
        return err_response(&state, Some(&job), &path, request_bytes, &e, started, "path_not_allowed").await;
    }

    // ---- Body policy: size + secret scan (blocking), injection markers (advisory) ----
    match state.policy.check_body(&body) {
        crate::policy::BodyVerdict::Blocked(reason) => {
            state.metrics.blocked_total.with_label_values(&[reason]).inc();
            let e = ApiError::Forbidden("blocked by policy".into());
            return err_response(&state, Some(&job), &path, request_bytes, &e, started, reason).await;
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
            if let Some(prompt) = state.store.get_system_prompt_async(job_id.clone()).await {
                state.policy.inject_system_prompt(jb, &prompt);
            }
        }
        adapter.transform_request(&path, jb);
    }

    // ---- Quotas ----
    // Estimates use the *forwarded* body size (injected operator prompt
    // included), not the original client body. Atomic reservation prevents
    // concurrent calls from all spending the same remaining budget.
    let est = json_body
        .as_ref()
        .map(|jb| {
            let fwd = serde_json::to_vec(jb).unwrap_or_default().len();
            preflight_estimate(fwd, jb, &path)
        })
        .unwrap_or_else(|| estimate_tokens(request_bytes, 0));
    if state.store.reserve_tokens_async(job_id.clone(), est).await.is_none() {
        let e = ApiError::RateLimited("token quota exhausted".into());
        return err_response(&state, Some(&job), &path, request_bytes, &e, started, "token_cap").await;
    }
    if state.store.reserve_request_async(job_id.clone()).await.is_none() {
        state.store.release_token_reservation_async(job_id.clone(), est).await;
        let e = ApiError::RateLimited("request quota exhausted".into());
        return err_response(&state, Some(&job), &path, request_bytes, &e, started, "request_cap").await;
    }

    // ---- Concurrency bound (reject saturation immediately; never queue) ----
    let permit = match &state.gate {
        Some(sem) => match sem.clone().try_acquire_owned() {
            Ok(p) => Some(p),
            Err(_) => {
                state.store.release_request_async(job_id.clone()).await;
                state.store.release_token_reservation_async(job_id.clone(), est).await;
                let e = ApiError::Unavailable("saturated".into());
                return err_response(&state, Some(&job), &path, request_bytes, &e, started, "saturated").await;
            }
        },
        None => None,
    };
    state.metrics.in_flight.inc();

    // Cancellation-safe accounting guard: if the handler future is dropped
    // while waiting for upstream headers (downstream cancellation), neither
    // forward() branch runs. The guard settles conservatively on drop.
    let mut guard = AccountingGuard {
        state: Some(state.clone()),
        job_id: job_id.clone(),
        est_tokens: est,
        forwarded: false,
        settled: false,
    };

    let result = forward(
        state.clone(),
        job.clone(),
        adapter,
        method,
        headers,
        path.clone(),
        uri.query().map(|q| format!("{path}?{q}")).unwrap_or(path),
        json_body,
        body,
        advisory,
        started,
        permit,
        est,
        &mut guard,
    )
    .await;
    // forward() completed (either branch): the guard must not fire.
    guard.settled = true;

    match result {
        Ok((resp, buffered_done)) => {
            if buffered_done {
                // Buffered response: body fully transferred; balance the
                // in-flight gauge here (streaming responses balance it in
                // the stream's finalizer, including on client drop).
                state.metrics.in_flight.dec();
            }
            resp
        }
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
    path_and_query: String,
    json_body: Option<Value>,
    raw_body: Bytes,
    advisory: bool,
    started: Instant,
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
    est_tokens: u64,
    guard: &mut AccountingGuard,
) -> Result<(Response, bool), Response> {
    let key = state
        .pool
        .get(&job.upstream_key_id)
        .map(|k| k.key.clone())
        .unwrap_or_default();
    let base = adapter.config().base_url.trim_end_matches('/').to_string();
    let url = format!("{base}{path_and_query}");

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
            // The send() call definitively ran: the guard must not settle.
            guard.forwarded = true;
            // Transport failure is ambiguous: the upstream may have accepted
            // the body and burned the call (e.g. connection dropped while
            // awaiting response headers). Only refund when the failure is
            // proven to precede forwarding (connect-phase errors).
            let is_connect_phase = e.is_connect() || e.is_builder();
            if is_connect_phase {
                state.store.release_request_async(job.job_id.clone()).await;
                state.store.release_token_reservation_async(job.job_id.clone(), est_tokens).await;
            } else {
                // Ambiguous: the upstream may have accepted the body and
                // generated a completion before the transport failed. Consume
                // the request slot AND charge the reserved token estimate
                // (conservative; a cost guard must not under-count).
                state.store.consume_request_async(job.job_id.clone()).await;
                let st = state.clone();
                let jid = job.job_id.clone();
                let _ = tokio::task::spawn_blocking(move || {
                    st.store.reconcile_tokens(&jid, est_tokens, est_tokens);
                })
                .await;
            }
            let msg = e.to_string();
            tracing::warn!(job_id = %job.job_id, path = %path, "upstream forward failed: {msg}");
            state.store.log_request_async(LogEntry {
                job_id: job.job_id.clone(),
                ts: Utc::now(),
                path: path.clone(),
                model: model_of(json_body.as_ref()),
                request_bytes: raw_body.len(),
                response_bytes: 0,
                status_code: None,
                duration_ms: Some(started.elapsed().as_millis() as u64),
                prompt_tokens: None,
                completion_tokens: None,
                tokens_estimated: false,
                policy_verdict: "upstream_forward_failed".into(),
                block_reason: None,
            }).await;
            return Err(ApiError::Upstream(msg).into_response());
        }
    };

    // Response headers received: the upstream call burned the request slot,
    // and the request definitively reached the upstream (drop from here on
    // is handled by the stream/buffered finalizers, not the guard).
    guard.forwarded = true;
    state.store.consume_request_async(job.job_id.clone()).await;

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
    // Preserve end-to-end headers (notably Retry-After from upstream
    // 429/503s); hop-by-hop headers never cross the proxy.
    let e2e_headers: Vec<(String, String)> = upstream_resp
        .headers()
        .iter()
        .filter_map(|(n, v)| {
            let n = n.as_str().to_ascii_lowercase();
            let hop = matches!(
                n.as_str(),
                "connection" | "transfer-encoding" | "keep-alive" | "proxy-authenticate" | "proxy-authorization" | "te" | "trailer" | "upgrade" | "content-length"
            );
            if hop {
                None
            } else {
                v.to_str().ok().map(|v| (n, v.to_string()))
            }
        })
        .collect();

    let request_bytes = raw_body.len();
    let finalize = {
        let state = state.clone();
        let job_id = job.job_id.clone();
        let tenant_id = job.tenant_id.clone();
        let key_id = job.upstream_key_id.clone();
        let provider_id = job.provider_id.clone();
        let path = path.clone();
        let model = model_of(json_body.as_ref());
        let fwd_bytes = json_body
            .as_ref()
            .map(|jb| serde_json::to_vec(jb).unwrap_or_default().len())
            .unwrap_or(request_bytes);
        let est = est_tokens;
        move |response_bytes: u64, usage: Option<Usage>| {
            let state = state.clone();
            let (u, estimated) = match usage {
                Some(u) if u.total() > 0 => (u, false),
                _ => (
                    Usage { prompt_tokens: estimate_tokens(fwd_bytes, response_bytes as usize), completion_tokens: 0 },
                    true,
                ),
            };
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
            // Runs from stream Drop (sync) and from buffered completion
            // (async): use the sync store API inside spawn_blocking via a
            // dedicated runtime handle when available, else a fresh runtime.
            let store = state.store.clone();
            let job_id2 = job_id.clone();
            let verdict = if advisory { "allowed_injection_flag" } else { "allowed" };
            let entry = LogEntry {
                job_id: job_id.clone(),
                ts: Utc::now(),
                path: path.clone(),
                model,
                request_bytes,
                response_bytes: response_bytes as usize,
                status_code: Some(status_code),
                duration_ms: Some(started.elapsed().as_millis() as u64),
                prompt_tokens: Some(u.prompt_tokens),
                completion_tokens: Some(u.completion_tokens),
                tokens_estimated: estimated,
                policy_verdict: verdict.to_string(),
                block_reason: None,
            };
            // Finalize may run from an async context (buffered completion)
            // or from stream Drop (client disconnect). Both must offload the
            // blocking SQLite work; Handle::try_current covers Drop-inside-
            // runtime, and spawn_blocking panics outside a runtime, so gate
            // on the handle explicitly. Fire-and-forget: reconciliation is
            // idempotent and the audit row is best-effort on drop.
            if let Ok(h) = tokio::runtime::Handle::try_current() {
                h.spawn(async move {
                    let exhausted = tokio::task::spawn_blocking(move || {
                        let exhausted = store.reconcile_tokens(&job_id2, est, u.total());
                        store.log_request(&entry);
                        exhausted
                    })
                    .await
                    .unwrap_or(false);
                    if exhausted {
                        tracing::info!(job_id = %job_id, "job marked exhausted (token cap)");
                    }
                });
            }
        }
    };

    // Failure finalize for the buffered path: charges the reserved token
    // estimate (the upstream may have completed the generation) and writes
    // the audit row with estimated usage.
    let finalize_failure = {
        let state = state.clone();
        let job_id = job.job_id.clone();
        let est = est_tokens;
        move || {
            // Charge the reserved estimate: the upstream may have completed
            // the generation before the truncation/timeout.
            if let Ok(h) = tokio::runtime::Handle::try_current() {
                h.spawn(async move {
                    let _ = tokio::task::spawn_blocking(move || {
                        state.store.reconcile_tokens(&job_id, est, est);
                    })
                    .await;
                });
            }
        }
    };

    if is_stream {
        // Permit ownership transfers into the stream state: released on EOF,
        // error, idle timeout, or drop (client disconnect).
        let stream = crate::stream_util::usage_stream(
            upstream_resp,
            adapter.clone(),
            finalize,
            state.metrics.clone(),
            permit,
        );
        let mut builder = Response::builder()
            .status(StatusCode::from_u16(status_code).unwrap())
            .header("content-type", upstream_ct)
            .header("cache-control", "no-cache")
            .header("x-accel-buffering", "no");
        for (n, v) in &e2e_headers {
            builder = builder.header(n.as_str(), v.as_str());
        }
        let resp = builder.body(Body::from_stream(stream)).unwrap();
        Ok((resp, false))
    } else {
        // Buffered: the download + finalize run in a spawned task that owns
        // the in-flight balance and the permit, so a downstream cancellation
        // (dropping this handler future mid-download) cannot leak either:
        // the task completes, finalizes, and decrements regardless.
        let adapter2 = adapter.clone();
        let metrics = state.metrics.clone();
        let finalize_failure = finalize_failure;
        let dl = tokio::spawn(async move {
            let res = match tokio::time::timeout(
                std::time::Duration::from_secs(600),
                upstream_resp.bytes(),
            )
            .await
            {
                Ok(Ok(b)) => Ok(b),
                Ok(Err(e)) => Err(e.to_string()),
                Err(_) => Err("upstream read timeout".to_string()),
            };
            match res {
                Ok(b) => {
                    let n = b.len() as u64;
                    let usage = serde_json::from_slice::<Value>(&b)
                        .ok()
                        .and_then(|v| adapter2.extract_usage_response(&v));
                    finalize(n, usage);
                    metrics.in_flight.dec();
                    drop(permit);
                    Ok(b)
                }
                Err(msg) => {
                    // Incomplete buffered response: the upstream may have
                    // burned the full generation before the truncation or
                    // timeout. Charge the reserved estimate (conservative)
                    // instead of refunding it; usage is marked estimated.
                    finalize_failure();
                    metrics.in_flight.dec();
                    drop(permit);
                    Err(msg)
                }
            }
        });
        let body_bytes = match dl.await {
            Ok(Ok(b)) => b,
            // The task already balanced in_flight and released the permit
            // (including for failures); the error path must NOT decrement
            // again in the outer handler — hence Ok(Err) is returned as a
            // normal response, not an Err that triggers the outer dec.
            Ok(Err(msg)) => {
                let e = ApiError::Upstream(msg);
                return Ok((e.into_response(), false));
            }
            Err(e) => {
                // Join error (task panicked/cancelled): the task may not
                // have balanced. Let the outer handler handle the gauge.
                return Err(ApiError::Internal(format!("download task: {e}")).into_response());
            }
        };
        let mut builder = Response::builder()
            .status(StatusCode::from_u16(status_code).unwrap())
            .header("content-type", upstream_ct.clone());
        for (n, v) in &e2e_headers {
            builder = builder.header(n.as_str(), v.as_str());
        }
        // Documented retry guidance for upstream 5xx when absent.
        if StatusCode::from_u16(status_code).unwrap().is_server_error()
            && !e2e_headers.iter().any(|(n, _)| n == "retry-after")
        {
            builder = builder.header("retry-after", "5");
        }
        let resp = builder.body(Body::from(body_bytes)).unwrap();
        // The spawned download task already balanced in_flight and released
        // the permit (including on cancellation), so the caller must not.
        Ok((resp, false))
    }
}

fn model_of(body: Option<&Value>) -> Option<String> {
    body.and_then(|b| b.get("model"))
        .and_then(|m| m.as_str())
        .map(|s| s.to_string())
}

async fn err_response(
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
        state.store.log_request_async(LogEntry {
            job_id: job.job_id.clone(),
            ts: Utc::now(),
            path: path.to_string(),
            model: None,
            request_bytes,
            response_bytes: 0,
            status_code: Some(e.status().as_u16()),
            duration_ms: Some(started.elapsed().as_millis() as u64),
            prompt_tokens: None,
            completion_tokens: None,
            tokens_estimated: false,
            policy_verdict: verdict.to_string(),
            block_reason: Some(reason.clone()),
        }).await;
    }
    (*e).clone().into_response()
}

/// Cancellation-safe accounting for the reservation window between quota
/// reservation and upstream response (or failure). If the handler future is
/// dropped mid-window (downstream cancellation), the guard settles
/// conservatively: consume the request slot (the body may have been sent)
/// and charge the reserved token estimate. Idempotent with the normal paths.
struct AccountingGuard {
    state: Option<SharedState>,
    job_id: String,
    est_tokens: u64,
    forwarded: bool,
    settled: bool,
}

impl Drop for AccountingGuard {
    fn drop(&mut self) {
        if self.settled || self.forwarded || self.state.is_none() {
            return;
        }
        let state = self.state.take().unwrap();
        let job_id = self.job_id.clone();
        let est = self.est_tokens;
        // Fire-and-forget on drop: cannot await; use Handle::try_current
        // and let the reconciliation run as a spawned task.
        if let Ok(h) = tokio::runtime::Handle::try_current() {
            h.spawn(async move {
                state.store.consume_request_async(job_id.clone()).await;
                let st = state.clone();
                let _ = tokio::task::spawn_blocking(move || {
                    st.store.reconcile_tokens(&job_id, est, est);
                })
                .await;
                state.metrics.in_flight.dec();
            });
        }
    }
}
