use crate::error::ApiError;
use crate::state::SharedState;
use crate::store::NewJob;
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::Json;
use chrono::{Duration, Utc};
use rand::RngCore;
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

/// Control-plane auth: shared static bearer token (over the private network
/// or mTLS-terminated). Distinct from job tokens.
pub fn check_control_auth(headers: &HeaderMap, state: &SharedState) -> Result<(), ApiError> {
    let expected = state
        .cfg
        .control_token
        .as_deref()
        .filter(|token| !token.trim().is_empty())
        .ok_or(ApiError::Unauthorized)?;
    // A blank configured credential must never authorize: an empty bearer
    // would trivially "match" an empty token in constant-time comparison.
    if expected.is_empty() {
        tracing::error!("control_token configured as empty; rejecting control-plane request");
        return Err(ApiError::Unauthorized);
    }
    let got = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .ok_or(ApiError::Unauthorized)?;
    if got.is_empty() {
        return Err(ApiError::Unauthorized);
    }
    // constant-time compare
    if subtle::ConstantTimeEq::ct_eq(got.as_bytes(), expected.as_bytes()).into() {
        Ok(())
    } else {
        Err(ApiError::Unauthorized)
    }
}

#[derive(Deserialize)]
pub struct IssueRequest {
    pub job_id: Option<String>,
    pub tenant_id: String,
    pub upstream_key_id: String,
    pub ttl_seconds: Option<u64>,
    pub max_requests: Option<u64>,
    pub max_tokens_total: Option<u64>,
    pub system_prompt: Option<String>,
}

/// POST /v1/jobs — issue a per-job token. The proxy generates the raw token
/// (32 bytes, CSPRNG), returns it once, stores only its SHA-256 hash.
pub async fn issue_job(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Json(req): Json<IssueRequest>,
) -> Result<Json<Value>, ApiError> {
    check_control_auth(&headers, &state)?;

    // Resolve key -> provider; unknown key ids fail loudly.
    let key = state
        .pool
        .get(&req.upstream_key_id)
        .ok_or_else(|| ApiError::BadRequest(format!(
            "unknown upstream_key_id {:?}",
            req.upstream_key_id
        )))?;
    let provider = state
        .adapters
        .get(&key.provider_id)
        .ok_or_else(|| ApiError::BadRequest(format!("unknown provider {:?}", key.provider_id)))?;

    let job_id = req.job_id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    if state.store.job_by_id(&job_id).is_some() {
        return Err(ApiError::BadRequest("job_id already exists".into()));
    }

    let ttl = req
        .ttl_seconds
        .unwrap_or(state.cfg.default_ttl_seconds)
        .min(state.cfg.max_ttl_seconds);
    let max_requests = req
        .max_requests
        .unwrap_or(state.cfg.default_max_requests)
        .min(state.cfg.max_max_requests);
    let max_tokens_total = req
        .max_tokens_total
        .unwrap_or(state.cfg.default_max_tokens_total)
        .min(state.cfg.max_max_tokens_total);

    let mut raw = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut raw);
    let token = format!("rg_{}", hex::encode(raw));
    let token_hash = hex::encode(Sha256::digest(token.as_bytes()));

    let now = Utc::now();
    state
        .store
        .create_job(&NewJob {
            job_id: job_id.clone(),
            tenant_id: req.tenant_id.clone(),
            provider_id: key.provider_id.clone(),
            upstream_key_id: req.upstream_key_id.clone(),
            token_hash,
            system_prompt: req.system_prompt.clone(),
            created_at: now,
            expires_at: now + Duration::seconds(ttl as i64),
            max_requests,
            max_tokens_total,
            status: "active".into(),
        })
        .map_err(|e| ApiError::Internal(e.to_string()))?;

    tracing::info!(
        job_id = %job_id,
        tenant_id = %req.tenant_id,
        provider = provider.id(),
        upstream_key_id = %req.upstream_key_id,
        "job issued"
    );

    let proxy_url = if let Some(base) = &state.cfg.public_base_url {
        base.clone()
    } else {
        let scheme = if state.cfg.allow_insecure && state.cfg.tls_cert_path.is_none() {
            "http"
        } else {
            "https"
        };
        format!("{}://{}:{}/v1", scheme, state.cfg.listen_addr, state.cfg.listen_port)
    };
    Ok(Json(json!({
        "job_id": job_id,
        "token": token,
        "expires_at": (now + Duration::seconds(ttl as i64)).to_rfc3339(),
        "proxy_url": proxy_url
    })))
}

#[derive(Deserialize)]
pub struct RevokeRequest {
    pub tenant_id: Option<String>,
    pub reason: Option<String>,
}

/// POST /v1/jobs/{job_id}/revoke — immediate kill switch.
/// In-flight requests complete; new requests get 401.
pub async fn revoke_job(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Path(job_id): Path<String>,
    body: Option<Json<RevokeRequest>>,
) -> Result<Json<Value>, ApiError> {
    check_control_auth(&headers, &state)?;
    let job = state
        .store
        .job_by_id(&job_id)
        .ok_or(ApiError::NotFound)?;
    // Shared-token model: builders may only revoke their own tenant's jobs.
    if let Some(Json(RevokeRequest { tenant_id: Some(t), .. })) = &body {
        if t != &job.tenant_id {
            return Err(ApiError::Forbidden("tenant mismatch".into()));
        }
    }
    let ok = state
        .store
        .mark_status(&job_id, "revoked", body.as_ref().and_then(|Json(b)| b.reason.as_deref()));
    tracing::info!(job_id = %job_id, ok, "job revoked");
    Ok(Json(json!({ "job_id": job_id, "revoked": ok })))
}

/// POST /v1/jobs/{job_id}/done — job finished normally.
pub async fn done_job(
    State(state): State<SharedState>,
    headers: HeaderMap,
    Path(job_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    check_control_auth(&headers, &state)?;
    let _job = state
        .store
        .job_by_id(&job_id)
        .ok_or(ApiError::NotFound)?;
    let ok = state.store.mark_status(&job_id, "done", None);
    tracing::info!(job_id = %job_id, ok, "job done");
    Ok(Json(json!({ "job_id": job_id, "done": ok })))
}
