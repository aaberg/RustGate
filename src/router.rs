use axum::routing::{get, post};
use axum::Router;
use crate::control;
use crate::proxy;
use crate::state::SharedState;

pub fn router(state: SharedState) -> Router {
    Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/metrics", get(metrics_handler))
        .route("/v1/jobs", post(control::issue_job))
        .route("/v1/jobs/{job_id}/revoke", post(control::revoke_job))
        .route("/v1/jobs/{job_id}/done", post(control::done_job))
        .route("/v1/job/me", get(job_me))
        .fallback(proxy::proxy)
        .with_state(state)
}

async fn metrics_handler(axum::extract::State(state): axum::extract::State<SharedState>) -> String {
    state.metrics.encode()
}

async fn job_me(
    axum::extract::State(state): axum::extract::State<SharedState>,
    headers: axum::http::HeaderMap,
) -> impl axum::response::IntoResponse {
    use sha2::Digest;
    let Some(token) = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
    else {
        return (axum::http::StatusCode::UNAUTHORIZED, axum::Json(serde_json::json!({"error": "unauthorized"})));
    };
    let hash = hex::encode(sha2::Sha256::digest(token.as_bytes()));
    match state.store.job_by_hash(&hash) {
        Some(j) => (
            axum::http::StatusCode::OK,
            axum::Json(serde_json::json!({
                "job_id": j.job_id,
                "tenant_id": j.tenant_id,
                "status": j.status,
                "expires_at": j.expires_at.to_rfc3339(),
                "requests": { "used": j.requests_used, "max": j.max_requests },
                "tokens": { "used": j.tokens_used, "max": j.max_tokens_total },
            })),
        ),
        None => (
            axum::http::StatusCode::UNAUTHORIZED,
            axum::Json(serde_json::json!({"error": "unauthorized"})),
        ),
    }
}
