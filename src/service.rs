use axum::{Json, Router, routing::get};
use serde::Serialize;

#[derive(Serialize)]
struct Health {
    status: &'static str,
}

pub fn router() -> Router {
    Router::new().route("/healthz", get(|| async { Json(Health { status: "ok" }) }))
}
