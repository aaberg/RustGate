use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::json;

/// Proxy-facing API errors. Messages are deliberately generic on the data
/// plane to avoid detail leakage; the store/log gets the real reason.
#[derive(Debug, Clone, thiserror::Error)]
pub enum ApiError {
    #[error("unauthorized")]
    Unauthorized,
    #[error("forbidden")]
    Forbidden(String),
    #[error("not found")]
    NotFound,
    #[error("bad request: {0}")]
    BadRequest(String),
    #[error("rate limited")]
    RateLimited(String),
    #[error("service unavailable")]
    Unavailable(String),
    #[error("upstream error: {0}")]
    Upstream(String),
    #[error("internal: {0}")]
    Internal(String),
}

impl ApiError {
    pub fn status(&self) -> StatusCode {
        match self {
            ApiError::Unauthorized => StatusCode::UNAUTHORIZED,
            ApiError::Forbidden(_) => StatusCode::FORBIDDEN,
            ApiError::NotFound => StatusCode::NOT_FOUND,
            ApiError::BadRequest(_) => StatusCode::BAD_REQUEST,
            ApiError::RateLimited(_) => StatusCode::TOO_MANY_REQUESTS,
            ApiError::Unavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
            ApiError::Upstream(_) => StatusCode::BAD_GATEWAY,
            ApiError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    /// The message the client sees. Data-plane reasons stay generic.
    pub fn public_message(&self) -> String {
        match self {
            ApiError::Unauthorized => "unauthorized".into(),
            ApiError::Forbidden(_) => "forbidden".into(),
            ApiError::NotFound => "not found".into(),
            ApiError::RateLimited(_) => "rate limited".into(),
            ApiError::Unavailable(_) => "service unavailable, retry later".into(),
            _ => self.to_string(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = axum::Json(json!({
            "error": { "message": self.public_message(), "type": self.status().canonical_reason().unwrap_or("error") }
        }));
        // Upstream 5xx passes through with retry-after guidance.
        let mut resp = (self.status(), body).into_response();
        if matches!(self, ApiError::Upstream(_) | ApiError::Unavailable(_)) {
            if let Ok(v) = "5".parse() {
                resp.headers_mut().insert("retry-after", v);
            }
        }
        resp
    }
}
