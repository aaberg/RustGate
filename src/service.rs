use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::Context;
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, FromRequest, Request, State, rejection::JsonRejection},
    http::{HeaderMap, Method, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use rand::TryRngCore;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use uuid::Uuid;

use crate::{
    config::Config,
    store::{InsertError, NewJob, Store},
};

const MAX_BODY_BYTES: usize = 1024 * 1024;

struct ServiceState {
    config: Config,
    store: Store,
    control_token_hash: [u8; 32],
    upstream: reqwest::Client,
}

#[derive(Serialize)]
struct Health {
    status: &'static str,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct IssueJob {
    job_id: String,
    tenant_id: String,
    upstream_key_id: String,
    system_prompt: String,
    ttl_seconds: Option<u64>,
    max_requests: Option<u64>,
    max_tokens_total: Option<u64>,
}

#[derive(Serialize)]
struct IssuedJob<'a> {
    job_id: String,
    token: String,
    expires_at: String,
    proxy_url: &'a str,
}

pub fn router(config: Config, store: Store) -> anyhow::Result<Router> {
    let upstream = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        // Egress is selected only by configuration, never environment proxies.
        .no_proxy()
        .build()
        .context("could not initialize upstream HTTP client")?;
    let state = Arc::new(ServiceState {
        control_token_hash: Sha256::digest(config.control_plane_token.as_bytes()).into(),
        config,
        store,
        upstream,
    });
    let issuance = Router::new()
        .route("/v1/jobs", post(issue_job))
        // Authentication wraps extraction, so unauthenticated bodies are never parsed.
        .route_layer(middleware::from_fn_with_state(state.clone(), authenticate))
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES));
    Ok(Router::new()
        .route("/healthz", get(|| async { Json(Health { status: "ok" }) }))
        .merge(issuance)
        .fallback(proxy_chat)
        .method_not_allowed_fallback(proxy_chat)
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
        .with_state(state))
}

fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    let mut values = headers.get_all(header::AUTHORIZATION).iter();
    let value = values.next()?;
    if values.next().is_some() {
        return None;
    }
    let (scheme, token) = value.to_str().ok()?.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("Bearer")
        || token.is_empty()
        || token.bytes().any(|byte| byte.is_ascii_whitespace())
    {
        return None;
    }
    Some(token)
}

async fn authenticate(
    State(state): State<Arc<ServiceState>>,
    request: Request,
    next: Next,
) -> Response {
    let authorized = bearer_token(request.headers())
        .map(|token| {
            let supplied: [u8; 32] = Sha256::digest(token.as_bytes()).into();
            bool::from(supplied.ct_eq(&state.control_token_hash))
        })
        .unwrap_or(false);
    let mut response = if authorized {
        next.run(request).await
    } else {
        let mut response = error(StatusCode::UNAUTHORIZED, "Unauthorized");
        response.headers_mut().insert(
            header::WWW_AUTHENTICATE,
            header::HeaderValue::from_static("Bearer"),
        );
        response
    };
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        header::HeaderValue::from_static("no-store"),
    );
    response
}

fn error(status: StatusCode, message: &'static str) -> Response {
    (
        status,
        [(header::CACHE_CONTROL, "no-store")],
        Json(json!({"error": {"message": message}})),
    )
        .into_response()
}

fn unauthorized() -> Response {
    let mut response = error(StatusCode::UNAUTHORIZED, "Unauthorized");
    response.headers_mut().insert(
        header::WWW_AUTHENTICATE,
        header::HeaderValue::from_static("Bearer"),
    );
    response
}

async fn proxy_chat(State(state): State<Arc<ServiceState>>, request: Request) -> Response {
    let Some(token) = bearer_token(request.headers()) else {
        return unauthorized();
    };
    let token_hash: [u8; 32] = Sha256::digest(token.as_bytes()).into();
    let Ok(now) = SystemTime::now().duration_since(UNIX_EPOCH) else {
        return error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Could not authenticate job",
        );
    };
    let Ok(now) = i64::try_from(now.as_secs()) else {
        return error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Could not authenticate job",
        );
    };
    let store = state.store.clone();
    let key_id =
        match tokio::task::spawn_blocking(move || store.active_job_key(token_hash, now)).await {
            Ok(Ok(Some(key_id))) => key_id,
            Ok(Ok(None)) => return unauthorized(),
            Ok(Err(_)) | Err(_) => {
                return error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Could not authenticate job",
                );
            }
        };
    if request.method() != Method::POST || request.uri().path() != "/v1/chat/completions" {
        return error(StatusCode::FORBIDDEN, "Forbidden route");
    }
    let Some(key) = state.config.upstream_keys.get(&key_id) else {
        // A persisted job may reference a key removed from the startup pool.
        return unauthorized();
    };
    let mut url = state.config.upstream_base_url.clone();
    url.set_path(&format!(
        "{}/v1/chat/completions",
        url.path().trim_end_matches('/')
    ));
    url.set_query(request.uri().query());

    // Authentication and route checks precede body reads. Reserialization gives
    // the proxy and upstream identical JSON semantics, including duplicate keys.
    let body = match Json::<Value>::from_request(request, &state).await {
        Ok(Json(body)) => body,
        Err(rejection) => {
            let status = if rejection.status() == StatusCode::PAYLOAD_TOO_LARGE {
                StatusCode::PAYLOAD_TOO_LARGE
            } else {
                StatusCode::BAD_REQUEST
            };
            return error(status, "Invalid JSON request");
        }
    };
    if !body.is_object()
        || body
            .get("stream")
            .is_some_and(|stream| stream != &Value::Bool(false))
    {
        return error(
            StatusCode::BAD_REQUEST,
            "Only non-streaming chat is supported",
        );
    }

    // Start a fresh request: no client credentials, cookies, routing headers,
    // or hop-by-hop headers cross the upstream trust boundary.
    let response = match state
        .upstream
        .post(url)
        .bearer_auth(key)
        .json(&body)
        .send()
        .await
    {
        Ok(response) => response,
        Err(_) => return error(StatusCode::BAD_GATEWAY, "Upstream request failed"),
    };
    let status = response.status();
    let mut headers = HeaderMap::new();
    for name in [
        header::CONTENT_TYPE,
        header::CONTENT_ENCODING,
        header::RETRY_AFTER,
    ] {
        for value in response.headers().get_all(&name) {
            headers.append(name.clone(), value.clone());
        }
    }
    let body = match response.bytes().await {
        Ok(body) => body,
        Err(_) => return error(StatusCode::BAD_GATEWAY, "Upstream request failed"),
    };
    headers.insert(
        header::CACHE_CONTROL,
        header::HeaderValue::from_static("no-store"),
    );
    (status, headers, body).into_response()
}

fn positive_limit(value: Option<u64>, default: u64, maximum: u64) -> Option<i64> {
    let value = value.unwrap_or(default);
    if value == 0 || value > maximum {
        return None;
    }
    i64::try_from(value).ok()
}

async fn issue_job(
    State(state): State<Arc<ServiceState>>,
    body: Result<Json<IssueJob>, JsonRejection>,
) -> Response {
    let Json(body) = match body {
        Ok(body) => body,
        Err(rejection) => {
            let status = if rejection.status() == StatusCode::PAYLOAD_TOO_LARGE {
                StatusCode::PAYLOAD_TOO_LARGE
            } else {
                StatusCode::BAD_REQUEST
            };
            return error(status, "Invalid JSON request");
        }
    };
    let Ok(job_id) = Uuid::parse_str(&body.job_id) else {
        return error(StatusCode::BAD_REQUEST, "Invalid job request");
    };
    if body.tenant_id.trim().is_empty()
        || body.tenant_id.len() > 256
        || body.upstream_key_id.trim().is_empty()
        || body.upstream_key_id.len() > 128
        || body.system_prompt.trim().is_empty()
        || !state
            .config
            .upstream_keys
            .contains_key(&body.upstream_key_id)
    {
        return error(StatusCode::BAD_REQUEST, "Invalid job request");
    }
    let Some(ttl_seconds) = positive_limit(
        body.ttl_seconds,
        state.config.default_ttl_seconds,
        state.config.max_ttl_seconds,
    ) else {
        return error(StatusCode::BAD_REQUEST, "Invalid job request");
    };
    let Some(max_requests) = positive_limit(
        body.max_requests,
        state.config.default_max_requests,
        state.config.max_requests,
    ) else {
        return error(StatusCode::BAD_REQUEST, "Invalid job request");
    };
    let Some(max_tokens_total) = positive_limit(
        body.max_tokens_total,
        state.config.default_max_tokens_total,
        state.config.max_tokens_total,
    ) else {
        return error(StatusCode::BAD_REQUEST, "Invalid job request");
    };
    let Some(created_at) = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_secs()).ok())
    else {
        return error(StatusCode::INTERNAL_SERVER_ERROR, "Could not issue job");
    };
    let Some(expires_at) = created_at.checked_add(ttl_seconds) else {
        return error(StatusCode::BAD_REQUEST, "Invalid job request");
    };
    let Ok(expires_at_text) = OffsetDateTime::from_unix_timestamp(expires_at) else {
        return error(StatusCode::BAD_REQUEST, "Invalid job request");
    };
    let Ok(expires_at_text) = expires_at_text.format(&Rfc3339) else {
        return error(StatusCode::BAD_REQUEST, "Invalid job request");
    };

    let mut random = [0_u8; 32];
    if rand::rngs::OsRng.try_fill_bytes(&mut random).is_err() {
        return error(StatusCode::INTERNAL_SERVER_ERROR, "Could not issue job");
    }
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut token = String::with_capacity(64);
    for byte in random {
        token.push(HEX[usize::from(byte >> 4)] as char);
        token.push(HEX[usize::from(byte & 15)] as char);
    }
    let job_id = job_id.to_string();
    let job = NewJob {
        job_id: job_id.clone(),
        tenant_id: body.tenant_id,
        upstream_key_id: body.upstream_key_id,
        system_prompt: body.system_prompt,
        token_hash: Sha256::digest(token.as_bytes()).into(),
        created_at,
        expires_at,
        max_requests,
        max_tokens_total,
    };
    let store = state.store.clone();
    match tokio::task::spawn_blocking(move || store.insert_job(job)).await {
        Ok(Ok(())) => (
            StatusCode::CREATED,
            [(header::CACHE_CONTROL, "no-store")],
            Json(IssuedJob {
                job_id,
                token,
                expires_at: expires_at_text,
                proxy_url: state.config.proxy_url.as_str(),
            }),
        )
            .into_response(),
        Ok(Err(InsertError::DuplicateJob)) => error(StatusCode::CONFLICT, "Job already exists"),
        Ok(Err(InsertError::Storage)) | Err(_) => {
            error(StatusCode::INTERNAL_SERVER_ERROR, "Could not issue job")
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, path::PathBuf};

    use axum::{
        body::{Body, to_bytes},
        http::Request,
    };
    use rusqlite::Connection;
    use serde_json::Value;
    use tower::ServiceExt;

    use super::*;

    mod proxy_tests;

    const CONTROL_TOKEN: &str = "private-control-credential";
    const UPSTREAM_SECRET: &str = "private-upstream-key";
    const JOB_ID: &str = "a6e024df-6080-411e-b0c2-5a0a94582112";
    const OTHER_JOB_ID: &str = "bd72ba7c-1ec5-49b6-843f-526d8a49afde";

    struct Fixture {
        _directory: tempfile::TempDir,
        path: PathBuf,
        app: Router,
    }

    fn config(path: PathBuf) -> Config {
        Config {
            listen_addr: "127.0.0.1:8443".parse().unwrap(),
            upstream_base_url: "http://127.0.0.1:9090".parse().unwrap(),
            tls_cert_path: PathBuf::from("unused-test-cert.pem"),
            tls_key_path: PathBuf::from("unused-test-key.pem"),
            database_path: path,
            control_plane_token: CONTROL_TOKEN.to_owned(),
            upstream_keys: HashMap::from([("pool-a".to_owned(), UPSTREAM_SECRET.to_owned())]),
            proxy_url: "https://proxy.internal:8443/v1".parse().unwrap(),
            default_ttl_seconds: 1800,
            max_ttl_seconds: 86400,
            default_max_requests: 100,
            max_requests: 10000,
            default_max_tokens_total: 2000000,
            max_tokens_total: 100000000,
        }
    }

    impl Fixture {
        fn new() -> Self {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("jobs.sqlite3");
            let store = Store::open(&path).unwrap();
            let app = router(config(path.clone()), store).unwrap();
            Self {
                _directory: directory,
                path,
                app,
            }
        }

        fn count(&self) -> i64 {
            Connection::open(&self.path)
                .unwrap()
                .query_row("SELECT count(*) FROM jobs", [], |row| row.get(0))
                .unwrap()
        }
    }

    fn valid_body() -> Value {
        json!({
            "job_id": JOB_ID,
            "tenant_id": "tenant-a",
            "upstream_key_id": "pool-a",
            "system_prompt": "  You are the authoritative system prompt.\n"
        })
    }

    fn request(body: impl Into<Body>, token: Option<&str>) -> Request<Body> {
        let mut request = Request::builder()
            .method("POST")
            .uri("/v1/jobs")
            .header(header::CONTENT_TYPE, "application/json");
        if let Some(token) = token {
            request = request.header(header::AUTHORIZATION, format!("Bearer {token}"));
        }
        request.body(body.into()).unwrap()
    }

    async fn send(app: Router, request: Request<Body>) -> (StatusCode, HeaderMap, Value) {
        let response = app.oneshot(request).await.unwrap();
        let status = response.status();
        let headers = response.headers().clone();
        let bytes = to_bytes(response.into_body(), MAX_BODY_BYTES * 2)
            .await
            .unwrap();
        let body = serde_json::from_slice(&bytes).unwrap();
        (status, headers, body)
    }

    async fn issue(app: Router, body: Value) -> (StatusCode, HeaderMap, Value) {
        send(app, request(body.to_string(), Some(CONTROL_TOKEN))).await
    }

    fn assert_private_error(body: &Value) {
        assert!(body["error"]["message"].is_string());
        assert!(body.get("token").is_none());
        let encoded = body.to_string();
        for secret in [
            CONTROL_TOKEN,
            UPSTREAM_SECRET,
            "secret-invalid-uuid",
            "unknown-secret-pool",
            "secret-value",
            "unexpected-secret-field",
            "no such table",
            "DROP TABLE",
            "SELECT",
        ] {
            assert!(!encoded.contains(secret));
        }
    }

    fn persisted(path: &std::path::Path) -> Value {
        Connection::open(path)
            .unwrap()
            .query_row(
                "SELECT job_id, tenant_id, upstream_key_id, system_prompt, token_hash,
                created_at, expires_at, max_requests, requests_used, max_tokens_total,
                tokens_used, status, revoked_at, revoked_reason FROM jobs",
                [],
                |row| {
                    Ok(json!({
                        "job_id": row.get::<_, String>(0)?,
                        "tenant_id": row.get::<_, String>(1)?,
                        "upstream_key_id": row.get::<_, String>(2)?,
                        "system_prompt": row.get::<_, String>(3)?,
                        "token_hash": row.get::<_, Vec<u8>>(4)?,
                        "created_at": row.get::<_, i64>(5)?,
                        "expires_at": row.get::<_, i64>(6)?,
                        "max_requests": row.get::<_, i64>(7)?,
                        "requests_used": row.get::<_, i64>(8)?,
                        "max_tokens_total": row.get::<_, i64>(9)?,
                        "tokens_used": row.get::<_, i64>(10)?,
                        "status": row.get::<_, String>(11)?,
                        "revoked_at": row.get::<_, Option<i64>>(12)?,
                        "revoked_reason": row.get::<_, Option<String>>(13)?
                    }))
                },
            )
            .unwrap()
    }

    #[tokio::test]
    async fn issuance_persists_hash_policy_defaults_and_survives_restart() {
        let fixture = Fixture::new();
        let before = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let mut body = valid_body();
        body["job_id"] = Value::String(JOB_ID.to_uppercase());
        let (status, headers, response) = issue(fixture.app.clone(), body.clone()).await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(headers[header::CACHE_CONTROL], "no-store");
        assert_eq!(response["job_id"], JOB_ID);
        assert_eq!(response["proxy_url"], "https://proxy.internal:8443/v1");
        let token = response["token"].as_str().unwrap();
        assert_eq!(token.len(), 64);
        assert!(
            token
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        );
        let row = persisted(&fixture.path);
        assert_eq!(row["job_id"], JOB_ID);
        assert_eq!(row["tenant_id"], "tenant-a");
        assert_eq!(row["upstream_key_id"], "pool-a");
        assert_eq!(row["system_prompt"], body["system_prompt"]);
        assert_eq!(
            row["token_hash"],
            json!(Sha256::digest(token.as_bytes()).to_vec())
        );
        assert!(row["created_at"].as_i64().unwrap() >= before);
        assert_eq!(
            row["expires_at"].as_i64().unwrap() - row["created_at"].as_i64().unwrap(),
            1800
        );
        assert_eq!(
            response["expires_at"],
            OffsetDateTime::from_unix_timestamp(row["expires_at"].as_i64().unwrap())
                .unwrap()
                .format(&Rfc3339)
                .unwrap()
        );
        assert!(response["expires_at"].as_str().unwrap().ends_with('Z'));
        assert_eq!(row["max_requests"], 100);
        assert_eq!(row["max_tokens_total"], 2000000);
        assert_eq!(row["requests_used"], 0);
        assert_eq!(row["tokens_used"], 0);
        assert_eq!(row["status"], "active");
        assert_eq!(row["revoked_at"], Value::Null);
        assert_eq!(row["revoked_reason"], Value::Null);
        drop(fixture.app);
        let reopened = Store::open(&fixture.path).unwrap();
        assert_eq!(persisted(&fixture.path), row);
        let restarted_app = router(config(fixture.path.clone()), reopened).unwrap();
        let (status, _, _) = issue(restarted_app, valid_body()).await;
        assert_eq!(status, StatusCode::CONFLICT);
        let bytes = std::fs::read(&fixture.path).unwrap();
        for secret in [token, CONTROL_TOKEN, UPSTREAM_SECRET] {
            assert!(
                !bytes
                    .windows(secret.len())
                    .any(|window| window == secret.as_bytes())
            );
        }
    }

    #[tokio::test]
    async fn authentication_precedes_parsing_and_rejects_ambiguous_headers() {
        let fixture = Fixture::new();
        let mut requests = vec![
            request("{malformed", None),
            request("{malformed", Some("wrong")),
            request("x".repeat(MAX_BODY_BYTES + 1), None),
        ];
        let mut duplicate = request("{malformed", Some(CONTROL_TOKEN));
        duplicate.headers_mut().append(
            header::AUTHORIZATION,
            format!("Bearer {CONTROL_TOKEN}").parse().unwrap(),
        );
        requests.push(duplicate);
        for authorization in [
            "Basic private-control-credential",
            "Bearer",
            "Bearer  private-control-credential",
            "Bearer private-control-credential, Bearer wrong",
        ] {
            let mut malformed = request("{malformed", None);
            malformed
                .headers_mut()
                .insert(header::AUTHORIZATION, authorization.parse().unwrap());
            requests.push(malformed);
        }
        for request in requests {
            let (status, headers, response) = send(fixture.app.clone(), request).await;
            assert_eq!(status, StatusCode::UNAUTHORIZED);
            assert_eq!(headers[header::WWW_AUTHENTICATE], "Bearer");
            assert_eq!(headers[header::CACHE_CONTROL], "no-store");
            assert_private_error(&response);
        }
        assert_eq!(fixture.count(), 0);
        let health = Request::builder()
            .uri("/healthz")
            .body(Body::empty())
            .unwrap();
        let (status, _, body) = send(fixture.app, health).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, json!({"status": "ok"}));
    }

    #[tokio::test]
    async fn job_bearer_is_not_a_control_plane_credential() {
        let fixture = Fixture::new();
        let (_, _, issued) = issue(fixture.app.clone(), valid_body()).await;
        let mut another = valid_body();
        another["job_id"] = json!(OTHER_JOB_ID);
        let (status, headers, _) = send(
            fixture.app.clone(),
            request(another.to_string(), issued["token"].as_str()),
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(headers[header::WWW_AUTHENTICATE], "Bearer");
        assert_eq!(fixture.count(), 1);
    }

    #[tokio::test]
    async fn invalid_and_unknown_fields_never_insert_or_echo_input() {
        let fixture = Fixture::new();
        let mut cases = Vec::new();
        for (field, value) in [
            ("job_id", json!("secret-invalid-uuid")),
            ("tenant_id", json!(" \n\t")),
            ("tenant_id", json!("é".repeat(129))),
            ("upstream_key_id", json!(" \t")),
            ("upstream_key_id", json!("é".repeat(65))),
            ("upstream_key_id", json!("unknown-secret-pool")),
            ("system_prompt", json!(" \n\t")),
            ("system_prompt", json!(42)),
            ("unexpected-secret-field", json!("secret-value")),
            ("ttl_seconds", json!(0)),
            ("ttl_seconds", json!(86401)),
            ("ttl_seconds", json!(-1)),
            ("ttl_seconds", json!(1.5)),
            ("max_requests", json!(0)),
            ("max_requests", json!(10001)),
            ("max_tokens_total", json!(0)),
            ("max_tokens_total", json!(100000001)),
        ] {
            let mut body = valid_body();
            body[field] = value;
            cases.push(body);
        }
        for field in ["job_id", "tenant_id", "upstream_key_id", "system_prompt"] {
            let mut body = valid_body();
            body.as_object_mut().unwrap().remove(field);
            cases.push(body);
        }
        for body in cases {
            let (status, headers, response) = issue(fixture.app.clone(), body).await;
            assert_eq!(status, StatusCode::BAD_REQUEST);
            assert_eq!(headers[header::CACHE_CONTROL], "no-store");
            assert_private_error(&response);
            assert_eq!(fixture.count(), 0);
        }
        for body in [
            "{malformed",
            r#"{"job_id":"secret","job_id":"duplicate"}"#,
            "[]",
            "null",
        ] {
            let (status, headers, response) =
                send(fixture.app.clone(), request(body, Some(CONTROL_TOKEN))).await;
            assert_eq!(status, StatusCode::BAD_REQUEST);
            assert_eq!(headers[header::CACHE_CONTROL], "no-store");
            assert_private_error(&response);
        }
        assert_eq!(fixture.count(), 0);
    }

    #[tokio::test]
    async fn body_limit_accepts_exactly_one_mib_and_rejects_more() {
        let fixture = Fixture::new();
        let mut body = valid_body();
        body["system_prompt"] = json!("");
        let overhead = body.to_string().len();
        body["system_prompt"] = json!("a".repeat(MAX_BODY_BYTES - overhead));
        let encoded = body.to_string();
        assert_eq!(encoded.len(), MAX_BODY_BYTES);
        let (status, _, _) = send(fixture.app.clone(), request(encoded, Some(CONTROL_TOKEN))).await;
        assert_eq!(status, StatusCode::CREATED);
        body["job_id"] = json!(OTHER_JOB_ID);
        body["system_prompt"] = json!("a".repeat(MAX_BODY_BYTES - overhead + 1));
        let (status, headers, response) = issue(fixture.app.clone(), body).await;
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(headers[header::CACHE_CONTROL], "no-store");
        assert_private_error(&response);
        assert_eq!(fixture.count(), 1);
    }

    #[tokio::test]
    async fn positive_and_cap_boundaries_are_persisted() {
        for (ttl, requests, tokens) in [(1, 1, 1), (86400, 10000, 100000000)] {
            let fixture = Fixture::new();
            let key_id = "k".repeat(128);
            let mut settings = config(fixture.path.clone());
            settings
                .upstream_keys
                .insert(key_id.clone(), UPSTREAM_SECRET.to_owned());
            let app = router(settings, Store::open(&fixture.path).unwrap()).unwrap();
            let mut body = valid_body();
            body["ttl_seconds"] = json!(ttl);
            body["max_requests"] = json!(requests);
            body["max_tokens_total"] = json!(tokens);
            body["tenant_id"] = json!("t".repeat(256));
            body["upstream_key_id"] = json!(key_id);
            let (status, _, _) = issue(app, body).await;
            assert_eq!(status, StatusCode::CREATED);
            let row = persisted(&fixture.path);
            assert_eq!(
                row["expires_at"].as_i64().unwrap() - row["created_at"].as_i64().unwrap(),
                ttl
            );
            assert_eq!(row["max_requests"], requests);
            assert_eq!(row["max_tokens_total"], tokens);
        }
    }

    #[tokio::test]
    async fn signed_integer_overflow_is_rejected_even_when_under_configured_caps() {
        let fixture = Fixture::new();
        let mut settings = config(fixture.path.clone());
        settings.max_ttl_seconds = u64::MAX;
        settings.max_requests = u64::MAX;
        settings.max_tokens_total = u64::MAX;
        let app = router(settings, Store::open(&fixture.path).unwrap()).unwrap();
        for field in ["ttl_seconds", "max_requests", "max_tokens_total"] {
            let mut body = valid_body();
            body[field] = json!(i64::MAX as u64 + 1);
            let (status, _, _) = issue(app.clone(), body).await;
            assert_eq!(status, StatusCode::BAD_REQUEST);
        }
        assert_eq!(fixture.count(), 0);
    }

    #[tokio::test]
    async fn duplicates_conflict_without_mutating_original_row() {
        let fixture = Fixture::new();
        assert_eq!(
            issue(fixture.app.clone(), valid_body()).await.0,
            StatusCode::CREATED
        );
        let original = persisted(&fixture.path);
        let mut duplicate = valid_body();
        duplicate["job_id"] = json!(JOB_ID.to_uppercase());
        duplicate["tenant_id"] = json!("different-tenant");
        duplicate["system_prompt"] = json!("different-policy");
        duplicate["max_requests"] = json!(1);
        let (status, headers, response) = issue(fixture.app.clone(), duplicate).await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(headers[header::CACHE_CONTROL], "no-store");
        assert_private_error(&response);
        assert_eq!(persisted(&fixture.path), original);
        assert_eq!(fixture.count(), 1);
    }

    #[tokio::test]
    async fn concurrent_duplicate_issuance_has_exactly_one_winner() {
        let fixture = Fixture::new();
        // Independent connections exercise the SQLite constraint, not only one mutex.
        let other_app = router(
            config(fixture.path.clone()),
            Store::open(&fixture.path).unwrap(),
        )
        .unwrap();
        let mut other_body = valid_body();
        other_body["tenant_id"] = json!("other-tenant");
        let (first, second) = tokio::join!(
            issue(fixture.app.clone(), valid_body()),
            issue(other_app, other_body),
        );
        let (winner, tenant) = match (first.0, second.0) {
            (StatusCode::CREATED, StatusCode::CONFLICT) => (first.2, "tenant-a"),
            (StatusCode::CONFLICT, StatusCode::CREATED) => (second.2, "other-tenant"),
            statuses => panic!("unexpected issuance statuses: {statuses:?}"),
        };
        assert_eq!(fixture.count(), 1);
        let row = persisted(&fixture.path);
        assert_eq!(row["tenant_id"], tenant);
        assert_eq!(
            row["token_hash"],
            json!(Sha256::digest(winner["token"].as_str().unwrap().as_bytes()).to_vec())
        );
    }

    #[tokio::test]
    async fn database_failures_are_generic_and_not_cached() {
        let fixture = Fixture::new();
        Connection::open(&fixture.path)
            .unwrap()
            .execute_batch("DROP TABLE jobs")
            .unwrap();
        let (status, headers, body) = issue(fixture.app, valid_body()).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(headers[header::CACHE_CONTROL], "no-store");
        assert_private_error(&body);
    }
}
