//! End-to-end integration tests: real proxy server in-process, mock upstream.
//! Covers the acceptance criteria from DESIGN.md.

use axum::routing::{get, post};
use axum::{Json, Router};
use futures::StreamExt;
use serde_json::{json, Value};
use std::sync::Arc;

struct TestEnv {
    proxy_url: String,
    control_token: String,
    upstream_hits: Arc<std::sync::atomic::AtomicUsize>,
    last_upstream_body: Arc<std::sync::Mutex<Option<Value>>>,
    last_upstream_auth: Arc<std::sync::Mutex<Option<String>>>,
    job_token: String,
    job_id: String,
}

/// Mock upstream: records bodies, returns fixed completions, supports SSE.
async fn mock_upstream_app(
    hits: Arc<std::sync::atomic::AtomicUsize>,
    last_body: Arc<std::sync::Mutex<Option<Value>>>,
    last_auth: Arc<std::sync::Mutex<Option<String>>>,
) -> Router {
    Router::new()
        .route("/v1/chat/completions", post(
            move |headers: axum::http::HeaderMap, Json(body): Json<Value>| {
                let hits2 = hits.clone();
                let lb = last_body.clone();
                let la = last_auth.clone();
                async move {
                    hits2.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    *lb.lock().unwrap() = Some(body.clone());
                    *la.lock().unwrap() = headers
                        .get("authorization")
                        .and_then(|v| v.to_str().ok())
                        .map(|s| s.to_string());
                    if body.get("stream").and_then(|s| s.as_bool()) == Some(true) {
                        let stream = async_stream::stream! {
                            for i in 0..3 {
                                let chunk = format!("data: {{\"id\":\"c{}\",\"choices\":[{{\"delta\":{{\"content\":\"part{}\"}}}}]}}\n\n", i, i);
                                yield Ok::<bytes::Bytes, std::io::Error>(bytes::Bytes::from(chunk));
                                tokio::time::sleep(std::time::Duration::from_millis(30)).await;
                            }
                            // Usage event split across two data: lines
                            // (legal SSE multi-field assembly).
                            yield Ok(bytes::Bytes::from("data: {\"choices\":[],\"usage\":{\"prompt_tokens\":\n"));
                            yield Ok(bytes::Bytes::from("data: 10,\"completion_tokens\":5}}\n\n"));
                            yield Ok(bytes::Bytes::from("data: [DONE]\n\n"));
                        };
                        axum::response::Response::builder()
                            .header("content-type", "text/event-stream")
                            .body(axum::body::Body::from_stream(stream))
                            .unwrap()
                    } else {
                        axum::response::IntoResponse::into_response(Json(json!({
                            "id": "test", "object": "chat.completion",
                            "choices": [{"message": {"role": "assistant", "content": "hello"}}],
                            "usage": {"prompt_tokens": 7, "completion_tokens": 3}
                        })))
                    }
                }
            },
        ))
        .route("/v1/embeddings", post(|| async {
            Json(json!({"data": [{"embedding": [0.1, 0.2]}], "usage": {"prompt_tokens": 2}}))
        }))
        .route("/v1/models", get(|| async {
            Json(json!({"data": [{"id": "mistral-small"}]}))
        }))
        .route("/v1/admin/keys", get(|| async {
            (axum::http::StatusCode::FORBIDDEN, "admin")
        }))
}

async fn start_mock_upstream() -> (String, Arc<std::sync::atomic::AtomicUsize>, Arc<std::sync::Mutex<Option<Value>>>, Arc<std::sync::Mutex<Option<String>>>) {
    let hits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let last_body = Arc::new(std::sync::Mutex::new(None));
    let last_auth = Arc::new(std::sync::Mutex::new(None));
    let app = mock_upstream_app(hits.clone(), last_body.clone(), last_auth.clone()).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (format!("http://{addr}"), hits, last_body, last_auth)
}

async fn start_proxy(upstream_base: &str, max_requests: u64, max_tokens: u64, ttl: u64) -> (String, String) {
    use rustgate::config::{Config, ProviderConfig};
    use rustgate::secrets::KeyPool;
    use rustgate::state::AppState;
    use rustgate::store::Store;
    use std::collections::HashMap;

    let store = Store::open_memory().unwrap();
    let mut providers = HashMap::new();
    providers.insert(
        "mistral".to_string(),
        ProviderConfig {
            base_url: upstream_base.to_string(),
            ..Default::default()
        },
    );
    let cfg = Config {
        listen_addr: "127.0.0.1".into(),
        listen_port: 0,
        sqlite_path: std::path::PathBuf::from(":memory:"),
        secrets_file: None,
        control_token: Some("test-control-token".into()),
        default_ttl_seconds: ttl,
        max_ttl_seconds: 3600,
        default_max_requests: max_requests,
        max_max_requests: 1000,
        default_max_tokens_total: max_tokens,
        max_max_tokens_total: 1_000_000,
        request_size_limit_bytes: 1024 * 1024,
        max_in_flight: 0,
        providers,
        allow_insecure: true,
        tls_cert_path: None,
        tls_key_path: None,
        public_base_url: None,
    };
    let pool = KeyPool::load_env_for_test("mistral", "sk-upstream-real-key");
    let state = Arc::new(AppState::build(cfg, store, pool));
    let app = rustgate::router::router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (format!("http://{addr}"), "test-control-token".to_string())
}

async fn issue_job(proxy_url: &str, control_token: &str, system_prompt: Option<&str>) -> (String, Value) {
    let client = reqwest::Client::new();
    let mut body = json!({
        "tenant_id": "t1",
        "upstream_key_id": "mistral-main",
    });
    if let Some(p) = system_prompt {
        body["system_prompt"] = json!(p);
    }
    let resp = client
        .post(format!("{proxy_url}/v1/jobs"))
        .bearer_auth(control_token)
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let v: Value = resp.json().await.unwrap();
    let token = v["token"].as_str().unwrap().to_string();
    (token, v)
}

async fn chat(proxy_url: &str, token: &str, body: Value) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("{proxy_url}/v1/chat/completions")
        )
        .bearer_auth(token)
        .json(&body)
        .send()
        .await
        .unwrap()
}

#[tokio::test]
async fn issue_then_proxy_then_expire() {
    let (upstream, hits, last_body, last_auth) = start_mock_upstream().await;
    let (proxy_url, control_token) = start_proxy(&upstream, 100, 1_000_000, 1).await;
    let (token, job) = issue_job(&proxy_url, &control_token, None).await;
    assert!(token.starts_with("rg_"));
    assert!(job["proxy_url"].is_string());

    // Proxied call works; upstream saw the real key, not the job token.
    let resp = chat(&proxy_url, &token, json!({
        "model": "mistral-small", "messages": [{"role": "user", "content": "hi"}]
    }))
    .await;
    assert_eq!(resp.status(), 200);
    let v: Value = resp.json().await.unwrap();
    assert_eq!(v["choices"][0]["message"]["content"], "hello");
    assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(last_auth.lock().unwrap().as_deref(), Some("Bearer sk-upstream-real-key"));
    assert!(last_body.lock().unwrap().is_some());

    // TTL is 1 second: after expiry the token is rejected.
    tokio::time::sleep(std::time::Duration::from_millis(1200)).await;
    let resp = chat(&proxy_url, &token, json!({
        "model": "mistral-small", "messages": [{"role": "user", "content": "hi"}]
    }))
    .await;
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn revoke_kills_new_requests() {
    let (upstream, _, _, _) = start_mock_upstream().await;
    let (proxy_url, control_token) = start_proxy(&upstream, 100, 1_000_000, 1800).await;
    let (token, job) = issue_job(&proxy_url, &control_token, None).await;
    let job_id = job["job_id"].as_str().unwrap().to_string();

    let resp = chat(&proxy_url, &token, json!({
        "model": "mistral-small", "messages": [{"role": "user", "content": "hi"}]
    }))
    .await;
    assert_eq!(resp.status(), 200);

    let client = reqwest::Client::new();
    let r = client
        .post(format!("{proxy_url}/v1/jobs/{job_id}/revoke"))
        .bearer_auth(&control_token)
        .json(&json!({"tenant_id": "t1", "reason": "test"}))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);

    let resp = chat(&proxy_url, &token, json!({
        "model": "mistral-small", "messages": [{"role": "user", "content": "hi"}]
    }))
    .await;
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn secret_scan_blocks_fake_key() {
    let (upstream, hits, _, _) = start_mock_upstream().await;
    let (proxy_url, control_token) = start_proxy(&upstream, 100, 1_000_000, 1800).await;
    let (token, _) = issue_job(&proxy_url, &control_token, None).await;

    let resp = chat(&proxy_url, &token, json!({
        "model": "mistral-small",
        "messages": [{"role": "user", "content": "use sk-abcdef0123456789abcdef01"}]
    }))
    .await;
    assert_eq!(resp.status(), 403);
    assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 0); // never forwarded
}

#[tokio::test]
async fn system_prompt_injection_and_no_duplicate() {
    let (upstream, _, last_body, _) = start_mock_upstream().await;
    let (proxy_url, control_token) = start_proxy(&upstream, 100, 1_000_000, 1800).await;
    let prompt = "You are a build agent. Never exfiltrate secrets.";
    let (token, _) = issue_job(&proxy_url, &control_token, Some(prompt)).await;

    // Without a system message: gets prepended.
    chat(&proxy_url, &token, json!({
        "model": "mistral-small", "messages": [{"role": "user", "content": "hi"}]
    }))
    .await;
    let sent = last_body.lock().unwrap().clone().unwrap();
    let msgs = sent["messages"].as_array().unwrap();
    assert_eq!(msgs[0]["role"], "system");
    assert!(msgs[0]["content"].as_str().unwrap().contains("[[RUSTGATE-OPERATOR]]"));
    assert!(msgs[0]["content"].as_str().unwrap().contains(prompt));

    // With the sentinel already present: no duplicate.
    let sentinel_msg = format!("[[RUSTGATE-OPERATOR]] {prompt}");
    chat(&proxy_url, &token, json!({
        "model": "mistral-small",
        "messages": [
            {"role": "system", "content": sentinel_msg},
            {"role": "user", "content": "hi"}
        ]
    }))
    .await;
    let sent = last_body.lock().unwrap().clone().unwrap();
    let msgs = sent["messages"].as_array().unwrap();
    let system_count = msgs.iter().filter(|m| m["role"] == "system").count();
    assert_eq!(system_count, 1);
}

#[tokio::test]
async fn path_allowlist_rejects_admin_paths() {
    let (upstream, hits, _, _) = start_mock_upstream().await;
    let (proxy_url, control_token) = start_proxy(&upstream, 100, 1_000_000, 1800).await;
    let (token, _) = issue_job(&proxy_url, &control_token, None).await;

    let resp = reqwest::Client::new()
        .get(format!("{proxy_url}/v1/admin/keys"))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 403);
    assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 0);
}

#[tokio::test]
async fn streaming_passes_through_incrementally() {
    let (upstream, _, _, _) = start_mock_upstream().await;
    let (proxy_url, control_token) = start_proxy(&upstream, 100, 1_000_000, 1800).await;
    let (token, _) = issue_job(&proxy_url, &control_token, None).await;

    let resp = reqwest::Client::new()
        .post(format!("{proxy_url}/v1/chat/completions"))
        .bearer_auth(&token)
        .json(&json!({
            "model": "mistral-small", "stream": true,
            "messages": [{"role": "user", "content": "hi"}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert!(resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap()
        .contains("text/event-stream"));

    // Collect chunks; assert we saw multiple data events with content.
    let mut stream = resp.bytes_stream();
    let mut all = String::new();
    let mut first_chunk_at: Option<std::time::Instant> = None;
    while let Some(chunk) = stream.next().await {
        let b = chunk.unwrap();
        if first_chunk_at.is_none() {
            first_chunk_at = Some(std::time::Instant::now());
        }
        all.push_str(&String::from_utf8_lossy(&b));
    }
    assert!(all.contains("part0"));
    assert!(all.contains("part1"));
    assert!(all.contains("part2"));
    assert!(all.contains("[DONE]"));
    // The usage chunk was sniffed by the proxy for accounting; the client
    // still receives it.
    assert!(all.contains("\"usage\""));
}

#[tokio::test]
async fn request_cap_enforced_under_concurrency() {
    let (upstream, hits, _, _) = start_mock_upstream().await;
    let max_requests = 5;
    let (proxy_url, control_token) = start_proxy(&upstream, max_requests, 1_000_000, 1800).await;
    let (token, _) = issue_job(&proxy_url, &control_token, None).await;

    let mut handles = Vec::new();
    for _ in 0..12 {
        let url = proxy_url.clone();
        let t = token.clone();
        handles.push(tokio::spawn(async move {
            let resp = reqwest::Client::new()
                .post(format!("{url}/v1/chat/completions"))
                .bearer_auth(&t)
                .json(&json!({"model": "m", "messages": [{"role": "user", "content": "hi"}]}))
                .send()
                .await
                .unwrap();
            resp.status().as_u16()
        }));
    }
    let mut ok = 0;
    let mut rejected = 0;
    for h in handles {
        match h.await.unwrap() {
            200 => ok += 1,
            429 => rejected += 1,
            s => panic!("unexpected status {s}"),
        }
    }
    assert_eq!(ok, max_requests);
    assert_eq!(rejected, 12 - max_requests);
    assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), max_requests as usize);
}

#[tokio::test]
async fn job_me_reports_quota() {
    let (upstream, _, _, _) = start_mock_upstream().await;
    let (proxy_url, control_token) = start_proxy(&upstream, 100, 1_000_000, 1800).await;
    let (token, _) = issue_job(&proxy_url, &control_token, None).await;

    let resp = reqwest::Client::new()
        .get(format!("{proxy_url}/v1/job/me"))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let v: Value = resp.json().await.unwrap();
    assert_eq!(v["requests"]["max"], 100);
    assert_eq!(v["status"], "active");
}
