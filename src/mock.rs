//! Deliberately insecure local fixture. Captures include credentials and bodies.

use std::{collections::VecDeque, convert::Infallible, sync::Arc, time::Duration};

use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, OriginalUri, Query, State},
    http::{HeaderMap, Method, StatusCode, header},
    response::{IntoResponse, Response, Sse, sse::Event},
    routing::{get, post},
};
use futures_util::stream;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::Mutex;

const MAX_BODY_BYTES: usize = 1024 * 1024;
const MAX_CAPTURED_REQUESTS: usize = 100;
const MAX_DELAY_MS: u64 = 10_000;

#[derive(Clone, Default)]
struct MockState {
    requests: Arc<Mutex<VecDeque<CapturedRequest>>>,
}

impl MockState {
    async fn capture(&self, request: CapturedRequest) {
        let mut requests = self.requests.lock().await;
        if requests.len() == MAX_CAPTURED_REQUESTS {
            requests.pop_front();
        }
        requests.push_back(request);
    }
}

#[derive(Serialize)]
struct CapturedRequest {
    method: String,
    path: String,
    authorization: Option<String>,
    body: Value,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
enum Mode {
    Normal,
    Delayed,
    Sse,
    Failure,
}

#[derive(Deserialize)]
struct Controls {
    #[serde(default)]
    mode: Option<Mode>,
    #[serde(default = "default_delay_ms")]
    delay_ms: u64,
}

fn default_delay_ms() -> u64 {
    100
}

impl Controls {
    fn validate(&self) -> Result<(), (StatusCode, Json<Value>)> {
        if self.delay_ms > MAX_DELAY_MS {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({"error": {"message": "delay_ms must be between 0 and 10000"}})),
            ));
        }
        Ok(())
    }
}

/// Creates an isolated local fixture, including its bounded request capture store.
///
/// This router exposes credentials and request bodies at `/__mock/requests` and
/// must only be served on loopback for local development, never in production.
/// Only requests with valid fixture controls and valid JSON where required are
/// captured. The newest 100 captures are retained; older entries are evicted.
pub fn router() -> Router {
    Router::new()
        .route("/healthz", get(|| async { Json(json!({"status": "ok"})) }))
        .route("/v1/chat/completions", post(chat_completions))
        .route("/v1/embeddings", post(embeddings))
        .route("/v1/models", get(models))
        .route("/__mock/requests", get(requests).delete(clear_requests))
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
        .with_state(MockState::default())
}

async fn requests(State(state): State<MockState>) -> Response {
    let requests = state.requests.lock().await;
    // Serialize in place rather than cloning up to 100 full request bodies.
    Json(&*requests).into_response()
}

async fn clear_requests(State(state): State<MockState>) -> StatusCode {
    state.requests.lock().await.clear();
    StatusCode::NO_CONTENT
}

async fn chat_completions(
    State(state): State<MockState>,
    Query(controls): Query<Controls>,
    method: Method,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    if let Err(error) = controls.validate() {
        return error.into_response();
    }

    // Match real chat providers without requiring client-specific query support.
    // Explicit fixture modes still override the requested response format.
    let mode = controls.mode.unwrap_or_else(|| {
        if body.get("stream") == Some(&Value::Bool(true)) {
            Mode::Sse
        } else {
            Mode::Normal
        }
    });

    state
        .capture(CapturedRequest {
            method: method.to_string(),
            path: uri.path().to_owned(),
            authorization: headers
                .get(header::AUTHORIZATION)
                .map(|value| String::from_utf8_lossy(value.as_bytes()).into_owned()),
            body,
        })
        .await;

    match mode {
        Mode::Normal => Json(completion()).into_response(),
        Mode::Delayed => {
            tokio::time::sleep(Duration::from_millis(controls.delay_ms)).await;
            Json(completion()).into_response()
        }
        Mode::Failure => (
            StatusCode::SERVICE_UNAVAILABLE,
            [(header::RETRY_AFTER, "1")],
            Json(json!({
                "error": {
                    "message": "Controlled mock upstream failure",
                    "type": "mock_failure",
                    "code": "mock_unavailable"
                }
            })),
        )
            .into_response(),
        Mode::Sse => {
            let events = stream::unfold(0_u8, move |index| async move {
                let data = match index {
                    0 => chunk(
                        json!([{
                            "index": 0,
                            "delta": {"role": "assistant", "content": "Hello "},
                            "finish_reason": null
                        }]),
                        None,
                    )
                    .to_string(),
                    1 => {
                        tokio::time::sleep(Duration::from_millis(controls.delay_ms)).await;
                        chunk(
                            json!([{
                                "index": 0,
                                "delta": {"content": "from mock."},
                                "finish_reason": "stop"
                            }]),
                            None,
                        )
                        .to_string()
                    }
                    2 => chunk(json!([]), Some(usage())).to_string(),
                    3 => "[DONE]".to_owned(),
                    _ => return None,
                };
                Some((Ok::<_, Infallible>(Event::default().data(data)), index + 1))
            });
            Sse::new(events).into_response()
        }
    }
}

async fn embeddings(
    State(state): State<MockState>,
    Query(controls): Query<Controls>,
    method: Method,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    if let Err(error) = validate_non_streaming(&controls) {
        return error.into_response();
    }
    let is_token = |value: &Value| value.as_u64().is_some_and(|n| u32::try_from(n).is_ok());
    let input = body.get("input").unwrap_or(&Value::Null);
    let inputs: &[Value] = match input {
        Value::String(text) if !text.is_empty() => std::slice::from_ref(input),
        Value::Array(items)
            if !items.is_empty()
                && items
                    .iter()
                    .all(|item| item.as_str().is_some_and(|s| !s.is_empty())) =>
        {
            items
        }
        Value::Array(items) if !items.is_empty() && items.iter().all(is_token) => {
            std::slice::from_ref(input)
        }
        Value::Array(items)
            if !items.is_empty()
                && items.iter().all(|item| {
                    item.as_array()
                        .is_some_and(|tokens| !tokens.is_empty() && tokens.iter().all(is_token))
                }) =>
        {
            items
        }
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": {"message": "input must contain text or token arrays"}})),
            )
                .into_response();
        }
    };
    let data: Vec<Value> = inputs
        .iter()
        .enumerate()
        .map(|(index, input)| {
            let length = match input {
                Value::String(text) => text.len(),
                Value::Array(tokens) => tokens.len(),
                _ => unreachable!("embedding inputs validated above"),
            };
            json!({
                "object": "embedding",
                "index": index,
                "embedding": [length as f64 / 100.0, 0.25, -0.5]
            })
        })
        .collect();
    let response = json!({
        "object": "list",
        "data": data,
        "model": body.get("model").and_then(Value::as_str).unwrap_or("mock-embed"),
        "usage": {"prompt_tokens": 5, "total_tokens": 5}
    });
    state
        .capture(CapturedRequest {
            method: method.to_string(),
            path: uri.path().to_owned(),
            authorization: headers
                .get(header::AUTHORIZATION)
                .map(|value| String::from_utf8_lossy(value.as_bytes()).into_owned()),
            body,
        })
        .await;
    non_streaming_response(controls, response).await
}

async fn models(
    State(state): State<MockState>,
    Query(controls): Query<Controls>,
    method: Method,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
) -> Response {
    if let Err(error) = validate_non_streaming(&controls) {
        return error.into_response();
    }
    state
        .capture(CapturedRequest {
            method: method.to_string(),
            path: uri.path().to_owned(),
            authorization: headers
                .get(header::AUTHORIZATION)
                .map(|value| String::from_utf8_lossy(value.as_bytes()).into_owned()),
            body: Value::Null,
        })
        .await;
    non_streaming_response(
        controls,
        json!({
            "object": "list",
            "data": [
                {"id": "mock-mistral", "object": "model", "created": 0, "owned_by": "mock"},
                {"id": "mock-embed", "object": "model", "created": 0, "owned_by": "mock"}
            ]
        }),
    )
    .await
}

fn validate_non_streaming(controls: &Controls) -> Result<(), (StatusCode, Json<Value>)> {
    controls.validate()?;
    if matches!(controls.mode, Some(Mode::Sse)) {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({"error": {"message": "mode=sse is not supported for this endpoint"}})),
        ));
    }
    Ok(())
}

async fn non_streaming_response(controls: Controls, body: Value) -> Response {
    match controls.mode.unwrap_or(Mode::Normal) {
        Mode::Delayed => {
            tokio::time::sleep(Duration::from_millis(controls.delay_ms)).await;
            Json(body).into_response()
        }
        Mode::Failure => (
            StatusCode::SERVICE_UNAVAILABLE,
            [(header::RETRY_AFTER, "1")],
            Json(json!({
                "error": {
                    "message": "Controlled mock upstream failure",
                    "type": "mock_failure",
                    "code": "mock_unavailable"
                }
            })),
        )
            .into_response(),
        Mode::Normal => Json(body).into_response(),
        Mode::Sse => unreachable!("non-streaming controls validated before capture"),
    }
}

fn usage() -> Value {
    json!({"prompt_tokens": 5, "completion_tokens": 3, "total_tokens": 8})
}

fn completion() -> Value {
    json!({
        "id": "chatcmpl-mock",
        "object": "chat.completion",
        "created": 0,
        "model": "mock-mistral",
        "choices": [{
            "index": 0,
            "message": {"role": "assistant", "content": "Hello from mock."},
            "finish_reason": "stop"
        }],
        "usage": usage()
    })
}

fn chunk(choices: Value, usage: Option<Value>) -> Value {
    let mut chunk = json!({
        "id": "chatcmpl-mock",
        "object": "chat.completion.chunk",
        "created": 0,
        "model": "mock-mistral",
        "choices": choices
    });
    if let Some(usage) = usage {
        chunk["usage"] = usage;
    }
    chunk
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn non_streaming_endpoints_capture_requests_and_reject_sse() {
        use axum::{
            body::{Body, to_bytes},
            http::Request,
        };
        use tower::ServiceExt;

        let app = router();
        for input in [
            json!("hello"),
            json!(["hello", "world"]),
            json!([1, 2]),
            json!([[1], [2]]),
        ] {
            let expected = if input[0].is_string() || input[0].is_array() {
                2
            } else {
                1
            };
            let body = json!({"model": "mock-embed", "input": input});
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/v1/embeddings?mode=delayed&delay_ms=0")
                        .header(header::CONTENT_TYPE, "application/json")
                        .header(header::AUTHORIZATION, "Bearer fixture-secret")
                        .body(Body::from(body.to_string()))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let bytes = to_bytes(response.into_body(), MAX_BODY_BYTES)
                .await
                .unwrap();
            let response: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(response["data"].as_array().unwrap().len(), expected);
            assert_eq!(
                response["usage"],
                json!({"prompt_tokens": 5, "total_tokens": 5})
            );
        }
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/models")
                    .header(header::AUTHORIZATION, "Bearer fixture-secret")
                    .body(Body::from("not JSON and must not be read"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = to_bytes(response.into_body(), MAX_BODY_BYTES)
            .await
            .unwrap();
        let response: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(response["data"][0]["id"], "mock-mistral");
        assert_eq!(response["data"][1]["id"], "mock-embed");

        for (method, path) in [("POST", "/v1/embeddings"), ("GET", "/v1/models")] {
            for (mode, status) in [
                ("sse", StatusCode::BAD_REQUEST),
                ("failure", StatusCode::SERVICE_UNAVAILABLE),
            ] {
                let response = app
                    .clone()
                    .oneshot(
                        Request::builder()
                            .method(method)
                            .uri(format!("{path}?mode={mode}"))
                            .header(header::CONTENT_TYPE, "application/json")
                            .body(Body::from(r#"{"model":"mock-embed","input":"hello"}"#))
                            .unwrap(),
                    )
                    .await
                    .unwrap();
                assert_eq!(response.status(), status);
            }
        }
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/__mock/requests")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let bytes = to_bytes(response.into_body(), MAX_BODY_BYTES)
            .await
            .unwrap();
        let captures: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(captures.as_array().unwrap().len(), 7);
        assert_eq!(captures[0]["method"], "POST");
        assert_eq!(captures[0]["path"], "/v1/embeddings");
        assert_eq!(captures[0]["authorization"], "Bearer fixture-secret");
        assert_eq!(
            captures[0]["body"],
            json!({"model": "mock-embed", "input": "hello"})
        );
        assert_eq!(captures[4]["method"], "GET");
        assert_eq!(captures[4]["path"], "/v1/models");
        assert_eq!(captures[4]["authorization"], "Bearer fixture-secret");
        assert_eq!(captures[4]["body"], Value::Null);
    }

    #[test]
    fn controls_reject_unknown_modes_and_invalid_delays() {
        for query in [
            "mode=unknown",
            "delay_ms=-1",
            "delay_ms=abc",
            "delay_ms=10001",
        ] {
            let uri = format!("/v1/chat/completions?{query}").parse().unwrap();
            if let Ok(Query(controls)) = Query::<Controls>::try_from_uri(&uri) {
                assert!(controls.validate().is_err());
            }
        }
        for query in ["", "mode=sse&delay_ms=0", "mode=delayed&delay_ms=10000"] {
            let uri = format!("/v1/chat/completions?{query}").parse().unwrap();
            let Query(controls) = Query::<Controls>::try_from_uri(&uri).unwrap();
            assert!(controls.validate().is_ok());
        }
    }

    #[tokio::test]
    async fn response_format_honors_stream_body_with_explicit_mode_precedence() {
        use axum::{body::to_bytes, http::Request};
        use tower::ServiceExt;

        for (query, body, streaming) in [
            ("delay_ms=0", json!({"stream": true}), true),
            ("", json!({"stream": false}), false),
            ("", json!({}), false),
            ("mode=normal", json!({"stream": true}), false),
            ("mode=sse&delay_ms=0", json!({"stream": false}), true),
        ] {
            let response = router()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri(format!("/v1/chat/completions?{query}"))
                        .header(header::CONTENT_TYPE, "application/json")
                        .body(axum::body::Body::from(body.to_string()))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let expected_type = if streaming {
                "text/event-stream"
            } else {
                "application/json"
            };
            assert!(
                response.headers()[header::CONTENT_TYPE]
                    .to_str()
                    .unwrap()
                    .starts_with(expected_type),
                "wrong response format for {query}, body {body}"
            );
            let bytes = to_bytes(response.into_body(), MAX_BODY_BYTES)
                .await
                .unwrap();
            if streaming {
                let text = std::str::from_utf8(&bytes).unwrap();
                let mut events: Vec<_> = text
                    .lines()
                    .filter_map(|line| line.strip_prefix("data: "))
                    .collect();
                assert_eq!(events.pop(), Some("[DONE]"));
                let chunks: Vec<Value> = events
                    .into_iter()
                    .map(|event| serde_json::from_str(event).unwrap())
                    .collect();
                let choices: Vec<_> = chunks
                    .iter()
                    .flat_map(|chunk| chunk["choices"].as_array().unwrap())
                    .collect();
                let content: String = choices
                    .iter()
                    .filter_map(|choice| choice["delta"]["content"].as_str())
                    .collect();
                assert_eq!(content, "Hello from mock.");
                assert_eq!(choices.last().unwrap()["finish_reason"], "stop");
            } else {
                let completion: Value = serde_json::from_slice(&bytes).unwrap();
                assert_eq!(
                    completion["choices"][0]["message"]["content"],
                    "Hello from mock."
                );
                assert_eq!(completion["choices"][0]["finish_reason"], "stop");
            }
        }
    }

    #[tokio::test]
    async fn captures_evict_oldest_and_clear_without_affecting_other_instances() {
        let state = MockState::default();
        let other = MockState::default();
        for index in 0..=MAX_CAPTURED_REQUESTS {
            state
                .capture(CapturedRequest {
                    method: "POST".to_owned(),
                    path: "/v1/chat/completions".to_owned(),
                    authorization: Some("Bearer local-test-secret".to_owned()),
                    body: json!({"index": index}),
                })
                .await;
        }
        let captured = captured_json(state.clone()).await;
        assert_eq!(captured.as_array().unwrap().len(), MAX_CAPTURED_REQUESTS);
        assert_eq!(captured[0]["body"]["index"], 1);
        assert_eq!(
            captured[MAX_CAPTURED_REQUESTS - 1]["body"]["index"],
            MAX_CAPTURED_REQUESTS
        );
        assert_eq!(captured_json(other.clone()).await, json!([]));
        assert_eq!(
            clear_requests(State(state.clone())).await,
            StatusCode::NO_CONTENT
        );
        assert_eq!(captured_json(state).await, json!([]));
        assert_eq!(captured_json(other).await, json!([]));
    }

    async fn captured_json(state: MockState) -> Value {
        let response = requests(State(state)).await;
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&body).unwrap()
    }
}
