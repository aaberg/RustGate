use super::*;
use axum::body::Bytes;
use futures_util::StreamExt;
use std::{io, net::SocketAddr, time::Duration};
use tokio::{net::TcpListener, sync::mpsc, task::JoinHandle};

type Chunk = Result<Bytes, io::Error>;

struct Server {
    address: SocketAddr,
    task: JoinHandle<()>,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn serve(app: Router) -> Server {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    Server { address, task }
}

// The first upstream response is controlled by the test. Later calls complete
// immediately, so capacity recovery can be observed without another open stream.
async fn fixture(
    configure: impl FnOnce(&mut Config),
) -> (
    Fixture,
    Server,
    mpsc::Sender<Chunk>,
    mpsc::UnboundedReceiver<Value>,
) {
    let (chunks, receiver) = mpsc::channel::<Chunk>(4);
    let receiver = Arc::new(tokio::sync::Mutex::new(Some(receiver)));
    let (captured, captures) = mpsc::unbounded_channel();
    let upstream = Router::new().route(
        "/v1/chat/completions",
        post(move |Json(body): Json<Value>| {
            let receiver = receiver.clone();
            let captured = captured.clone();
            async move {
                captured.send(body).unwrap();
                let body = match receiver.lock().await.take() {
                    Some(receiver) => Body::from_stream(futures_util::stream::unfold(
                        receiver,
                        |mut receiver| async move {
                            receiver.recv().await.map(|chunk| (chunk, receiver))
                        },
                    )),
                    None => Body::from("{\"recovered\":true}"),
                };
                (
                    [
                        (header::CONTENT_TYPE, "text/event-stream"),
                        (header::CACHE_CONTROL, "public, max-age=100"),
                    ],
                    body,
                )
            }
        }),
    );
    let server = serve(upstream).await;
    let mut fixture = Fixture::new();
    let mut settings = config(fixture.path.clone());
    settings.upstream_base_url = format!("http://{}", server.address).parse().unwrap();
    settings.first_byte_timeout_seconds = 30;
    settings.idle_timeout_seconds = 30;
    settings.overall_timeout_seconds = 30;
    configure(&mut settings);
    fixture.app = router(settings, Store::open(&fixture.path).unwrap()).unwrap();
    (fixture, server, chunks, captures)
}

fn chat(body: impl Into<Body>, token: &str) -> Request<Body> {
    let mut request = request(body, Some(token));
    *request.uri_mut() = "/v1/chat/completions".parse().unwrap();
    request
}

fn payload() -> String {
    json!({"model":"stream-model", "stream":true, "messages":[
        {"role":"system", "content":"untrusted client policy"},
        {"role":"user", "content":"Hello"}
    ]})
    .to_string()
}

async fn token(fixture: &Fixture) -> String {
    let (status, _, issued) = issue(fixture.app.clone(), valid_body()).await;
    assert_eq!(status, StatusCode::CREATED);
    issued["token"].as_str().unwrap().to_owned()
}

const STREAM_BUDGET: i64 = 4096;

async fn full_budget_token(fixture: &Fixture) -> String {
    let mut job = valid_body();
    job["max_tokens_total"] = json!(STREAM_BUDGET);
    let (status, _, issued) = issue(fixture.app.clone(), job).await;
    assert_eq!(status, StatusCode::CREATED);
    issued["token"].as_str().unwrap().to_owned()
}

fn token_balances(fixture: &Fixture) -> (i64, i64, i64) {
    Connection::open(&fixture.path)
        .unwrap()
        .query_row(
            "SELECT tokens_used, tokens_unreported, tokens_reserved FROM jobs",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap()
}

async fn budget_exhausted(fixture: &Fixture, token: &str) {
    // Exhaustion must reject before consuming a body, and must not look like
    // leaked in-flight capacity.
    let unread = Body::from_stream(futures_util::stream::pending::<Chunk>());
    let (status, headers, error) = bounded(send(fixture.app.clone(), chat(unread, token))).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(headers[header::CACHE_CONTROL], "no-store");
    assert_private_error(&error);
    assert_eq!(persisted(&fixture.path)["requests_used"], 1);
}

fn conservative_settlement_survives_duplicate_and_restart(
    fixture: &Fixture,
    expected: Option<i64>,
) {
    let balances = (
        expected.unwrap_or(0),
        STREAM_BUDGET - expected.unwrap_or(0),
        0,
    );
    assert_eq!(token_balances(fixture), balances);
    assert_eq!(persisted(&fixture.path)["status"], "exhausted");
    let (request_id, prompt, completion, total): (i64, Option<i64>, Option<i64>, Option<i64>) =
        Connection::open(&fixture.path)
            .unwrap()
            .query_row(
                "SELECT request_id, prompt_tokens, completion_tokens, total_tokens
                 FROM request_records ORDER BY request_id LIMIT 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
    let usage = expected.map(|total_tokens| crate::store::Usage {
        prompt_tokens: prompt.unwrap(),
        completion_tokens: completion.unwrap(),
        total_tokens,
    });
    assert_eq!(total, expected);
    if let Some(usage) = usage {
        assert_eq!(
            usage.prompt_tokens + usage.completion_tokens,
            usage.total_tokens
        );
    }
    let before = accounting_record(fixture);
    Store::open(&fixture.path)
        .unwrap()
        .finish_request(request_id, "completed", usage, false)
        .unwrap();
    assert_eq!(token_balances(fixture), balances);
    assert_eq!(accounting_record(fixture), before);
    drop(Store::open(&fixture.path).unwrap());
    assert_eq!(token_balances(fixture), balances);
    assert_eq!(accounting_record(fixture), before);
    assert_eq!(persisted(&fixture.path)["requests_used"], 1);
}

async fn bounded<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .expect("stream operation did not complete")
}

async fn recovered(fixture: &Fixture, token: &str) {
    bounded(async {
        loop {
            let (status, _, body) = send(fixture.app.clone(), chat(payload(), token)).await;
            if status == StatusCode::SERVICE_UNAVAILABLE {
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            }
            assert_eq!(status, StatusCode::OK);
            assert_eq!(body, json!({"recovered":true}));
            break;
        }
    })
    .await;
    wait_for_accounting(fixture).await;
}

async fn deadline_transport_precedes_contended_accounting(overall: bool) {
    let (fixture, _server, chunks, mut captures) = fixture(|settings| {
        settings.max_in_flight_requests = 1;
        if overall {
            settings.overall_timeout_seconds = 2;
        } else {
            settings.idle_timeout_seconds = 2;
        }
    })
    .await;
    let token = token(&fixture).await;
    let report =
        b"data: {\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":3,\"total_tokens\":8}}\n\n";
    chunks.send(Ok(Bytes::from_static(report))).await.unwrap();
    let response = bounded(fixture.app.clone().oneshot(chat(payload(), &token)))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    bounded(captures.recv()).await.unwrap();
    let mut body = response.into_body().into_data_stream();
    assert_eq!(
        bounded(body.next()).await.unwrap().unwrap().as_ref(),
        report
    );
    let reservation = token_balances(&fixture).2;
    assert!(reservation > 8);

    // Reading the report synchronizes admission and known usage before taking
    // the external writer lock. Keep it held until transport termination and
    // capacity checks have both completed, rather than releasing on a timer.
    let writer = Connection::open(&fixture.path).unwrap();
    writer.execute_batch("BEGIN IMMEDIATE").unwrap();
    assert_eq!(accounting_record(&fixture), ("pending".to_owned(), None));
    let terminal = tokio::time::timeout(Duration::from_secs(4), body.next())
        .await
        .expect("deadline transport error waited for the SQLite writer");
    assert!(terminal.expect("deadline became clean EOF").is_err());
    bounded(chunks.closed()).await;
    assert_eq!(accounting_record(&fixture), ("pending".to_owned(), None));
    assert_eq!(persisted(&fixture.path)["tokens_used"], 0);
    let (status, headers, error) =
        bounded(send(fixture.app.clone(), chat(payload(), &token))).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(headers[header::RETRY_AFTER], "1");
    assert_private_error(&error);
    assert_eq!(persisted(&fixture.path)["requests_used"], 1);
    assert!(captures.try_recv().is_err());

    writer.execute_batch("COMMIT").unwrap();
    wait_for_accounting(&fixture).await;
    assert_eq!(accounting_record(&fixture), ("timeout".to_owned(), Some(8)));
    assert_eq!(persisted(&fixture.path)["tokens_used"], 8);
    assert_eq!(token_balances(&fixture), (8, reservation - 8, 0));
    let usage: (i64, i64, i64) = writer
        .query_row(
            "SELECT prompt_tokens, completion_tokens, total_tokens
             FROM request_records ORDER BY request_id LIMIT 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(usage, (5, 3, 8));
    recovered(&fixture, &token).await;
    assert_eq!(persisted(&fixture.path)["requests_used"], 2);
    assert_eq!(persisted(&fixture.path)["tokens_used"], 8);
}

#[tokio::test]
async fn idle_deadline_terminates_transport_while_sqlite_writer_holds_accounting() {
    deadline_transport_precedes_contended_accounting(false).await;
}

#[tokio::test]
async fn overall_deadline_terminates_transport_while_sqlite_writer_holds_accounting() {
    deadline_transport_precedes_contended_accounting(true).await;
}

#[tokio::test]
async fn sse_chunks_arrive_before_eof_with_authoritative_prompt_and_no_buffering() {
    let (fixture, _server, chunks, mut captures) = fixture(|_| {}).await;
    let token = token(&fixture).await;
    let response = tokio::spawn(fixture.app.clone().oneshot(chat(payload(), &token)));
    let forwarded = bounded(captures.recv()).await.unwrap();
    assert_eq!(forwarded["stream"], true);
    assert_eq!(
        forwarded["messages"],
        json!([
            {"role":"system", "content":valid_body()["system_prompt"]},
            {"role":"system", "content":"untrusted client policy"},
            {"role":"user", "content":"Hello"}
        ])
    );
    chunks
        .send(Ok(Bytes::from_static(b"data: {\"part\":1}\n\n")))
        .await
        .unwrap();
    let response = bounded(response).await.unwrap().unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()[header::CONTENT_TYPE],
        "text/event-stream"
    );
    assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
    let mut body = response.into_body().into_data_stream();
    assert_eq!(
        bounded(body.next()).await.unwrap().unwrap(),
        "data: {\"part\":1}\n\n"
    );
    chunks
        .send(Ok(Bytes::from_static(b"data: [DONE]\n\n")))
        .await
        .unwrap();
    assert_eq!(
        bounded(body.next()).await.unwrap().unwrap(),
        "data: [DONE]\n\n"
    );
    drop(chunks);
    assert!(bounded(body.next()).await.is_none());
    wait_for_accounting(&fixture).await;
    assert_eq!(persisted(&fixture.path)["requests_used"], 1);
    assert!(captures.try_recv().is_err());
}

async fn precommit_error_precedes_contended_accounting(transport_error: bool) {
    let (fixture, _server, chunks, mut captures) = fixture(|settings| {
        settings.max_in_flight_requests = 1;
        settings.first_byte_timeout_seconds = 2;
    })
    .await;
    let token = token(&fixture).await;
    let pending = tokio::spawn(send(fixture.app.clone(), chat(payload(), &token)));
    // Capture proves admission has committed; no upstream body byte is released.
    bounded(captures.recv()).await.unwrap();
    let writer = Connection::open(&fixture.path).unwrap();
    writer.execute_batch("BEGIN IMMEDIATE").unwrap();
    assert_eq!(accounting_record(&fixture), ("pending".to_owned(), None));
    if transport_error {
        chunks
            .send(Err(io::Error::other("controlled pre-first-byte failure")))
            .await
            .unwrap();
    }
    let (status, headers, body) = tokio::time::timeout(Duration::from_secs(4), pending)
        .await
        .expect("pre-commit error waited for the SQLite writer")
        .unwrap();
    assert_eq!(
        status,
        if transport_error {
            StatusCode::BAD_GATEWAY
        } else {
            StatusCode::GATEWAY_TIMEOUT
        }
    );
    assert_eq!(headers[header::CACHE_CONTROL], "no-store");
    assert_private_error(&body);
    assert_eq!(
        body["error"]["message"],
        if transport_error {
            "Upstream request failed"
        } else {
            "Upstream request timed out"
        }
    );
    bounded(chunks.closed()).await;
    assert_eq!(accounting_record(&fixture), ("pending".to_owned(), None));
    assert_eq!(persisted(&fixture.path)["tokens_used"], 0);
    let (status, headers, error) =
        bounded(send(fixture.app.clone(), chat(payload(), &token))).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(headers[header::RETRY_AFTER], "1");
    assert_private_error(&error);
    assert_eq!(persisted(&fixture.path)["requests_used"], 1);
    assert!(captures.try_recv().is_err());

    writer.execute_batch("COMMIT").unwrap();
    wait_for_accounting(&fixture).await;
    assert_eq!(
        accounting_record(&fixture),
        (
            if transport_error {
                "upstream_error"
            } else {
                "timeout"
            }
            .to_owned(),
            None
        )
    );
    let usage: (Option<i64>, Option<i64>, Option<i64>) = writer
        .query_row(
            "SELECT prompt_tokens, completion_tokens, total_tokens
             FROM request_records ORDER BY request_id LIMIT 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(usage, (None, None, None));
    recovered(&fixture, &token).await;
    assert_eq!(persisted(&fixture.path)["requests_used"], 2);
    assert_eq!(persisted(&fixture.path)["tokens_used"], 0);
    bounded(captures.recv()).await.unwrap();
    assert!(captures.try_recv().is_err());
}

#[tokio::test]
async fn first_byte_timeout_returns_while_sqlite_writer_holds_accounting() {
    precommit_error_precedes_contended_accounting(false).await;
}

#[tokio::test]
async fn pre_first_byte_transport_error_returns_while_sqlite_writer_holds_accounting() {
    precommit_error_precedes_contended_accounting(true).await;
}

#[tokio::test]
async fn first_byte_deadline_includes_headers_without_body_and_does_not_retry() {
    let (fixture, _server, chunks, mut captures) = fixture(|settings| {
        settings.first_byte_timeout_seconds = 1;
    })
    .await;
    let token = token(&fixture).await;
    let pending = tokio::spawn(send(fixture.app.clone(), chat(payload(), &token)));
    bounded(captures.recv()).await.unwrap();
    let (status, headers, body) = bounded(pending).await.unwrap();
    assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
    assert_eq!(headers[header::CACHE_CONTROL], "no-store");
    assert_private_error(&body);
    assert_eq!(body["error"]["message"], "Upstream request timed out");
    bounded(chunks.closed()).await;
    assert!(captures.try_recv().is_err());
    assert_eq!(persisted(&fixture.path)["requests_used"], 1);
    wait_for_accounting(&fixture).await;
    assert_eq!(accounting_record(&fixture), ("timeout".to_owned(), None));
}

#[tokio::test]
async fn idle_deadline_after_commit_is_a_transport_error_not_an_sse_event() {
    let (fixture, _server, chunks, mut captures) = fixture(|settings| {
        settings.idle_timeout_seconds = 1;
    })
    .await;
    let token = token(&fixture).await;
    chunks
        .send(Ok(Bytes::from_static(b"data: first\n\n")))
        .await
        .unwrap();
    let response = bounded(fixture.app.clone().oneshot(chat(payload(), &token)))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let mut body = response.into_body().into_data_stream();
    assert_eq!(
        bounded(body.next()).await.unwrap().unwrap(),
        "data: first\n\n"
    );
    let error = bounded(body.next())
        .await
        .expect("timeout was a clean EOF")
        .expect_err("timeout was encoded as a successful SSE event");
    assert!(!error.to_string().contains(UPSTREAM_SECRET));
    bounded(chunks.closed()).await;
    bounded(captures.recv()).await.unwrap();
    assert!(captures.try_recv().is_err());
    assert_eq!(persisted(&fixture.path)["requests_used"], 1);
    wait_for_accounting(&fixture).await;
    assert_eq!(accounting_record(&fixture), ("timeout".to_owned(), None));
}

#[tokio::test]
async fn overall_deadline_does_not_reset_when_upstream_keeps_producing_chunks() {
    let (fixture, _server, chunks, mut captures) = fixture(|settings| {
        settings.overall_timeout_seconds = 1;
        settings.idle_timeout_seconds = 2;
    })
    .await;
    let token = token(&fixture).await;
    let producer = tokio::spawn(async move {
        loop {
            if chunks
                .send(Ok(Bytes::from_static(b"data: active\n\n")))
                .await
                .is_err()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    });
    let response = bounded(fixture.app.clone().oneshot(chat(payload(), &token)))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let mut body = response.into_body().into_data_stream();
    let count = bounded(async {
        let mut count = 0;
        loop {
            match body.next().await {
                Some(Ok(bytes)) => {
                    assert_eq!(bytes, "data: active\n\n");
                    count += 1;
                }
                Some(Err(_)) => break count,
                None => panic!("overall timeout was a clean EOF"),
            }
        }
    })
    .await;
    assert!(
        count >= 2,
        "upstream did not remain active before overall timeout"
    );
    bounded(producer).await.unwrap();
    bounded(captures.recv()).await.unwrap();
    assert!(captures.try_recv().is_err());
}

#[tokio::test]
async fn overall_deadline_covers_stalled_incoming_body_and_releases_capacity() {
    let (fixture, _server, _chunks, mut captures) = fixture(|settings| {
        settings.overall_timeout_seconds = 1;
        settings.max_in_flight_requests = 1;
    })
    .await;
    let token = token(&fixture).await;
    let body = Body::from_stream(futures_util::stream::pending::<Chunk>());
    let (status, _, body) = bounded(send(fixture.app.clone(), chat(body, &token))).await;
    assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
    assert_private_error(&body);
    assert!(captures.try_recv().is_err());
    assert_eq!(persisted(&fixture.path)["requests_used"], 1);
    wait_for_accounting(&fixture).await;
    // A malformed request needs no upstream stream but still must acquire capacity.
    let (status, _, _) = bounded(send(fixture.app.clone(), chat("{", &token))).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(persisted(&fixture.path)["requests_used"], 2);
}

#[tokio::test]
async fn saturated_capacity_is_not_charged_and_dropped_body_recovers_it() {
    let (fixture, _server, chunks, mut captures) = fixture(|settings| {
        settings.max_in_flight_requests = 1;
    })
    .await;
    let token = token(&fixture).await;
    chunks
        .send(Ok(Bytes::from_static(b"data: first\n\n")))
        .await
        .unwrap();
    let response = bounded(fixture.app.clone().oneshot(chat(payload(), &token)))
        .await
        .unwrap();
    bounded(captures.recv()).await.unwrap();
    let (status, headers, body) = bounded(send(fixture.app.clone(), chat(payload(), &token))).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(headers[header::RETRY_AFTER], "1");
    assert_eq!(headers[header::CACHE_CONTROL], "no-store");
    assert_private_error(&body);
    assert_eq!(persisted(&fixture.path)["requests_used"], 1);
    assert!(captures.try_recv().is_err());
    // Authentication still precedes capacity, even when all permits are held.
    let mut invalid = chat(payload(), &token);
    invalid.headers_mut().remove(header::AUTHORIZATION);
    assert_eq!(
        bounded(send(fixture.app.clone(), invalid)).await.0,
        StatusCode::UNAUTHORIZED
    );
    let health = Request::builder()
        .uri("/healthz")
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        bounded(fixture.app.clone().oneshot(health))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    drop(response);
    bounded(chunks.closed()).await;
    recovered(&fixture, &token).await;
    assert_eq!(persisted(&fixture.path)["requests_used"], 2);
}

#[tokio::test]
async fn overall_timeout_releases_capacity_even_if_downstream_never_polls() {
    let (fixture, _server, chunks, mut captures) = fixture(|settings| {
        settings.max_in_flight_requests = 1;
        settings.overall_timeout_seconds = 1;
    })
    .await;
    let token = token(&fixture).await;
    chunks
        .send(Ok(Bytes::from_static(b"data: first\n\n")))
        .await
        .unwrap();
    let response = bounded(fixture.app.clone().oneshot(chat(payload(), &token)))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    bounded(captures.recv()).await.unwrap();
    let producer = tokio::spawn(async move {
        while chunks
            .send(Ok(Bytes::from(vec![b'x'; 64 * 1024])))
            .await
            .is_ok()
        {}
    });
    // Keep response alive without polling its body, filling every bounded queue.
    bounded(producer).await.unwrap();
    recovered(&fixture, &token).await;
    assert_eq!(persisted(&fixture.path)["requests_used"], 2);
    let mut body = response.into_body().into_data_stream();
    bounded(async {
        loop {
            match body.next().await {
                Some(Ok(_)) => {}
                Some(Err(_)) => break,
                None => panic!("unpolled timeout became a clean EOF"),
            }
        }
    })
    .await;
}

#[tokio::test]
async fn actual_http_disconnect_after_commit_closes_upstream_and_recovers_capacity() {
    let (fixture, _upstream, chunks, mut captures) = fixture(|settings| {
        settings.max_in_flight_requests = 1;
    })
    .await;
    let token = token(&fixture).await;
    let server = serve(fixture.app.clone()).await;
    chunks
        .send(Ok(Bytes::from_static(b"data: first\n\n")))
        .await
        .unwrap();
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    let mut response = bounded(
        client
            .post(format!("http://{}/v1/chat/completions", server.address))
            .bearer_auth(&token)
            .header(header::CONTENT_TYPE, "application/json")
            .body(payload())
            .send(),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        bounded(response.chunk()).await.unwrap().unwrap(),
        "data: first\n\n"
    );
    bounded(captures.recv()).await.unwrap();
    drop(response);
    bounded(chunks.closed()).await;
    recovered(&fixture, &token).await;
    assert_eq!(persisted(&fixture.path)["requests_used"], 2);
}

#[tokio::test]
async fn actual_http_disconnect_before_first_byte_closes_upstream_and_recovers_capacity() {
    use tokio::io::AsyncWriteExt;
    let (fixture, _upstream, chunks, mut captures) = fixture(|settings| {
        settings.max_in_flight_requests = 1;
    })
    .await;
    let token = token(&fixture).await;
    let server = serve(fixture.app.clone()).await;
    let mut socket = tokio::net::TcpStream::connect(server.address)
        .await
        .unwrap();
    let body = payload();
    let request = format!(
        "POST /v1/chat/completions HTTP/1.1\r\nHost: {}\r\nAuthorization: Bearer {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
        server.address,
        token,
        body.len(),
        body,
    );
    socket.write_all(request.as_bytes()).await.unwrap();
    // Admission and upstream dispatch have happened, but no body byte exists.
    bounded(captures.recv()).await.unwrap();
    drop(socket);
    bounded(chunks.closed()).await;
    recovered(&fixture, &token).await;
    assert_eq!(persisted(&fixture.path)["requests_used"], 2);
}

#[tokio::test]
async fn connect_deadline_includes_tls_handshake_without_retrying() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let connections = Arc::new(AtomicUsize::new(0));
    let observed = connections.clone();
    let task = tokio::spawn(async move {
        let mut sockets = Vec::new();
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            observed.fetch_add(1, Ordering::SeqCst);
            // Accept TCP but never answer the TLS ClientHello.
            sockets.push(socket);
        }
    });
    let _server = Server { address, task };
    let mut fixture = Fixture::new();
    let mut settings = config(fixture.path.clone());
    settings.upstream_base_url = format!("https://{address}").parse().unwrap();
    settings.connect_timeout_seconds = 1;
    settings.first_byte_timeout_seconds = 30;
    settings.overall_timeout_seconds = 30;
    fixture.app = router(settings, Store::open(&fixture.path).unwrap()).unwrap();
    let token = token(&fixture).await;
    let (status, _, body) = bounded(send(fixture.app.clone(), chat(payload(), &token))).await;
    assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
    assert_private_error(&body);
    assert_eq!(connections.load(Ordering::SeqCst), 1);
    assert_eq!(persisted(&fixture.path)["requests_used"], 1);
    wait_for_accounting(&fixture).await;
    assert_eq!(accounting_record(&fixture), ("timeout".to_owned(), None));
}

fn blocked_admission_retains_capacity(timeout: bool) {
    // Keep admission queued deterministically, without sleeps or production hooks.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    runtime.block_on(async {
        let (fixture, _server, _chunks, mut captures) = fixture(|settings| {
            settings.max_in_flight_requests = 1;
            settings.overall_timeout_seconds = 1;
        })
        .await;
        let token = token(&fixture).await;
        let (release, wait) = std::sync::mpsc::channel::<()>();
        let (started, entered) = tokio::sync::oneshot::channel();
        let blocker = tokio::task::spawn_blocking(move || {
            let _ = started.send(());
            // Dropping release also unblocks this worker during panic cleanup.
            let _ = wait.recv();
        });
        bounded(entered).await.unwrap();

        let mut pending = Box::pin(fixture.app.clone().oneshot(chat(payload(), &token)));
        assert!(futures_util::poll!(pending.as_mut()).is_pending());
        if timeout {
            assert_eq!(
                bounded(pending).await.unwrap().status(),
                StatusCode::GATEWAY_TIMEOUT
            );
        } else {
            drop(pending);
        }

        let (status, headers, _) = bounded(send(fixture.app.clone(), chat("{", &token))).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(headers[header::RETRY_AFTER], "1");
        assert_eq!(persisted(&fixture.path)["requests_used"], 0);
        assert!(captures.try_recv().is_err());

        drop(release);
        bounded(blocker).await.unwrap();
        bounded(async {
            loop {
                let (status, _, _) = send(fixture.app.clone(), chat("{", &token)).await;
                if status == StatusCode::SERVICE_UNAVAILABLE {
                    tokio::task::yield_now().await;
                    continue;
                }
                assert_eq!(status, StatusCode::BAD_REQUEST);
                break;
            }
        })
        .await;
        // The abandoned admission still charges once, but never forwards. Only
        // the subsequent malformed request consumes another slot after recovery.
        assert_eq!(persisted(&fixture.path)["requests_used"], 2);
        assert_eq!(
            accounting_record(&fixture),
            ("cancelled".to_owned(), Some(0))
        );
        assert!(captures.try_recv().is_err());
    });
}

#[test]
fn cancelled_blocking_admission_remains_in_flight_until_worker_finishes() {
    blocked_admission_retains_capacity(false);
}

#[test]
fn timed_out_blocking_admission_remains_in_flight_until_worker_finishes() {
    blocked_admission_retains_capacity(true);
}

fn blocked_reservation_narrowing_retains_capacity(timeout: bool) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    runtime.block_on(async {
        let (fixture, _server, _chunks, mut captures) = fixture(|settings| {
            settings.max_in_flight_requests = 1;
            settings.overall_timeout_seconds = 1;
        })
        .await;
        let token = full_budget_token(&fixture).await;
        let (body_ready, body_entered) = tokio::sync::oneshot::channel();
        let mut body_ready = Some(body_ready);
        let (release_body, mut incoming) = mpsc::channel::<Chunk>(1);
        let body = Body::from_stream(futures_util::stream::poll_fn(move |context| {
            if let Some(ready) = body_ready.take() {
                let _ = ready.send(());
            }
            incoming.poll_recv(context)
        }));
        let mut pending = Box::pin(fixture.app.clone().oneshot(chat(body, &token)));
        bounded(async {
            tokio::select! {
                entered = body_entered => entered.unwrap(),
                result = pending.as_mut() => panic!("request finished before reading its body: {result:?}"),
            }
        })
        .await;
        // Body polling happens only after the first transaction commits. This
        // lock therefore blocks reservation narrowing, not initial admission.
        assert_eq!(persisted(&fixture.path)["requests_used"], 1);
        assert_eq!(token_balances(&fixture), (0, 0, STREAM_BUDGET));
        let writer = Connection::open(&fixture.path).unwrap();
        writer.execute_batch("BEGIN IMMEDIATE").unwrap();
        let mut body: Value = serde_json::from_str(&payload()).unwrap();
        body["max_tokens"] = json!(1);
        release_body
            .send(Ok(Bytes::from(body.to_string())))
            .await
            .unwrap();
        drop(release_body);
        // Both the bytes and EOF are ready, so this poll runs synchronously
        // through body parsing to spawning the second transaction's worker.
        assert!(futures_util::poll!(pending.as_mut()).is_pending());
        assert_eq!(accounting_record(&fixture), ("pending".to_owned(), None));
        assert_eq!(token_balances(&fixture), (0, 0, STREAM_BUDGET));
        if timeout {
            assert_eq!(
                bounded(pending).await.unwrap().status(),
                StatusCode::GATEWAY_TIMEOUT
            );
        } else {
            drop(pending);
        }
        let (status, headers, _) =
            bounded(send(fixture.app.clone(), chat("{", &token))).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(headers[header::RETRY_AFTER], "1");
        assert_eq!(accounting_record(&fixture), ("pending".to_owned(), None));
        assert_eq!(token_balances(&fixture), (0, 0, STREAM_BUDGET));
        assert_eq!(persisted(&fixture.path)["requests_used"], 1);
        assert!(captures.try_recv().is_err());

        writer.execute_batch("COMMIT").unwrap();
        bounded(async {
            loop {
                let (status, _, _) = send(fixture.app.clone(), chat("{", &token)).await;
                if status == StatusCode::SERVICE_UNAVAILABLE {
                    tokio::task::yield_now().await;
                    continue;
                }
                assert_eq!(status, StatusCode::BAD_REQUEST);
                break;
            }
        })
        .await;
        wait_for_accounting(&fixture).await;
        assert_eq!(accounting_record(&fixture), ("cancelled".to_owned(), Some(0)));
        assert_eq!(token_balances(&fixture), (0, 0, 0));
        assert_eq!(persisted(&fixture.path)["requests_used"], 2);
        assert_eq!(persisted(&fixture.path)["status"], "active");
        let request_id = writer
            .query_row(
                "SELECT request_id FROM request_records ORDER BY request_id LIMIT 1",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap();
        Store::open(&fixture.path)
            .unwrap()
            .finish_request(
                request_id,
                "cancelled",
                Some(crate::store::Usage {
                    prompt_tokens: 0,
                    completion_tokens: 0,
                    total_tokens: 0,
                }),
                true,
            )
            .unwrap();
        assert_eq!(token_balances(&fixture), (0, 0, 0));
        assert_eq!(persisted(&fixture.path)["requests_used"], 2);
        assert!(captures.try_recv().is_err());
    });
}

#[test]
fn cancelled_reservation_narrowing_retains_capacity_until_worker_finishes() {
    blocked_reservation_narrowing_retains_capacity(false);
}

#[test]
fn timed_out_reservation_narrowing_retains_capacity_until_worker_finishes() {
    blocked_reservation_narrowing_retains_capacity(true);
}

fn accounting_record(fixture: &Fixture) -> (String, Option<i64>) {
    Connection::open(&fixture.path)
        .unwrap()
        .query_row(
            "SELECT outcome, total_tokens FROM request_records ORDER BY request_id LIMIT 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap()
}

#[tokio::test]
async fn streaming_usage_reconciles_after_done_without_double_counting_reports() {
    let (fixture, _server, chunks, mut captures) = fixture(|_| {}).await;
    let token = token(&fixture).await;
    let pending = tokio::spawn(fixture.app.clone().oneshot(chat(payload(), &token)));
    bounded(captures.recv()).await.unwrap();
    chunks
        .send(Ok(Bytes::from_static(b"data: {\"usage\":null}\n\n")))
        .await
        .unwrap();
    let response = bounded(pending).await.unwrap().unwrap();
    let mut body = response.into_body().into_data_stream();
    bounded(body.next()).await.unwrap().unwrap();
    assert_eq!(accounting_record(&fixture), ("pending".to_owned(), None));
    let mut done = request(Body::empty(), Some(CONTROL_TOKEN));
    *done.uri_mut() = format!("/v1/jobs/{JOB_ID}/done").parse().unwrap();
    assert_eq!(
        fixture.app.clone().oneshot(done).await.unwrap().status(),
        StatusCode::NO_CONTENT
    );
    let report =
        b"data: {\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":3,\"total_tokens\":8}}\n\n";
    // Cumulative duplicate reports must not charge twice; frame boundaries need
    // not match transport chunks.
    for part in [
        &report[..17],
        &report[17..],
        &report[..],
        b"data: [DONE]\n\n",
    ] {
        chunks.send(Ok(Bytes::copy_from_slice(part))).await.unwrap();
        bounded(body.next()).await.unwrap().unwrap();
    }
    drop(chunks);
    assert!(bounded(body.next()).await.is_none());
    wait_for_accounting(&fixture).await;
    assert_eq!(
        accounting_record(&fixture),
        ("completed".to_owned(), Some(8))
    );
    assert_eq!(persisted(&fixture.path)["tokens_used"], 8);
    assert_eq!(persisted(&fixture.path)["status"], "done");
}

#[tokio::test]
async fn broken_stream_usage_is_unknown_unless_a_complete_report_was_received() {
    for known in [false, true] {
        let (fixture, _server, chunks, mut captures) = fixture(|_| {}).await;
        let token = full_budget_token(&fixture).await;
        let pending = tokio::spawn(fixture.app.clone().oneshot(chat(payload(), &token)));
        bounded(captures.recv()).await.unwrap();
        let first = if known {
            b"data: {\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":3,\"total_tokens\":8}}\n\n".as_slice()
        } else {
            b"data: {\"usage\":{\"prompt_tokens\":5,".as_slice()
        };
        chunks
            .send(Ok(Bytes::copy_from_slice(first)))
            .await
            .unwrap();
        let response = bounded(pending).await.unwrap().unwrap();
        let mut body = response.into_body().into_data_stream();
        assert_eq!(bounded(body.next()).await.unwrap().unwrap().as_ref(), first);
        chunks
            .send(Err(io::Error::other("controlled broken stream")))
            .await
            .unwrap();
        assert!(bounded(body.next()).await.unwrap().is_err());
        wait_for_accounting(&fixture).await;
        assert_eq!(
            accounting_record(&fixture),
            (
                "upstream_error".to_owned(),
                if known { Some(8) } else { None }
            )
        );
        assert_eq!(
            persisted(&fixture.path)["tokens_used"],
            if known { 8 } else { 0 }
        );
        conservative_settlement_survives_duplicate_and_restart(
            &fixture,
            if known { Some(8) } else { None },
        );
        budget_exhausted(&fixture, &token).await;
        assert!(captures.try_recv().is_err());
    }
}

#[tokio::test]
async fn partial_sse_usage_never_refunds_an_interrupted_full_budget() {
    for ending in ["clean_eof", "transport_error", "disconnect"] {
        let (fixture, _server, chunks, mut captures) = fixture(|settings| {
            settings.max_in_flight_requests = 1;
        })
        .await;
        let token = full_budget_token(&fixture).await;
        let report =
            b"data: {\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":3,\"total_tokens\":8}}\n\n";
        chunks.send(Ok(Bytes::from_static(report))).await.unwrap();
        let response = bounded(fixture.app.clone().oneshot(chat(payload(), &token)))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        bounded(captures.recv()).await.unwrap();
        assert_eq!(token_balances(&fixture), (0, 0, STREAM_BUDGET));
        let mut body = response.into_body().into_data_stream();
        assert_eq!(
            bounded(body.next()).await.unwrap().unwrap().as_ref(),
            report
        );
        // Duplicate cumulative snapshots cannot double-charge known usage.
        chunks.send(Ok(Bytes::from_static(report))).await.unwrap();
        assert_eq!(
            bounded(body.next()).await.unwrap().unwrap().as_ref(),
            report
        );
        let partial =
            b"data: {\"usage\":{\"prompt_tokens\":8,\"completion_tokens\":5,\"total_tokens\":13}}\n";
        chunks.send(Ok(Bytes::from_static(partial))).await.unwrap();
        assert_eq!(
            bounded(body.next()).await.unwrap().unwrap().as_ref(),
            partial
        );
        assert_eq!(accounting_record(&fixture), ("pending".to_owned(), None));
        match ending {
            "clean_eof" => {
                drop(chunks);
                assert!(bounded(body.next()).await.is_none());
            }
            "transport_error" => {
                chunks
                    .send(Err(io::Error::other("controlled interrupted usage")))
                    .await
                    .unwrap();
                assert!(bounded(body.next()).await.unwrap().is_err());
            }
            "disconnect" => {
                drop(body);
                bounded(chunks.closed()).await;
            }
            _ => unreachable!(),
        }
        wait_for_accounting(&fixture).await;
        let outcome = match ending {
            "clean_eof" => "completed",
            "transport_error" => "upstream_error",
            _ => "disconnected",
        };
        assert_eq!(accounting_record(&fixture), (outcome.to_owned(), Some(8)));
        conservative_settlement_survives_duplicate_and_restart(&fixture, Some(8));
        budget_exhausted(&fixture, &token).await;
        assert!(captures.try_recv().is_err());
    }
}

#[tokio::test]
async fn disconnect_reconciles_unknown_usage_and_releases_capacity() {
    let (fixture, _server, chunks, mut captures) = fixture(|settings| {
        settings.max_in_flight_requests = 1;
    })
    .await;
    let token = token(&fixture).await;
    let pending = tokio::spawn(fixture.app.clone().oneshot(chat(payload(), &token)));
    bounded(captures.recv()).await.unwrap();
    chunks
        .send(Ok(Bytes::from_static(b"data: {\"choices\":[]}\n\n")))
        .await
        .unwrap();
    let response = bounded(pending).await.unwrap().unwrap();
    drop(response);
    bounded(chunks.closed()).await;
    recovered(&fixture, &token).await;
    assert_eq!(
        accounting_record(&fixture),
        ("disconnected".to_owned(), None)
    );
    assert_eq!(persisted(&fixture.path)["tokens_used"], 0);
}

#[tokio::test]
async fn usage_in_first_upstream_chunk_survives_disconnect_before_body_polling() {
    let (fixture, _server, chunks, mut captures) = fixture(|settings| {
        settings.max_in_flight_requests = 1;
    })
    .await;
    let token = full_budget_token(&fixture).await;
    chunks.send(Ok(Bytes::from_static(
        b"data: {\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":3,\"total_tokens\":8}}\n\n"
    ))).await.unwrap();
    // Await the handler directly on this current-thread runtime. The response
    // is dropped before the newly spawned relay can be scheduled.
    let response = bounded(fixture.app.clone().oneshot(chat(payload(), &token)))
        .await
        .unwrap();
    drop(response);
    bounded(chunks.closed()).await;
    wait_for_accounting(&fixture).await;
    assert_eq!(
        accounting_record(&fixture),
        ("disconnected".to_owned(), Some(8))
    );
    assert_eq!(persisted(&fixture.path)["tokens_used"], 8);
    conservative_settlement_survives_duplicate_and_restart(&fixture, Some(8));
    budget_exhausted(&fixture, &token).await;
    bounded(captures.recv()).await.unwrap();
    assert!(captures.try_recv().is_err());
}

#[test]
fn background_finalization_keeps_timeout_outcome_when_runtime_shuts_down_immediately() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let fixture = runtime.block_on(async {
        let fixture = Fixture::new();
        let token = token(&fixture).await;
        let store = Store::open(&fixture.path).unwrap();
        let JobAdmission::Admitted(job) = store
            .admit_job(Sha256::digest(token.as_bytes()).into())
            .unwrap()
        else {
            panic!("request was not admitted");
        };
        let capacity = Arc::new(Semaphore::new(1));
        let mut accounting = accounting::Accounting::new(
            store,
            job.request_id,
            capacity.try_acquire_owned().unwrap(),
        );
        accounting.dispatched();
        accounting.finish_in_background("timeout", None, false);
        // No executor yield after dispatch: an intermediate async task could be
        // cancelled before it transfers the intended outcome to a worker.
        fixture
    });
    drop(runtime);
    assert_eq!(accounting_record(&fixture), ("timeout".to_owned(), None));
    assert_eq!(persisted(&fixture.path)["requests_used"], 1);
}

fn shutdown_after_observed_stream(
    first: &'static [u8],
    later: Option<&'static [u8]>,
    expected: Option<i64>,
) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let (fixture, _server, _chunks, _body, token) = runtime.block_on(async {
        let (fixture, server, chunks, _captures) = fixture(|settings| {
            settings.max_in_flight_requests = 1;
        })
        .await;
        let token = full_budget_token(&fixture).await;
        chunks.send(Ok(Bytes::from_static(first))).await.unwrap();
        let response = bounded(fixture.app.clone().oneshot(chat(payload(), &token)))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let mut body = response.into_body().into_data_stream();
        assert_eq!(bounded(body.next()).await.unwrap().unwrap().as_ref(), first);
        if let Some(later) = later {
            chunks.send(Ok(Bytes::from_static(later))).await.unwrap();
            assert_eq!(bounded(body.next()).await.unwrap().unwrap().as_ref(), later);
        }
        assert_eq!(accounting_record(&fixture), ("pending".to_owned(), None));
        // Keep upstream and downstream open outside block_on. The runtime,
        // rather than a disconnect or EOF, must cancel the unfinished relay.
        (fixture, server, chunks, body, token)
    });
    drop(runtime);
    assert_eq!(
        accounting_record(&fixture),
        ("cancelled".to_owned(), expected)
    );
    assert_eq!(
        persisted(&fixture.path)["tokens_used"],
        expected.unwrap_or(0)
    );
    assert_eq!(persisted(&fixture.path)["requests_used"], 1);
    conservative_settlement_survives_duplicate_and_restart(&fixture, expected);
    let restarted = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    restarted.block_on(budget_exhausted(&fixture, &token));
}

#[test]
fn runtime_shutdown_preserves_usage_in_first_stream_chunk() {
    shutdown_after_observed_stream(
        b"data: {\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":3,\"total_tokens\":8}}\n\n",
        None,
        Some(8),
    );
}

#[test]
fn runtime_shutdown_preserves_usage_in_later_stream_chunk() {
    shutdown_after_observed_stream(
        b"data: {\"choices\":[]}\n\n",
        Some(b"data: {\"usage\":{\"prompt_tokens\":8,\"completion_tokens\":5,\"total_tokens\":13}}\n\n"),
        Some(13),
    );
}

#[test]
fn runtime_shutdown_counts_latest_complete_report_once() {
    shutdown_after_observed_stream(
        b"data: {\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":3,\"total_tokens\":8}}\n\n",
        Some(b"data: {\"usage\":{\"prompt_tokens\":8,\"completion_tokens\":5,\"total_tokens\":13}}\n\n"),
        Some(13),
    );
}

#[test]
fn runtime_shutdown_keeps_last_complete_usage_before_partial_report() {
    shutdown_after_observed_stream(
        b"data: {\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":3,\"total_tokens\":8}}\n\n",
        Some(b"data: {\"usage\":{\"prompt_tokens\":8,\"completion_tokens\":5,\"total_tokens\":13}}\n"),
        Some(8),
    );
}

#[test]
fn runtime_shutdown_leaves_incomplete_usage_unknown() {
    shutdown_after_observed_stream(
        b"data: {\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":3,\"total_tokens\":8}}\n",
        None,
        None,
    );
}

#[test]
fn rejected_shutdown_worker_still_persists_observed_usage() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let handle = runtime.handle().clone();
    let (fixture, accounting, capacity) = runtime.block_on(async {
        let fixture = Fixture::new();
        let token = full_budget_token(&fixture).await;
        let store = Store::open(&fixture.path).unwrap();
        let JobAdmission::Admitted(job) = store
            .admit_job(Sha256::digest(token.as_bytes()).into())
            .unwrap()
        else {
            panic!("request was not admitted");
        };
        let capacity = Arc::new(Semaphore::new(1));
        let mut accounting = accounting::Accounting::new(
            store,
            job.request_id,
            capacity.clone().try_acquire_owned().unwrap(),
        );
        accounting.dispatched();
        accounting.observe_usage(Some(crate::store::Usage {
            prompt_tokens: 5,
            completion_tokens: 3,
            total_tokens: 8,
        }));
        (fixture, accounting, capacity)
    });
    drop(runtime);
    // Model relay cancellation after the blocking pool has closed: the
    // finalizer's queued closure is discarded without ever being executed.
    let _entered = handle.enter();
    drop(accounting);
    assert_eq!(
        accounting_record(&fixture),
        ("cancelled".to_owned(), Some(8))
    );
    assert_eq!(persisted(&fixture.path)["tokens_used"], 8);
    conservative_settlement_survives_duplicate_and_restart(&fixture, Some(8));
    assert_eq!(capacity.available_permits(), 1);
}

#[tokio::test]
async fn sse_token_escrow_counts_choices_and_settles_known_or_missing_usage() {
    for known in [false, true] {
        let (fixture, _server, chunks, mut captures) = fixture(|_| {}).await;
        let mut body: Value = serde_json::from_str(&payload()).unwrap();
        body["n"] = json!(3);
        body["max_completion_tokens"] = json!(100_000);
        let mut expected = body.clone();
        expected
            .as_object_mut()
            .unwrap()
            .remove("max_completion_tokens");
        expected["messages"].as_array_mut().unwrap().insert(
            0,
            json!({"role":"system","content":valid_body()["system_prompt"]}),
        );
        expected["stream_options"] = json!({"include_usage":true});
        let prompt_bound = serde_json::to_vec(&expected).unwrap().len() as i64
            + 256
            + 32 * expected["messages"].as_array().unwrap().len() as i64;
        let allowance = prompt_bound + 3 * 17;
        let mut job = valid_body();
        job["max_tokens_total"] = json!(allowance);
        let (status, _, issued) = issue(fixture.app.clone(), job).await;
        assert_eq!(status, StatusCode::CREATED);
        let token = issued["token"].as_str().unwrap();
        let pending = tokio::spawn(fixture.app.clone().oneshot(chat(body.to_string(), token)));
        let forwarded = bounded(captures.recv()).await.unwrap();
        expected["max_completion_tokens"] = json!(17);
        assert_eq!(forwarded, expected);
        let balances = || {
            Connection::open(&fixture.path)
                .unwrap()
                .query_row(
                    "SELECT tokens_used, tokens_unreported, tokens_reserved FROM jobs",
                    [],
                    |row| {
                        Ok((
                            row.get::<_, i64>(0)?,
                            row.get::<_, i64>(1)?,
                            row.get::<_, i64>(2)?,
                        ))
                    },
                )
                .unwrap()
        };
        assert_eq!(balances(), (0, 0, allowance));
        assert_eq!(persisted(&fixture.path)["status"], "active");
        let first = if known {
            b"data: {\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":3,\"total_tokens\":8}}\n\n".as_slice()
        } else {
            b"data: {\"choices\":[]}\n\n".as_slice()
        };
        chunks
            .send(Ok(Bytes::copy_from_slice(first)))
            .await
            .unwrap();
        let response = bounded(pending).await.unwrap().unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let mut stream = response.into_body().into_data_stream();
        assert_eq!(
            bounded(stream.next()).await.unwrap().unwrap().as_ref(),
            first
        );
        assert_eq!(balances(), (0, 0, allowance));
        // Even repeated cumulative usage reports must refund escrow only once.
        if known {
            chunks
                .send(Ok(Bytes::copy_from_slice(first)))
                .await
                .unwrap();
            assert_eq!(
                bounded(stream.next()).await.unwrap().unwrap().as_ref(),
                first
            );
        }
        chunks
            .send(Ok(Bytes::from_static(b"data: [DONE]\n\n")))
            .await
            .unwrap();
        assert_eq!(
            bounded(stream.next()).await.unwrap().unwrap(),
            "data: [DONE]\n\n"
        );
        drop(chunks);
        assert!(bounded(stream.next()).await.is_none());
        wait_for_accounting(&fixture).await;
        assert_eq!(
            accounting_record(&fixture),
            ("completed".to_owned(), if known { Some(8) } else { None })
        );
        assert_eq!(
            balances(),
            if known { (8, 0, 0) } else { (0, allowance, 0) }
        );
        assert_eq!(persisted(&fixture.path)["requests_used"], 1);
        if known {
            assert_eq!(persisted(&fixture.path)["status"], "active");
            let (status, _, result) =
                bounded(send(fixture.app.clone(), chat(body.to_string(), token))).await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(result, json!({"recovered":true}));
            let forwarded = bounded(captures.recv()).await.unwrap();
            assert_eq!(
                forwarded["max_completion_tokens"],
                (allowance - 8 - prompt_bound) / 3
            );
            wait_for_accounting(&fixture).await;
        } else {
            assert_eq!(persisted(&fixture.path)["status"], "exhausted");
            let unread =
                Body::from_stream(futures_util::stream::pending::<Result<String, io::Error>>());
            let (status, headers, error) =
                bounded(send(fixture.app.clone(), chat(unread, token))).await;
            assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
            assert_eq!(headers[header::CACHE_CONTROL], "no-store");
            assert_private_error(&error);
            assert_eq!(persisted(&fixture.path)["requests_used"], 1);
            assert_eq!(balances(), (0, allowance, 0));
        }
        assert!(captures.try_recv().is_err());
    }
}
