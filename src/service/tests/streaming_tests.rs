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
    assert_eq!(persisted(&fixture.path)["requests_used"], 1);
    assert!(captures.try_recv().is_err());
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
}
