use super::*;
use std::{
    future::Future,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};
use tokio::{net::TcpListener, sync::oneshot, task::JoinHandle};

async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .expect("lifecycle request did not complete")
}

fn lifecycle(job_id: &str, action: &str, token: Option<&str>, body: Body) -> Request<Body> {
    let mut request = Request::builder()
        .method("POST")
        .uri(format!("/v1/jobs/{job_id}/{action}"));
    if let Some(token) = token {
        request = request.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }
    request.body(body).unwrap()
}

fn unread_body() -> Body {
    Body::from_stream(futures_util::stream::pending::<
        Result<String, std::io::Error>,
    >())
}

fn chat(token: &str, body: Body) -> Request<Body> {
    let mut request = request(body, Some(token));
    *request.uri_mut() = "/v1/chat/completions".parse().unwrap();
    request
}

async fn issued_token(fixture: &Fixture) -> String {
    let (status, _, body) = issue(fixture.app.clone(), valid_body()).await;
    assert_eq!(status, StatusCode::CREATED);
    body["token"].as_str().unwrap().to_owned()
}

async fn assert_no_content(app: Router, request: Request<Body>) {
    let response = bounded(app.oneshot(request)).await.unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
    assert!(
        bounded(to_bytes(response.into_body(), MAX_BODY_BYTES))
            .await
            .unwrap()
            .is_empty()
    );
}

async fn assert_error(app: Router, request: Request<Body>, expected: StatusCode) -> Value {
    let (status, headers, body) = bounded(send(app, request)).await;
    assert_eq!(status, expected);
    assert_eq!(headers[header::CACHE_CONTROL], "no-store");
    if expected == StatusCode::UNAUTHORIZED {
        assert_eq!(headers[header::WWW_AUTHENTICATE], "Bearer");
    }
    if expected == StatusCode::TOO_MANY_REQUESTS {
        assert!(!headers.contains_key(header::RETRY_AFTER));
        assert_eq!(body, json!({"error":{"message":"Request quota exhausted"}}));
    }
    assert_private_error(&body);
    body
}

fn epoch_seconds() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

#[tokio::test]
async fn lifecycle_authentication_precedes_path_extraction_and_never_reads_a_body() {
    let fixture = Fixture::new();
    let token = issued_token(&fixture).await;
    let original = persisted(&fixture.path);
    for action in ["revoke", "done"] {
        for (job_id, credential) in [
            (JOB_ID, None),
            ("secret-invalid-uuid", Some(token.as_str())),
            ("%FF", Some("wrong-control-token")),
        ] {
            assert_error(
                fixture.app.clone(),
                lifecycle(job_id, action, credential, unread_body()),
                StatusCode::UNAUTHORIZED,
            )
            .await;
        }
        let mut duplicate = lifecycle(
            JOB_ID,
            action,
            Some(CONTROL_TOKEN),
            Body::from("{malformed"),
        );
        duplicate.headers_mut().append(
            header::AUTHORIZATION,
            format!("Bearer {CONTROL_TOKEN}").parse().unwrap(),
        );
        assert_error(fixture.app.clone(), duplicate, StatusCode::UNAUTHORIZED).await;
        for job_id in ["secret-invalid-uuid", "%FF"] {
            assert_error(
                fixture.app.clone(),
                lifecycle(job_id, action, Some(CONTROL_TOKEN), unread_body()),
                StatusCode::BAD_REQUEST,
            )
            .await;
        }
        assert_error(
            fixture.app.clone(),
            lifecycle(OTHER_JOB_ID, action, Some(CONTROL_TOKEN), Body::empty()),
            StatusCode::NOT_FOUND,
        )
        .await;
        // An absent path segment is not a job and must never mutate the issued job.
        let response = bounded(fixture.app.clone().oneshot(lifecycle(
            "",
            action,
            Some(CONTROL_TOKEN),
            Body::empty(),
        )))
        .await
        .unwrap();
        assert!(response.status().is_client_error());
    }
    assert_eq!(persisted(&fixture.path), original);
}

#[tokio::test]
async fn transitions_are_canonical_bodyless_durable_and_first_terminal_wins() {
    for (action, terminal, other) in [("revoke", "revoked", "done"), ("done", "done", "revoke")] {
        let mut fixture = Fixture::new();
        let token = issued_token(&fixture).await;
        Connection::open(&fixture.path)
            .unwrap()
            .execute_batch("UPDATE jobs SET requests_used = 6, tokens_used = 123")
            .unwrap();
        assert_error(
            fixture.app.clone(),
            chat(&token, Body::from("{malformed")),
            StatusCode::BAD_REQUEST,
        )
        .await;
        assert_eq!(persisted(&fixture.path)["requests_used"], 7);
        let mut expected = persisted(&fixture.path);
        let before = epoch_seconds();
        assert_no_content(
            fixture.app.clone(),
            lifecycle(
                &JOB_ID.to_uppercase(),
                action,
                Some(CONTROL_TOKEN),
                Body::empty(),
            ),
        )
        .await;
        let transitioned = persisted(&fixture.path);
        expected["status"] = json!(terminal);
        if action == "revoke" {
            let revoked_at = transitioned["revoked_at"].as_i64().unwrap();
            assert!((before..=epoch_seconds()).contains(&revoked_at));
            expected["revoked_at"] = json!(revoked_at);
        }
        assert_eq!(transitioned, expected);
        // Neither a reason body nor any other request body is part of this API.
        assert_no_content(
            fixture.app.clone(),
            lifecycle(JOB_ID, action, Some(CONTROL_TOKEN), unread_body()),
        )
        .await;
        assert_eq!(persisted(&fixture.path), expected);
        assert_error(
            fixture.app.clone(),
            lifecycle(JOB_ID, other, Some(CONTROL_TOKEN), Body::from("{malformed")),
            StatusCode::CONFLICT,
        )
        .await;
        assert_error(
            fixture.app.clone(),
            chat(&token, unread_body()),
            StatusCode::UNAUTHORIZED,
        )
        .await;

        fixture.app = router(
            config(fixture.path.clone()),
            Store::open(&fixture.path).unwrap(),
        )
        .unwrap();
        assert_eq!(persisted(&fixture.path), expected);
        assert_error(
            fixture.app.clone(),
            chat(&token, unread_body()),
            StatusCode::UNAUTHORIZED,
        )
        .await;
        assert_no_content(
            fixture.app.clone(),
            lifecycle(JOB_ID, action, Some(CONTROL_TOKEN), Body::empty()),
        )
        .await;
        let (status, _, body) = issue(fixture.app.clone(), valid_body()).await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_private_error(&body);
        assert_eq!(persisted(&fixture.path), expected);
    }
}

#[tokio::test]
async fn overdue_jobs_are_materialized_by_startup_admission_and_lifecycle_operations() {
    for trigger in ["startup", "admission", "revoke", "done"] {
        let mut fixture = Fixture::new();
        let token = issued_token(&fixture).await;
        assert_error(
            fixture.app.clone(),
            chat(&token, Body::from("{malformed")),
            StatusCode::BAD_REQUEST,
        )
        .await;
        assert_eq!(persisted(&fixture.path)["requests_used"], 1);
        Connection::open(&fixture.path)
            .unwrap()
            .execute(
                "UPDATE jobs SET created_at = 0, expires_at = ?1",
                [epoch_seconds()],
            )
            .unwrap();
        let mut expected = persisted(&fixture.path);
        assert_eq!(expected["status"], "active");
        match trigger {
            "startup" => {
                fixture.app = router(
                    config(fixture.path.clone()),
                    Store::open(&fixture.path).unwrap(),
                )
                .unwrap();
            }
            "admission" => {
                assert_error(
                    fixture.app.clone(),
                    chat(&token, unread_body()),
                    StatusCode::UNAUTHORIZED,
                )
                .await;
            }
            action => {
                assert_error(
                    fixture.app.clone(),
                    lifecycle(JOB_ID, action, Some(CONTROL_TOKEN), Body::empty()),
                    StatusCode::CONFLICT,
                )
                .await;
            }
        }
        expected["status"] = json!("expired");
        assert_eq!(
            persisted(&fixture.path),
            expected,
            "expiry trigger: {trigger}"
        );
        fixture.app = router(
            config(fixture.path.clone()),
            Store::open(&fixture.path).unwrap(),
        )
        .unwrap();
        assert_error(
            fixture.app.clone(),
            chat(&token, unread_body()),
            StatusCode::UNAUTHORIZED,
        )
        .await;
        for action in ["revoke", "done"] {
            assert_error(
                fixture.app.clone(),
                lifecycle(JOB_ID, action, Some(CONTROL_TOKEN), Body::empty()),
                StatusCode::CONFLICT,
            )
            .await;
        }
        let (status, _, _) = issue(fixture.app.clone(), valid_body()).await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(persisted(&fixture.path), expected);
    }
}

#[tokio::test]
async fn exhausted_jobs_cannot_be_reopened_by_either_lifecycle_endpoint() {
    let fixture = Fixture::new();
    let mut body = valid_body();
    body["max_requests"] = json!(1);
    let (status, _, issued) = issue(fixture.app.clone(), body).await;
    assert_eq!(status, StatusCode::CREATED);
    let token = issued["token"].as_str().unwrap();
    assert_error(
        fixture.app.clone(),
        chat(token, Body::from("{malformed")),
        StatusCode::BAD_REQUEST,
    )
    .await;
    let original = persisted(&fixture.path);
    for action in ["revoke", "done"] {
        assert_error(
            fixture.app.clone(),
            lifecycle(JOB_ID, action, Some(CONTROL_TOKEN), Body::empty()),
            StatusCode::CONFLICT,
        )
        .await;
    }
    assert_error(
        fixture.app.clone(),
        chat(token, unread_body()),
        StatusCode::TOO_MANY_REQUESTS,
    )
    .await;
    assert_eq!(persisted(&fixture.path), original);
}

#[tokio::test]
async fn lifecycle_storage_failures_return_private_generic_errors() {
    let fixture = Fixture::new();
    issued_token(&fixture).await;
    Connection::open(&fixture.path)
        .unwrap()
        .execute_batch("DROP TABLE jobs")
        .unwrap();
    for action in ["revoke", "done"] {
        assert_error(
            fixture.app.clone(),
            lifecycle(JOB_ID, action, Some(CONTROL_TOKEN), Body::empty()),
            StatusCode::INTERNAL_SERVER_ERROR,
        )
        .await;
    }
}

#[tokio::test]
async fn competing_revoke_and_done_commit_exactly_one_terminal_state() {
    let fixture = Fixture::new();
    issued_token(&fixture).await;
    let (revoked, done) = bounded(async {
        tokio::join!(
            fixture.app.clone().oneshot(lifecycle(
                JOB_ID,
                "revoke",
                Some(CONTROL_TOKEN),
                Body::empty()
            )),
            fixture.app.clone().oneshot(lifecycle(
                JOB_ID,
                "done",
                Some(CONTROL_TOKEN),
                Body::empty()
            )),
        )
    })
    .await;
    let revoked = revoked.unwrap();
    let done = done.unwrap();
    let (winner, loser, terminal) = match (revoked.status(), done.status()) {
        (StatusCode::NO_CONTENT, StatusCode::CONFLICT) => (revoked, done, "revoked"),
        (StatusCode::CONFLICT, StatusCode::NO_CONTENT) => (done, revoked, "done"),
        statuses => panic!("expected exactly one terminal winner, got {statuses:?}"),
    };
    assert_eq!(winner.headers()[header::CACHE_CONTROL], "no-store");
    assert_eq!(loser.headers()[header::CACHE_CONTROL], "no-store");
    assert!(
        to_bytes(winner.into_body(), MAX_BODY_BYTES)
            .await
            .unwrap()
            .is_empty()
    );
    let body: Value =
        serde_json::from_slice(&to_bytes(loser.into_body(), MAX_BODY_BYTES).await.unwrap())
            .unwrap();
    assert_private_error(&body);
    assert_eq!(persisted(&fixture.path)["status"], terminal);
}

struct HeldUpstream {
    task: JoinHandle<()>,
    entered: oneshot::Receiver<()>,
    release: Option<oneshot::Sender<()>>,
    calls: Arc<AtomicUsize>,
}

impl Drop for HeldUpstream {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn held_upstream() -> (Fixture, HeldUpstream) {
    let (entered_tx, entered) = oneshot::channel();
    let (release, release_rx) = oneshot::channel();
    let entered_tx = Arc::new(std::sync::Mutex::new(Some(entered_tx)));
    let release_rx = Arc::new(tokio::sync::Mutex::new(Some(release_rx)));
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let upstream = Router::new().route(
        "/v1/chat/completions",
        post(move || {
            let entered_tx = entered_tx.clone();
            let release_rx = release_rx.clone();
            let count = count.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                entered_tx.lock().unwrap().take().unwrap().send(()).unwrap();
                let receiver = release_rx.lock().await.take().unwrap();
                receiver.await.unwrap();
                Json(json!({
                    "id": "held-completion",
                    "choices": [{"message": {"role": "assistant", "content": "Completed after the job became terminal"}}],
                    "usage": {"prompt_tokens": 8, "completion_tokens": 5, "total_tokens": 13}
                }))
            }
        }),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move { axum::serve(listener, upstream).await.unwrap() });
    let mut fixture = Fixture::new();
    let mut settings = config(fixture.path.clone());
    settings.upstream_base_url = format!("http://{address}").parse().unwrap();
    fixture.app = router(settings, Store::open(&fixture.path).unwrap()).unwrap();
    (
        fixture,
        HeldUpstream {
            task,
            entered,
            release: Some(release),
            calls,
        },
    )
}

#[tokio::test]
async fn admitted_requests_finish_after_terminal_transition_even_before_body_read_completes() {
    for action in ["revoke", "done", "expire", "exhaust"] {
        let (fixture, mut upstream) = held_upstream().await;
        let mut issue_body = valid_body();
        if action == "exhaust" {
            issue_body["max_requests"] = json!(1);
        }
        let (status, _, issued) = issue(fixture.app.clone(), issue_body).await;
        assert_eq!(status, StatusCode::CREATED);
        let token = issued["token"].as_str().unwrap();
        let (body_entered_tx, body_entered_rx) = oneshot::channel();
        let (body_release_tx, body_release_rx) = oneshot::channel();
        let body = Body::from_stream(futures_util::stream::once(async move {
            body_entered_tx.send(()).unwrap();
            body_release_rx.await.unwrap();
            Ok::<_, std::io::Error>(
                json!({
                    "model": "test-model",
                    "messages": [{"role": "user", "content": "Complete this admitted request"}]
                })
                .to_string(),
            )
        }));
        let app = fixture.app.clone();
        let admitted = tokio::spawn(send(app, chat(token, body)));
        // Reaching the body proves admission has completed, without relying on a sleep.
        bounded(body_entered_rx).await.unwrap();
        assert_eq!(upstream.calls.load(Ordering::SeqCst), 0);
        assert_eq!(persisted(&fixture.path)["requests_used"], 1);
        if action == "exhaust" {
            for action in ["revoke", "done"] {
                assert_error(
                    fixture.app.clone(),
                    lifecycle(JOB_ID, action, Some(CONTROL_TOKEN), Body::empty()),
                    StatusCode::CONFLICT,
                )
                .await;
            }
        } else if action == "expire" {
            Connection::open(&fixture.path)
                .unwrap()
                .execute(
                    "UPDATE jobs SET created_at = 0, expires_at = ?1",
                    [epoch_seconds()],
                )
                .unwrap();
        } else {
            assert_no_content(
                fixture.app.clone(),
                lifecycle(JOB_ID, action, Some(CONTROL_TOKEN), Body::empty()),
            )
            .await;
        }
        let rejection = if action == "exhaust" {
            StatusCode::TOO_MANY_REQUESTS
        } else {
            StatusCode::UNAUTHORIZED
        };
        assert_error(fixture.app.clone(), chat(token, unread_body()), rejection).await;
        let terminal = persisted(&fixture.path);
        assert_eq!(
            terminal["status"],
            match action {
                "revoke" => "revoked",
                "done" => "done",
                "exhaust" => "exhausted",
                _ => "expired",
            }
        );
        assert_eq!(terminal["requests_used"], 1);
        assert_eq!(terminal["tokens_used"], 0);

        body_release_tx.send(()).unwrap();
        bounded(&mut upstream.entered).await.unwrap();
        assert!(!admitted.is_finished());
        assert_error(
            fixture.app.clone(),
            chat(token, Body::from("{malformed")),
            rejection,
        )
        .await;
        upstream.release.take().unwrap().send(()).unwrap();
        let (status, _, completion) = bounded(admitted).await.unwrap();
        assert_eq!(status, StatusCode::OK);
        assert_eq!(completion["id"], "held-completion");
        assert_eq!(
            completion["choices"][0]["message"]["content"],
            "Completed after the job became terminal"
        );
        assert_eq!(upstream.calls.load(Ordering::SeqCst), 1);
        wait_for_accounting(&fixture).await;
        let reconciled = persisted(&fixture.path);
        assert_eq!(reconciled["status"], terminal["status"]);
        assert_eq!(reconciled["requests_used"], 1);
        assert_eq!(reconciled["tokens_used"], 13);
        assert_eq!(reconciled["revoked_at"], terminal["revoked_at"]);
    }
}

#[tokio::test]
async fn cancelled_admissions_remain_charged_and_exhausted_after_restart() {
    let (mut fixture, upstream) = held_upstream().await;
    let mut body = valid_body();
    body["max_requests"] = json!(1);
    let (status, _, issued) = issue(fixture.app.clone(), body).await;
    assert_eq!(status, StatusCode::CREATED);
    let token = issued["token"].as_str().unwrap();
    let (entered_tx, entered_rx) = oneshot::channel();
    let body = Body::from_stream(futures_util::stream::once(async move {
        entered_tx.send(()).unwrap();
        std::future::pending::<Result<String, std::io::Error>>().await
    }));
    let admitted = tokio::spawn(send(fixture.app.clone(), chat(token, body)));
    bounded(entered_rx).await.unwrap();
    let charged = persisted(&fixture.path);
    assert_eq!(charged["requests_used"], 1);
    assert_eq!(charged["status"], "exhausted");
    admitted.abort();
    assert!(bounded(admitted).await.unwrap_err().is_cancelled());
    assert_eq!(persisted(&fixture.path), charged);
    assert_eq!(upstream.calls.load(Ordering::SeqCst), 0);

    fixture.app = router(
        config(fixture.path.clone()),
        Store::open(&fixture.path).unwrap(),
    )
    .unwrap();
    assert_error(
        fixture.app.clone(),
        chat(token, unread_body()),
        StatusCode::TOO_MANY_REQUESTS,
    )
    .await;
    assert_eq!(persisted(&fixture.path), charged);
    assert_eq!(upstream.calls.load(Ordering::SeqCst), 0);
}
