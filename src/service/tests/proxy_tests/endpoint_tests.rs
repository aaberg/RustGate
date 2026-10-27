use super::*;

fn endpoint(method: &str, path: &str, body: impl Into<Body>, token: &str) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(path)
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(body.into())
        .unwrap()
}

#[tokio::test]
async fn endpoints_preserve_inputs_without_chat_transformation_and_replace_credentials() {
    let mock = crate::mock::router();
    let upstream = Router::new().nest("/gateway", mock.clone());
    let (fixture, _upstream) = fixture(upstream, "/gateway/").await;
    let token = token(&fixture).await;
    let inputs = [
        json!("日本語 café"),
        json!(["one", "two"]),
        json!([0, 17]),
        json!([[0], [17, 42]]),
    ];
    for (input, expected_items) in inputs.into_iter().zip([1, 2, 1, 2]) {
        let body = json!({
            "model": "mock-embed", "input": input,
            "messages": [{"role":"system", "content":"client metadata, not chat"}]
        });
        let (status, _, response) = send(
            fixture.app.clone(),
            endpoint("POST", "/v1/embeddings", body.to_string(), &token),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(response["model"], "mock-embed");
        assert_eq!(response["data"].as_array().unwrap().len(), expected_items);
        assert_eq!(
            response["usage"],
            json!({"prompt_tokens":5,"total_tokens":5})
        );
        let captured = captures(mock.clone()).await;
        let last = captured.as_array().unwrap().last().unwrap();
        assert_eq!(last["body"], body);
        assert_eq!(last["method"], "POST");
        assert_eq!(last["path"], "/gateway/v1/embeddings");
        assert_eq!(last["authorization"], format!("Bearer {UPSTREAM_SECRET}"));
    }
    // A GET must neither wait for nor forward a client body, regardless of its
    // content type. In particular it cannot run chat JSON transformation.
    let (status, _, response) = bounded_send(
        fixture.app.clone(),
        endpoint("GET", "/v1/models", unread_body(), &token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(response["data"][0]["id"], "mock-mistral");
    assert_eq!(response["data"][1]["id"], "mock-embed");
    let captured = captures(mock).await;
    assert_eq!(
        captured[4],
        json!({"method":"GET", "path":"/gateway/v1/models", "authorization":format!("Bearer {UPSTREAM_SECRET}"), "body":null})
    );
    wait_for_accounting(&fixture).await;
    assert_eq!(persisted(&fixture.path)["requests_used"], 5);
    assert_eq!(token_balances(&fixture), (20, 0, 0));
    let records: Vec<(i64, i64, i64)> = Connection::open(&fixture.path).unwrap()
        .prepare("SELECT prompt_tokens, completion_tokens, total_tokens FROM request_records ORDER BY request_id").unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?))).unwrap()
        .collect::<rusqlite::Result<_>>().unwrap();
    assert_eq!(
        records,
        vec![(5, 0, 5), (5, 0, 5), (5, 0, 5), (5, 0, 5), (0, 0, 0)]
    );
}

#[tokio::test]
async fn embeddings_reject_invalid_inputs_and_scan_secrets_before_dispatch() {
    let mock = crate::mock::router();
    let (fixture, _upstream) = fixture(mock.clone(), "").await;
    let token = token(&fixture).await;
    let mut invalid = vec![json!({}), json!([]), json!({"model":"", "input":"hello"})];
    for input in [
        json!(null),
        json!(""),
        json!([]),
        json!([""]),
        json!(["one", 2]),
        json!([-1]),
        json!([1.5]),
        json!([4294967296_u64]),
        json!([[]]),
        json!([[1], ["two"]]),
        json!({"image":"remote"}),
    ] {
        invalid.push(json!({"model":"mock-embed", "input":input}));
    }
    for (field, value) in [
        ("stream", json!(false)),
        ("stream", json!(true)),
        ("stream_options", json!({})),
        ("max_tokens", json!(1)),
        ("max_completion_tokens", json!(1)),
        ("n", json!(1)),
        ("dimensions", json!(0)),
        ("dimensions", json!(1.5)),
        ("encoding_format", json!("invalid")),
        ("user", json!(3)),
    ] {
        let mut body = json!({"model":"mock-embed", "input":"hello"});
        body[field] = value;
        invalid.push(body);
    }
    let attempts = invalid.len() as i64 + 3;
    for body in invalid {
        let (status, _, _) = send(
            fixture.app.clone(),
            endpoint("POST", "/v1/embeddings", body.to_string(), &token),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    }
    for (body, expected) in [
        (
            r#"{"model":"mock-embed","input":"sk-abcdefghijklmnopqrstuvwx"}"#.to_owned(),
            StatusCode::FORBIDDEN,
        ),
        (
            r#"{"model":"mock-embed","input":"\u0073\u006b-abcdefghijklmnopqrstuvwx"}"#.to_owned(),
            StatusCode::FORBIDDEN,
        ),
        (
            "x".repeat(MAX_BODY_BYTES + 1),
            StatusCode::PAYLOAD_TOO_LARGE,
        ),
    ] {
        assert_eq!(
            send(
                fixture.app.clone(),
                endpoint("POST", "/v1/embeddings", body, &token)
            )
            .await
            .0,
            expected
        );
    }
    wait_for_accounting(&fixture).await;
    assert_eq!(captures(mock).await, json!([]));
    assert_eq!(persisted(&fixture.path)["requests_used"], attempts);
    assert_eq!(token_balances(&fixture), (0, 0, 0));
    let blocked: i64 = Connection::open(&fixture.path).unwrap().query_row("SELECT count(*) FROM request_records WHERE block_reason = 'possible_secret' AND total_tokens = 0", [], |row| row.get(0)).unwrap();
    assert_eq!(blocked, 2);
}

#[tokio::test]
async fn embeddings_boundaries_retain_missing_usage_without_generation_reservations() {
    let upstream = Router::new().route(
        "/v1/embeddings",
        post(|| async { Json(json!({"object":"list", "data":[]})) }),
    );
    let body = json!({"model":"mock-embed", "input":["日本語", "two"]});
    let bound = body.to_string().len() as i64 + 256 + 2 * 32;
    for (allowance, expected) in [
        (bound - 1, StatusCode::TOO_MANY_REQUESTS),
        (bound, StatusCode::OK),
    ] {
        let (fixture, _upstream) = fixture(upstream.clone(), "").await;
        let mut job = valid_body();
        job["max_tokens_total"] = json!(allowance);
        let (_, _, issued) = issue(fixture.app.clone(), job).await;
        let token = issued["token"].as_str().unwrap();
        let (status, _, _) = send(
            fixture.app.clone(),
            endpoint("POST", "/v1/embeddings", body.to_string(), token),
        )
        .await;
        assert_eq!(status, expected);
        wait_for_accounting(&fixture).await;
        assert_eq!(persisted(&fixture.path)["requests_used"], 1);
        if status == StatusCode::OK {
            assert_eq!(token_balances(&fixture), (0, bound, 0));
            let unknown: Option<i64> = Connection::open(&fixture.path)
                .unwrap()
                .query_row("SELECT total_tokens FROM request_records", [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(unknown, None);
            // Even token-free model listing cannot bypass an exhausted job.
            let (status, headers, _) = bounded_send(
                fixture.app.clone(),
                endpoint("GET", "/v1/models", unread_body(), token),
            )
            .await;
            assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
            assert!(!headers.contains_key(header::RETRY_AFTER));
            assert_eq!(persisted(&fixture.path)["requests_used"], 1);
        } else {
            assert_eq!(token_balances(&fixture), (0, 0, 0));
        }
    }
}

#[tokio::test]
async fn models_errors_and_unexpected_usage_remain_known_zero_but_charge_requests() {
    let upstream = Router::new().route("/v1/models", get(|| async {
        (StatusCode::SERVICE_UNAVAILABLE, [(header::RETRY_AFTER, "7")], Json(json!({"error":{"message":"unavailable"}, "usage":{"prompt_tokens":100,"completion_tokens":200,"total_tokens":300}})))
    }));
    let (fixture, mut upstream) = fixture(upstream, "").await;
    let token = token(&fixture).await;
    let (status, headers, _) = bounded_send(
        fixture.app.clone(),
        endpoint("GET", "/v1/models", unread_body(), &token),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(headers[header::RETRY_AFTER], "7");
    upstream.task.abort();
    let _ = (&mut upstream.task).await;
    let (status, _, _) = bounded_send(
        fixture.app.clone(),
        endpoint("GET", "/v1/models", unread_body(), &token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    wait_for_accounting(&fixture).await;
    assert_eq!(token_balances(&fixture), (0, 0, 0));
    assert_eq!(persisted(&fixture.path)["requests_used"], 2);
    let known_zero: i64 = Connection::open(&fixture.path).unwrap().query_row("SELECT count(*) FROM request_records WHERE outcome = 'upstream_error' AND prompt_tokens = 0 AND completion_tokens = 0 AND total_tokens = 0", [], |row| row.get(0)).unwrap();
    assert_eq!(known_zero, 2);
}

#[tokio::test]
async fn new_endpoint_methods_are_exact_and_rejections_do_not_read_bodies() {
    let mock = crate::mock::router();
    let (fixture, _upstream) = fixture(mock.clone(), "").await;
    let token = token(&fixture).await;
    let forbidden = [
        ("HEAD", "/v1/models"),
        ("POST", "/v1/models"),
        ("OPTIONS", "/v1/models"),
        ("GET", "/v1/models/"),
        ("GET", "/v1/%6dodels"),
        ("GET", "/v1/embeddings"),
        ("PUT", "/v1/embeddings"),
        ("POST", "/v1/embeddings/"),
        ("POST", "/v1/%65mbeddings"),
    ];
    for (method, path) in forbidden {
        let response = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            fixture
                .app
                .clone()
                .oneshot(endpoint(method, path, unread_body(), &token)),
        )
        .await
        .expect("route rejection read the body")
        .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN, "{method} {path}");
    }
    assert_eq!(captures(mock).await, json!([]));
    assert_eq!(persisted(&fixture.path)["requests_used"], forbidden.len());
    assert_eq!(token_balances(&fixture), (0, 0, 0));
}
