use super::*;

fn operational_request(path: &str, token: Option<&str>) -> Request<Body> {
    let mut request = Request::builder().uri(path);
    if let Some(token) = token {
        request = request.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }
    request.body(Body::empty()).unwrap()
}

#[tokio::test]
async fn operational_endpoints_require_control_auth_and_never_charge_jobs() {
    let fixture = Fixture::new();
    let (_, _, issued) = issue(fixture.app.clone(), valid_body()).await;
    let token = issued["token"].as_str().unwrap();
    for path in ["/metrics", "/v1/usage"] {
        for supplied in [None, Some("invalid"), Some(token)] {
            let (status, headers, body) =
                send(fixture.app.clone(), operational_request(path, supplied)).await;
            assert_eq!(status, StatusCode::UNAUTHORIZED);
            assert_eq!(headers[header::WWW_AUTHENTICATE], "Bearer");
            assert_eq!(headers[header::CACHE_CONTROL], "no-store");
            assert_private_error(&body);
        }
    }
    let (status, headers, body) = send(
        fixture.app.clone(),
        operational_request("/v1/usage", Some(CONTROL_TOKEN)),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers[header::CACHE_CONTROL], "no-store");
    assert_eq!(
        body,
        json!({"keys":[{"upstream_key_id":"pool-a", "requests_used":0,
        "tokens_used":0,"tokens_unreported":0,"tokens_reserved":0}],
        "jobs":[{"job_id":JOB_ID,"tenant_id":"tenant-a","requests_used":0}],
        "outcomes":[],"blocked":[]})
    );
    assert_eq!(persisted(&fixture.path)["requests_used"], 0);
}

#[tokio::test]
async fn metrics_escape_operator_labels_and_preserve_durable_policy_counts_after_restart() {
    let mut fixture = Fixture::new();
    let tenant = "tenant\"\\\nmetric_fake 99";
    let mut body = valid_body();
    body["tenant_id"] = json!(tenant);
    let (_, _, issued) = issue(fixture.app.clone(), body).await;
    let token = issued["token"].as_str().unwrap();
    let secret = "sk-fakecredential12345678901234567890";
    let response = fixture.app.clone().oneshot(Request::builder().method("POST")
        .uri("/v1/chat/completions?private-query-marker")
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(json!({"model":"private-model-marker","messages":[{"role":"user","content":secret}]}).to_string())).unwrap()).await.unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    wait_for_accounting(&fixture).await;
    for restart in [false, true] {
        if restart {
            fixture.app = router(
                config(fixture.path.clone()),
                Store::open(&fixture.path).unwrap(),
            )
            .unwrap();
        }
        let response = fixture
            .app
            .clone()
            .oneshot(operational_request("/metrics", Some(CONTROL_TOKEN)))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()[header::CONTENT_TYPE],
            "text/plain; version=0.0.4; charset=utf-8"
        );
        assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
        let text = String::from_utf8(
            to_bytes(response.into_body(), MAX_BODY_BYTES)
                .await
                .unwrap()
                .to_vec(),
        )
        .unwrap();
        // A parser must see each family's samples directly after its metadata,
        // not as untyped series separated from their TYPE declaration.
        let mut family = "";
        for line in text.lines() {
            if let Some(metadata) = line.strip_prefix("# TYPE ") {
                family = metadata.split_once(' ').unwrap().0;
            } else if !line.starts_with('#') && !line.is_empty() {
                let name = line.split(['{', ' ']).next().unwrap();
                assert!(
                    name == family
                        || name.strip_prefix(family).is_some_and(|suffix| {
                            matches!(suffix, "_bucket" | "_sum" | "_count")
                        }),
                    "sample {name} separated from its family {family}"
                );
            }
        }
        assert!(text.contains("rustgate_key_requests_total{key_id=\"pool-a\"} 1\n"));
        assert!(text.contains("rustgate_key_tokens_total{key_id=\"pool-a\"} 0\n"));
        assert!(text.contains("rustgate_key_tokens_reserved{key_id=\"pool-a\"} 0\n"));
        assert!(text.contains("rustgate_blocked_requests_total{reason=\"possible_secret\"} 1\n"));
        assert!(text.contains("rustgate_requests_by_outcome{outcome=\"rejected\"} 1\n"));
        assert!(text.contains("tenant_id=\"tenant\\\"\\\\\\nmetric_fake 99\""));
        assert!(!text.lines().any(|line| line.starts_with("metric_fake")));
        for sensitive in [
            secret,
            token,
            CONTROL_TOKEN,
            UPSTREAM_SECRET,
            "private-model-marker",
            "private-query-marker",
            "authoritative system prompt",
        ] {
            assert!(
                !text.contains(sensitive),
                "operational metrics leaked private content"
            );
        }
    }
}
