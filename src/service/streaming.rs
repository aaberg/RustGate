use std::{io, time::Duration};

use axum::{
    body::{Body, Bytes},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use futures_util::stream;
use tokio::{
    sync::{mpsc, oneshot},
    time::{Instant, timeout, timeout_at},
};

use super::{
    accounting::Accounting,
    error,
    usage::{UsageKind, UsageTracker},
};

/// Wait for an actual body byte (or clean EOF), not just response headers.
/// Nothing is committed downstream until this succeeds, allowing a useful
/// gateway status for connection and first-byte failures.
pub(super) async fn forward(
    request: reqwest::RequestBuilder,
    mut accounting: Accounting,
    usage_kind: UsageKind,
    overall_deadline: Instant,
    first_byte_timeout: Duration,
    idle_timeout: Duration,
) -> Response {
    let first_deadline = overall_deadline.min(Instant::now() + first_byte_timeout);
    let first = timeout_at(first_deadline, async {
        let mut response = request.send().await?;
        let first = loop {
            match response.chunk().await? {
                Some(bytes) if bytes.is_empty() => continue,
                chunk => break chunk,
            }
        };
        Ok::<_, reqwest::Error>((response, first))
    })
    .await;
    let (response, first) = match first {
        Ok(Ok(response)) => response,
        Ok(Err(failure)) if failure.is_timeout() => {
            accounting.finish_in_background("timeout", None, false);
            return error(StatusCode::GATEWAY_TIMEOUT, "Upstream request timed out");
        }
        Ok(Err(_)) => {
            accounting.finish_in_background("upstream_error", None, false);
            return error(StatusCode::BAD_GATEWAY, "Upstream request failed");
        }
        Err(_) => {
            accounting.finish_in_background("timeout", None, false);
            return error(StatusCode::GATEWAY_TIMEOUT, "Upstream request timed out");
        }
    };

    let status = response.status();
    let completed = if status.is_success() {
        "completed"
    } else {
        "upstream_error"
    };
    let mut usage = UsageTracker::new(response.headers(), usage_kind);
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
    headers.insert(
        header::CACHE_CONTROL,
        header::HeaderValue::from_static("no-store"),
    );

    let Some(first) = first else {
        drop(response);
        let known = usage.finish();
        accounting.finish_in_background(completed, known, usage.is_final());
        return (status, headers, Body::empty()).into_response();
    };
    // Account bytes already received even if the downstream disconnects before
    // the relay task gets its first poll.
    usage.feed(&first);
    accounting.observe_usage(usage.observed());
    // One queued chunk plus the producer's current chunk bounds read-ahead.
    // Accounting retains only a bounded JSON body or a bounded SSE event.
    let (sender, receiver) = mpsc::channel::<Bytes>(1);
    let (finished, terminal) = oneshot::channel();
    tokio::spawn(async move {
        let (outcome, result) = {
            let relay = async {
                let mut response = response;
                sender
                    .send(first)
                    .await
                    .map_err(|_| ("disconnected", io::Error::other("Downstream disconnected")))?;
                loop {
                    let chunk = timeout(idle_timeout, async {
                        loop {
                            match response.chunk().await? {
                                Some(bytes) if bytes.is_empty() => continue,
                                chunk => break Ok::<_, reqwest::Error>(chunk),
                            }
                        }
                    })
                    .await
                    .map_err(|_| {
                        (
                            "timeout",
                            io::Error::new(io::ErrorKind::TimedOut, "Upstream idle timeout"),
                        )
                    })?
                    .map_err(|_| {
                        (
                            "upstream_error",
                            io::Error::other("Upstream response failed"),
                        )
                    })?;
                    let Some(chunk) = chunk else { return Ok(()) };
                    usage.feed(&chunk);
                    accounting.observe_usage(usage.observed());
                    sender.send(chunk).await.map_err(|_| {
                        ("disconnected", io::Error::other("Downstream disconnected"))
                    })?;
                }
            };
            // Deadlines keep running even if downstream stops polling. Dropping
            // the body closes the receiver and cancels upstream immediately.
            tokio::select! {
                biased;
                _ = sender.closed() => ("disconnected", Ok(())),
                result = timeout_at(overall_deadline, relay) => match result {
                    Ok(Ok(())) => (completed, Ok(())),
                    Ok(Err((outcome, error))) => (outcome, Err(error)),
                    Err(_) => ("timeout", Err(io::Error::new(io::ErrorKind::TimedOut, "Overall request timeout"))),
                },
            }
        };
        let known = if outcome == completed && result.is_ok() {
            usage.finish()
        } else {
            // Preserve the last known snapshot without treating it as final:
            // interrupted work may have consumed the remaining reservation.
            usage.observed()
        };
        // Transport completion must not wait for SQLite. The relay future has
        // already dropped upstream; retain accounting (and its permit) here
        // until the blocking reconciliation worker finishes.
        let usage_final = outcome == completed && result.is_ok() && usage.is_final();
        drop(sender);
        let _ = finished.send(result);
        accounting.finish(outcome, known, usage_final).await;
    });
    let chunks = stream::unfold(
        (receiver, Some(terminal)),
        |(mut receiver, terminal)| async move {
            if let Some(bytes) = receiver.recv().await {
                return Some((Ok(bytes), (receiver, terminal)));
            }
            let terminal = terminal?;
            let result = terminal
                .await
                .unwrap_or_else(|_| Err(io::Error::other("Upstream response failed")));
            result.err().map(|error| (Err(error), (receiver, None)))
        },
    );
    (status, headers, Body::from_stream(chunks)).into_response()
}
