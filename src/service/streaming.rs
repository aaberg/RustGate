use std::{io, time::Duration};

use axum::{
    body::{Body, Bytes},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use futures_util::stream;
use tokio::{
    sync::{OwnedSemaphorePermit, mpsc, oneshot},
    time::{Instant, timeout, timeout_at},
};

use super::error;

/// Wait for an actual body byte (or clean EOF), not just response headers.
/// Nothing is committed downstream until this succeeds, allowing a useful
/// gateway status for connection and first-byte failures.
pub(super) async fn forward(
    request: reqwest::RequestBuilder,
    permit: OwnedSemaphorePermit,
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
            return error(StatusCode::GATEWAY_TIMEOUT, "Upstream request timed out");
        }
        Ok(Err(_)) => return error(StatusCode::BAD_GATEWAY, "Upstream request failed"),
        Err(_) => return error(StatusCode::GATEWAY_TIMEOUT, "Upstream request timed out"),
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
    headers.insert(
        header::CACHE_CONTROL,
        header::HeaderValue::from_static("no-store"),
    );

    let Some(first) = first else {
        return (status, headers, Body::empty()).into_response();
    };
    // One queued chunk plus the producer's current chunk bounds read-ahead.
    // A separate terminal result cannot be lost when that queue is full.
    let (sender, receiver) = mpsc::channel::<Bytes>(1);
    let (finished, terminal) = oneshot::channel();
    tokio::spawn(async move {
        let result = {
            let relay = async {
                let _permit = permit;
                let mut response = response;
                sender
                    .send(first)
                    .await
                    .map_err(|_| io::Error::other("Downstream disconnected"))?;
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
                    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "Upstream idle timeout"))?
                    .map_err(|_| io::Error::other("Upstream response failed"))?;
                    let Some(chunk) = chunk else { return Ok(()) };
                    sender
                        .send(chunk)
                        .await
                        .map_err(|_| io::Error::other("Downstream disconnected"))?;
                }
            };
            // Deadlines keep running even if downstream stops polling. Dropping
            // the body closes the receiver and cancels upstream immediately.
            tokio::select! {
                biased;
                _ = sender.closed() => Ok(()),
                result = timeout_at(overall_deadline, relay) => result.unwrap_or_else(|_| {
                    Err(io::Error::new(io::ErrorKind::TimedOut, "Overall request timeout"))
                }),
            }
        };
        drop(sender);
        let _ = finished.send(result);
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
