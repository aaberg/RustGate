use crate::adapters::{ProviderAdapter, Usage};
use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;

const IDLE_STREAM_TIMEOUT: Duration = Duration::from_secs(120);
const SSE_DATA_PREFIX: &str = "data:";

type Finalize = Box<dyn FnOnce(u64, Option<Usage>) + Send>;

struct StreamState {
    resp: reqwest::Response,
    adapter: Arc<dyn ProviderAdapter>,
    finalize: Option<Finalize>,
    total: u64,
    usage: Option<Usage>,
    sse_buffer: Vec<u8>,
    metrics: Option<crate::metrics::Metrics>,
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
}

impl Drop for StreamState {
    fn drop(&mut self) {
        // Client disconnects drop the response stream without polling it to
        // EOF: charge observed bytes here so accounting, audit, in-flight and
        // the permit are always reconciled. finish() is idempotent.
        finish(self);
    }
}

/// Pass-through body stream: flushes chunks immediately, sniffs SSE data
/// lines for usage, counts bytes, enforces an idle timeout, and runs the
/// finalize callback (token reconciliation + logging) exactly once when the
/// stream ends, errors, or times out.
pub fn usage_stream(
    resp: reqwest::Response,
    adapter: Arc<dyn ProviderAdapter>,
    finalize: impl FnOnce(u64, Option<Usage>) + Send + 'static,
    metrics: crate::metrics::Metrics,
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
) -> impl futures::Stream<Item = Result<Bytes, std::io::Error>> {
    let state = StreamState {
        resp,
        adapter,
        finalize: Some(Box::new(finalize)),
        total: 0,
        usage: None,
        sse_buffer: Vec::new(),
        metrics: Some(metrics),
        permit,
    };
    futures::stream::unfold(state, |mut s| async move {
        if s.finalize.is_none() {
            return None;
        }
        let chunk = tokio::time::timeout(IDLE_STREAM_TIMEOUT, s.resp.chunk()).await;
        match chunk {
            Ok(Ok(Some(chunk))) => {
                s.total += chunk.len() as u64;
                sniff_sse(&mut s.sse_buffer, s.adapter.as_ref(), &chunk, &mut s.usage);
                Some((Ok(chunk), s))
            }
            Ok(Ok(None)) => {
                finish(&mut s);
                Some((Ok(Bytes::new()), s))
            }
            Ok(Err(e)) => {
                finish(&mut s);
                Some((Err(std::io::Error::other(e)), s))
            }
            Err(_) => {
                finish(&mut s);
                Some((
                    Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "idle stream timeout",
                    )),
                    s,
                ))
            }
        }
    })
}

fn sniff_sse(buffer: &mut Vec<u8>, adapter: &dyn ProviderAdapter, chunk: &Bytes, usage: &mut Option<Usage>) {
    buffer.extend_from_slice(chunk);
    while let Some(pos) = buffer.iter().position(|&b| b == b'\n') {
        let line: Vec<u8> = buffer.drain(..=pos).collect();
        let line = String::from_utf8_lossy(&line);
        let line = line.trim();
        let Some(payload) = line.strip_prefix(SSE_DATA_PREFIX) else {
            continue;
        };
        let payload = payload.trim_start();
        if payload == "[DONE]" {
            continue;
        }
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(payload) {
            if let Some(u) = adapter.extract_usage_chunk(&v) {
                *usage = Some(u);
            }
        }
    }
}

fn finish(s: &mut StreamState) {
    if let Some(f) = s.finalize.take() {
        f(s.total, s.usage.take());
    }
    if let Some(m) = &s.metrics {
        m.in_flight.dec();
    }
    s.permit.take();
}
