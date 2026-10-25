use crate::adapters::{ProviderAdapter, Usage};
use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;

const IDLE_STREAM_TIMEOUT: Duration = Duration::from_secs(120);

/// Upper bound on the SSE line-inspection buffer. Lines larger than this are
/// not usage payloads in any real provider's format; they are skipped
/// (dropped from the buffer) rather than accumulated, keeping memory bounded
/// and scanning linear. Pass-through of chunks to the client is unaffected.
const MAX_SSE_LINE: usize = 64 * 1024;
/// Upper bound on one assembled SSE event payload for usage parsing.
const MAX_SSE_EVENT: usize = 64 * 1024;

type Finalize = Box<dyn FnOnce(u64, Option<Usage>) + Send>;

/// SSE event assembly state: `data:` fields are joined per the SSE spec
/// (fields of one event, delimited by a blank line), so usage events split
/// across multiple `data:` lines parse correctly.
#[derive(Default)]
struct SseEvent {
    data: String,
}

impl SseEvent {
    fn push_data(&mut self, payload: &str) {
        if !self.data.is_empty() {
            self.data.push('\n');
        }
        self.data.push_str(payload);
    }

    /// Try to extract usage from the assembled event; reset for the next.
    fn finish(&mut self, adapter: &dyn ProviderAdapter, usage: &mut Option<Usage>) {
        if self.data.is_empty() {
            return;
        }
        if self.data.len() <= MAX_SSE_EVENT {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(&self.data) {
                if let Some(u) = adapter.extract_usage_chunk(&v) {
                    *usage = Some(u);
                }
            }
        }
        self.data.clear();
    }
}

struct StreamState {
    resp: reqwest::Response,
    adapter: Arc<dyn ProviderAdapter>,
    finalize: Option<Finalize>,
    total: u64,
    usage: Option<Usage>,
    /// Bounded partial-line buffer for SSE sniffing.
    line_buf: Vec<u8>,
    /// Assembled multi-field SSE event.
    event: SseEvent,
    metrics: Option<crate::metrics::Metrics>,
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
}

/// Pass-through body stream: flushes chunks immediately, sniffs SSE events
/// for usage (multi-`data:`-field aware, bounded memory), counts bytes,
/// enforces an idle timeout, and runs the finalize callback exactly once
/// when the stream ends, errors, times out, or is dropped unpolled.
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
        line_buf: Vec::new(),
        event: SseEvent::default(),
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
                sniff_sse(&mut s);
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

/// Consume complete lines from the bounded buffer; handle oversized lines by
/// dropping them (memory bound) while continuing byte counting and delivery.
fn sniff_sse(s: &mut StreamState) {
    loop {
        let Some(pos) = s.line_buf.iter().position(|&b| b == b'\n') else {
            // No complete line: bound the buffer. If the partial line already
            // exceeds the cap, drop it (it cannot be a usage payload).
            if s.line_buf.len() > MAX_SSE_LINE {
                s.line_buf.clear();
                s.event.data.clear();
            }
            return;
        };
        let line: Vec<u8> = s.line_buf.drain(..=pos).collect();
        let line = String::from_utf8_lossy(&line);
        let line = line.trim_end_matches(['\r', '\n']);
        if line.is_empty() {
            // Blank line: event delimiter — parse the assembled event.
            s.event.finish(s.adapter.as_ref(), &mut s.usage);
            continue;
        }
        if let Some(payload) = line.strip_prefix("data:") {
            let payload = payload.trim_start();
            if payload == "[DONE]" {
                // Convention: final marker; the event (if any) ends here.
                s.event.finish(s.adapter.as_ref(), &mut s.usage);
                continue;
            }
            s.event.push_data(payload);
        }
        // Non-data fields (event:, id:, retry:, comments) are ignored for
        // sniffing; pass-through is unaffected.
    }
}

fn finish(s: &mut StreamState) {
    // Flush any assembled (undelimited) final event before finalizing.
    s.event.finish(s.adapter.as_ref(), &mut s.usage);
    if let Some(f) = s.finalize.take() {
        f(s.total, s.usage.take());
    }
    // Take (not borrow) metrics and permit so finish is idempotent: it can
    // run once on EOF/error/timeout and again in Drop without double-
    // decrementing the gauge or double-releasing the permit.
    if let Some(m) = s.metrics.take() {
        m.in_flight.dec();
    }
    s.permit.take();
}

impl Drop for StreamState {
    fn drop(&mut self) {
        // Client disconnects drop the response stream without polling it to
        // EOF: charge observed bytes here so accounting, audit, in-flight and
        // the permit are always reconciled. finish() is idempotent.
        finish(self);
    }
}
