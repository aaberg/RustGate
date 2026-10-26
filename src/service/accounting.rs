use tokio::sync::OwnedSemaphorePermit;

use crate::store::{Store, Usage};

/// Owns admission until durable reconciliation, including abandoned blocking
/// admissions and futures cancelled before a response body exists.
pub(super) struct Accounting {
    pending: Option<Pending>,
}

struct Pending {
    store: Store,
    request_id: i64,
    outcome: &'static str,
    usage: Option<Usage>,
    usage_final: bool,
    _permit: OwnedSemaphorePermit,
}

impl Drop for Pending {
    fn drop(&mut self) {
        // Normally dropped on a blocking worker. If shutdown discards that
        // worker's closure, reconcile during teardown instead of losing usage.
        if self
            .store
            .finish_request(self.request_id, self.outcome, self.usage, self.usage_final)
            .is_err()
        {
            // Leave the pending row recoverable at startup. Never log database
            // errors, which can contain prompts or other sensitive values.
            eprintln!("Request accounting persistence failed; startup recovery required");
        }
    }
}

impl Accounting {
    pub(super) fn new(store: Store, request_id: i64, permit: OwnedSemaphorePermit) -> Self {
        Self {
            pending: Some(Pending {
                store,
                request_id,
                outcome: "cancelled",
                // Before dispatch no upstream tokens can have been consumed.
                usage: Some(Usage {
                    prompt_tokens: 0,
                    completion_tokens: 0,
                    total_tokens: 0,
                }),
                usage_final: true,
                _permit: permit,
            }),
        }
    }

    pub(super) fn dispatched(&mut self) {
        self.pending.as_mut().unwrap().usage = None;
        self.pending.as_mut().unwrap().usage_final = false;
    }

    pub(super) fn observe_usage(&mut self, usage: Option<Usage>) {
        // The relay can be cancelled at its next await. Keep its latest
        // complete report in the guard that owns cancellation reconciliation.
        self.pending.as_mut().unwrap().usage = usage;
        self.pending.as_mut().unwrap().usage_final = false;
    }

    pub(super) async fn finish(
        self,
        outcome: &'static str,
        usage: Option<Usage>,
        usage_final: bool,
    ) {
        let _ = self.finish_worker(outcome, usage, usage_final).await;
    }

    pub(super) fn finish_in_background(
        self,
        outcome: &'static str,
        usage: Option<Usage>,
        usage_final: bool,
    ) {
        drop(self.finish_worker(outcome, usage, usage_final));
    }

    fn finish_worker(
        mut self,
        outcome: &'static str,
        usage: Option<Usage>,
        usage_final: bool,
    ) -> tokio::task::JoinHandle<()> {
        let mut pending = self.pending.take().unwrap();
        pending.outcome = outcome;
        pending.usage = usage;
        pending.usage_final = usage_final;
        // Configure the terminal outcome and transfer ownership synchronously,
        // before a response can return or runtime shutdown cancels async tasks.
        tokio::task::spawn_blocking(move || drop(pending))
    }

    pub(super) async fn reject(self) {
        let usage = self.pending.as_ref().unwrap().usage;
        self.finish("rejected", usage, true).await;
    }
}

impl Drop for Accounting {
    fn drop(&mut self) {
        if let Some(pending) = self.pending.take() {
            // A dropped JoinHandle's output can be destroyed on the admission
            // worker itself. Runtime handles are available on blocking workers.
            tokio::task::spawn_blocking(move || drop(pending));
        }
    }
}
