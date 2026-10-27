use tokio::sync::OwnedSemaphorePermit;

use crate::store::{Store, Usage};

use super::{telemetry::Observation, usage::UsageKind};

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
    usage_kind: UsageKind,
    _permit: OwnedSemaphorePermit,
    observation: Option<Observation>,
    dispatched: bool,
}

impl Drop for Pending {
    fn drop(&mut self) {
        // Normally dropped on a blocking worker. If shutdown discards that
        // worker's closure, reconcile during teardown instead of losing usage.
        let persisted = self
            .store
            .finish_request(self.request_id, self.outcome, self.usage, self.usage_final)
            .is_ok();
        if let Some(observation) = &self.observation {
            observation.usage(self.outcome, self.usage, self.usage_final);
            observation.update(serde_json::json!({"accounting_persisted":persisted}));
            if self.dispatched && self.outcome != "completed" {
                observation.upstream_failed();
            }
        }
        if !persisted {
            super::telemetry::event(serde_json::json!({
                "event":"accounting_failure", "message":"Startup recovery required"
            }));
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
                usage_kind: UsageKind::Chat,
                _permit: permit,
                observation: None,
                dispatched: false,
            }),
        }
    }

    pub(super) fn observe(&mut self, observation: Observation) {
        self.pending.as_mut().unwrap().observation = Some(observation);
    }

    pub(super) fn policy(&self, blocked: bool, prompt_injection: bool) {
        if let Some(observation) = &self.pending.as_ref().unwrap().observation {
            observation.update(serde_json::json!({
                "policy_verdict": if blocked { "blocked" } else { "allowed" },
                "block_reason": blocked.then_some("possible_secret"),
                "prompt_injection": prompt_injection,
            }));
        }
    }

    pub(super) fn dispatched(&mut self, usage_kind: UsageKind) {
        let pending = self.pending.as_mut().unwrap();
        pending.usage_kind = usage_kind;
        pending.dispatched = true;
        if let Some(observation) = &pending.observation {
            observation.dispatched();
        }
        if !matches!(usage_kind, UsageKind::Models) {
            pending.usage = None;
            pending.usage_final = false;
        }
    }

    pub(super) fn observe_usage(&mut self, usage: Option<Usage>) {
        // The relay can be cancelled at its next await. Keep its latest
        // complete report in the guard that owns cancellation reconciliation.
        let pending = self.pending.as_mut().unwrap();
        if !matches!(pending.usage_kind, UsageKind::Models) {
            pending.usage = usage;
            pending.usage_final = false;
        }
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
        // Models never consume tokens, even if upstream reports usage or the
        // relay fails before it can produce a final report.
        if !matches!(pending.usage_kind, UsageKind::Models) {
            pending.usage = usage;
            pending.usage_final = usage_final;
        }
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::sync::Semaphore;

    use super::*;
    use crate::store::{JobAdmission, NewJob};

    fn admitted() -> (tempfile::TempDir, Accounting, Arc<Semaphore>) {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(&directory.path().join("jobs.sqlite3")).unwrap();
        assert!(
            store
                .insert_job(NewJob {
                    job_id: "accounting-test".to_owned(),
                    tenant_id: "tenant".to_owned(),
                    upstream_key_id: "upstream".to_owned(),
                    system_prompt: String::new(),
                    token_hash: [7; 32],
                    created_at: 1,
                    expires_at: i64::MAX,
                    max_requests: 1,
                    max_tokens_total: 100,
                })
                .is_ok()
        );
        let JobAdmission::Admitted(job) = store.admit_job([7; 32]).unwrap() else {
            panic!("request was not admitted");
        };
        let capacity = Arc::new(Semaphore::new(1));
        let accounting = Accounting::new(
            store,
            job.request_id,
            capacity.clone().try_acquire_owned().unwrap(),
        );
        (directory, accounting, capacity)
    }

    fn settlement(directory: &tempfile::TempDir) -> (String, Option<i64>, i64, i64, i64, i64) {
        let connection = rusqlite::Connection::open(directory.path().join("jobs.sqlite3")).unwrap();
        connection
            .query_row(
                "SELECT r.outcome, r.total_tokens, j.requests_used, j.tokens_used,
                    j.tokens_unreported, j.tokens_reserved
             FROM request_records r JOIN jobs j ON j.job_id = r.job_id",
                [],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                    ))
                },
            )
            .unwrap()
    }

    #[test]
    fn models_remain_known_zero_through_errors_and_immediate_shutdown() {
        for outcome in [
            "completed",
            "upstream_error",
            "timeout",
            "disconnected",
            "cancelled",
        ] {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let (directory, capacity) = runtime.block_on(async {
                let (directory, mut accounting, capacity) = admitted();
                accounting.dispatched(UsageKind::Models);
                accounting.observe_usage(Some(Usage {
                    prompt_tokens: 30,
                    completion_tokens: 20,
                    total_tokens: 50,
                }));
                accounting.observe_usage(None);
                if outcome == "cancelled" {
                    drop(accounting);
                } else {
                    accounting.finish_in_background(outcome, None, false);
                }
                (directory, capacity)
            });
            drop(runtime);
            assert_eq!(
                settlement(&directory),
                (outcome.to_owned(), Some(0), 1, 0, 0, 0),
            );
            assert_eq!(capacity.available_permits(), 1);
        }
    }

    #[test]
    fn models_remain_known_zero_when_shutdown_discards_the_finalizer() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let handle = runtime.handle().clone();
        let (directory, accounting, capacity) = runtime.block_on(async {
            let (directory, mut accounting, capacity) = admitted();
            accounting.dispatched(UsageKind::Models);
            accounting.observe_usage(Some(Usage {
                prompt_tokens: 30,
                completion_tokens: 20,
                total_tokens: 50,
            }));
            (directory, accounting, capacity)
        });
        drop(runtime);
        let _entered = handle.enter();
        drop(accounting);
        assert_eq!(
            settlement(&directory),
            ("cancelled".to_owned(), Some(0), 1, 0, 0, 0),
        );
        assert_eq!(capacity.available_permits(), 1);
    }

    #[tokio::test]
    async fn models_ignore_bogus_final_upstream_usage() {
        let (directory, mut accounting, capacity) = admitted();
        accounting.dispatched(UsageKind::Models);
        accounting
            .finish(
                "completed",
                Some(Usage {
                    prompt_tokens: 30,
                    completion_tokens: 20,
                    total_tokens: 50,
                }),
                true,
            )
            .await;
        assert_eq!(
            settlement(&directory),
            ("completed".to_owned(), Some(0), 1, 0, 0, 0),
        );
        assert_eq!(capacity.available_permits(), 1);
    }

    #[tokio::test]
    async fn malformed_embeddings_usage_retains_the_full_reservation() {
        use super::super::usage::UsageTracker;
        use axum::http::{HeaderMap, header};

        let (directory, mut accounting, capacity) = admitted();
        accounting.dispatched(UsageKind::Embeddings);
        let mut headers = HeaderMap::new();
        headers.insert(header::CONTENT_TYPE, "application/json".parse().unwrap());
        let mut tracker = UsageTracker::new(&headers, UsageKind::Embeddings);
        tracker.feed(br#"{"usage":{"prompt_tokens":3,"completion_tokens":2,"total_tokens":5}}"#);
        let usage = tracker.finish();
        accounting
            .finish("completed", usage, tracker.is_final())
            .await;
        assert_eq!(
            settlement(&directory),
            ("completed".to_owned(), None, 1, 0, 100, 0),
        );
        assert_eq!(capacity.available_permits(), 1);
    }
}
