use crate::config::JobPolicySnapshot;
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OptionalExtension};
use std::path::Path;
use std::sync::Mutex;

/// Thin wrapper: a single SQLite connection behind a Mutex, called via
/// spawn_blocking from async handlers.
#[derive(Clone)]
pub struct Store(std::sync::Arc<Mutex<Connection>>);

impl Store {
    pub fn open(path: &Path) -> Result<Self, StoreError> {
        let conn = Connection::open(path)
            .map_err(|e| StoreError::Open(format!("{path:?}: {e}")))?;
        conn.pragma_update(None, "journal_mode", "WAL")
            .map_err(StoreError::Sql)?;
        conn.pragma_update(None, "synchronous", "NORMAL")
            .map_err(StoreError::Sql)?;
        conn.pragma_update(None, "busy_timeout", 5000)
            .map_err(StoreError::Sql)?;
        let store = Store(std::sync::Arc::new(Mutex::new(conn)));
        store.migrate()?;
        Ok(store)
    }

    pub fn open_memory() -> Result<Self, StoreError> {
        let conn = Connection::open_in_memory().map_err(StoreError::Sql)?;
        let store = Store(std::sync::Arc::new(Mutex::new(conn)));
        store.migrate()?;
        Ok(store)
    }

    fn migrate(&self) -> Result<(), StoreError> {
        let conn = self.0.lock().unwrap();
        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS jobs (
                job_id TEXT PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                provider_id TEXT NOT NULL,
                upstream_key_id TEXT NOT NULL,
                token_hash TEXT NOT NULL UNIQUE,
                system_prompt TEXT,
                created_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                max_requests INTEGER NOT NULL,
                requests_used INTEGER NOT NULL DEFAULT 0,
                max_tokens_total INTEGER NOT NULL,
                tokens_used INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'active',
                revoked_at TEXT,
                revoked_reason TEXT
            );
            CREATE TABLE IF NOT EXISTS request_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id TEXT NOT NULL,
                ts TEXT NOT NULL,
                path TEXT NOT NULL,
                model TEXT,
                request_bytes INTEGER NOT NULL,
                response_bytes INTEGER NOT NULL DEFAULT 0,
                status_code INTEGER,
                duration_ms INTEGER,
                prompt_tokens INTEGER,
                completion_tokens INTEGER,
                tokens_estimated INTEGER NOT NULL DEFAULT 0,
                policy_verdict TEXT NOT NULL,
                block_reason TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_jobs_tenant ON jobs(tenant_id);
            CREATE INDEX IF NOT EXISTS idx_log_job ON request_log(job_id);
            CREATE INDEX IF NOT EXISTS idx_log_ts ON request_log(ts);
            "#,
        )
        .map_err(StoreError::Sql)?;
        Ok(())
    }

    // -- job lifecycle ------------------------------------------------------

    pub fn create_job(&self, j: &NewJob) -> Result<(), StoreError> {
        let conn = self.0.lock().unwrap();
        conn.execute(
            "INSERT INTO jobs (job_id, tenant_id, provider_id, upstream_key_id, token_hash, system_prompt,
             created_at, expires_at, max_requests, requests_used, max_tokens_total, tokens_used, status)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 0, ?10, 0, ?11)",
            params![
                j.job_id,
                j.tenant_id,
                j.provider_id,
                j.upstream_key_id,
                j.token_hash,
                j.system_prompt,
                j.created_at.to_rfc3339(),
                j.expires_at.to_rfc3339(),
                j.max_requests,
                j.max_tokens_total,
                j.status,
            ],
        )
        .map_err(StoreError::Sql)?;
        Ok(())
    }

    pub fn job_by_hash(&self, token_hash: &str) -> Option<JobPolicySnapshot> {
        let conn = self.0.lock().unwrap();
        conn.query_row(
            "SELECT job_id, tenant_id, provider_id, upstream_key_id, status, expires_at,
                    max_requests, requests_used, max_tokens_total, tokens_used
             FROM jobs WHERE token_hash = ?1",
            params![token_hash],
            row_to_snapshot,
        )
        .optional()
        .map_err(StoreError::Sql)
        .unwrap_or(None)
    }

    pub fn job_by_id(&self, job_id: &str) -> Option<JobPolicySnapshot> {
        let conn = self.0.lock().unwrap();
        conn.query_row(
            "SELECT job_id, tenant_id, provider_id, upstream_key_id, status, expires_at,
                    max_requests, requests_used, max_tokens_total, tokens_used
             FROM jobs WHERE job_id = ?1",
            params![job_id],
            row_to_snapshot,
        )
        .optional()
        .map_err(StoreError::Sql)
        .unwrap_or(None)
    }

    /// Atomic request-cap reservation. Returns Some(()) if incremented within
    /// the cap, None if the cap is exhausted (job is marked exhausted).
    pub fn reserve_request(&self, job_id: &str) -> Option<()> {
        let conn = self.0.lock().unwrap();
        let n = conn
            .execute(
                "UPDATE jobs SET requests_used = requests_used + 1
                 WHERE job_id = ?1 AND status = 'active' AND requests_used < max_requests",
                params![job_id],
            )
            .map_err(StoreError::Sql)
            .unwrap_or(0);
        if n == 1 {
            Some(())
        } else {
            // Mark exhausted only if the cap is what stopped us (not revocation).
            let capped: Option<i64> = conn
                .query_row(
                    "SELECT requests_used >= max_requests FROM jobs WHERE job_id = ?1 AND status = 'active'",
                    params![job_id],
                    |r| r.get(0),
                )
                .optional()
                .unwrap_or(None);
            if capped == Some(1) {
                let _ = conn.execute(
                    "UPDATE jobs SET status = 'exhausted' WHERE job_id = ?1 AND status = 'active'",
                    params![job_id],
                );
            }
            None
        }
    }

    /// Release a request reservation when the proxy failed before forwarding.
    pub fn release_request(&self, job_id: &str) {
        let conn = self.0.lock().unwrap();
        let _ = conn.execute(
            "UPDATE jobs SET requests_used = MAX(requests_used - 1, 0) WHERE job_id = ?1",
            params![job_id],
        );
    }

    /// Reconcile tokens after a request completes. Returns false if the
    /// reconciliation pushed the job over its token cap (job marked exhausted).
    pub fn add_tokens(&self, job_id: &str, total_tokens: u64) -> bool {
        let conn = self.0.lock().unwrap();
        let n = conn
            .execute(
                "UPDATE jobs SET tokens_used = tokens_used + ?2 WHERE job_id = ?1",
                params![job_id, total_tokens],
            )
            .map_err(StoreError::Sql)
            .unwrap_or(0);
        if n == 0 {
            return false;
        }
        conn.execute(
            "UPDATE jobs SET status = 'exhausted'
             WHERE job_id = ?1 AND status = 'active' AND tokens_used >= max_tokens_total",
            params![job_id],
        )
        .map_err(StoreError::Sql)
        .unwrap_or(0) == 1
    }

    pub fn mark_status(&self, job_id: &str, status: &str, reason: Option<&str>) -> bool {
        let conn = self.0.lock().unwrap();
        let now = Utc::now().to_rfc3339();
        let n = conn
            .execute(
                "UPDATE jobs SET status = ?2,
                 revoked_at = CASE WHEN ?2 IN ('revoked','done') THEN ?4 ELSE revoked_at END,
                 revoked_reason = CASE WHEN ?2 = 'revoked' THEN ?3 ELSE revoked_reason END
                 WHERE job_id = ?1 AND status = 'active'",
                params![job_id, status, reason, now],
            )
            .map_err(StoreError::Sql)
            .unwrap_or(0);
        n == 1
    }

    /// Laze-expire any active jobs past their TTL; called from auth path and sweep.
    pub fn sweep_expired(&self) -> usize {
        let conn = self.0.lock().unwrap();
        conn.execute(
            "UPDATE jobs SET status = 'expired'
             WHERE status = 'active' AND expires_at < ?1",
            params![Utc::now().to_rfc3339()],
        )
        .map_err(StoreError::Sql)
        .unwrap_or(0)
    }

    pub fn get_system_prompt(&self, job_id: &str) -> Option<String> {
        let conn = self.0.lock().unwrap();
        conn.query_row(
            "SELECT system_prompt FROM jobs WHERE job_id = ?1",
            params![job_id],
            |r| r.get::<_, Option<String>>(0),
        )
        .optional()
        .unwrap_or(None)
        .flatten()
    }

    // -- request log ---------------------------------------------------------

    pub fn log_request(&self, e: &LogEntry) {
        let conn = self.0.lock().unwrap();
        let _ = conn.execute(
            "INSERT INTO request_log (job_id, ts, path, model, request_bytes, response_bytes,
             status_code, duration_ms, prompt_tokens, completion_tokens, tokens_estimated,
             policy_verdict, block_reason)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
            params![
                e.job_id,
                e.ts.to_rfc3339(),
                e.path,
                e.model,
                e.request_bytes as i64,
                e.response_bytes as i64,
                e.status_code,
                e.duration_ms,
                e.prompt_tokens,
                e.completion_tokens,
                e.tokens_estimated,
                e.policy_verdict,
                e.block_reason,
            ],
        );
    }

    pub fn get_usage(&self, job_id: &str) -> Option<(u64, u64)> {
        let conn = self.0.lock().unwrap();
        conn.query_row(
            "SELECT requests_used, tokens_used FROM jobs WHERE job_id = ?1",
            params![job_id],
            |r| Ok((r.get::<_, i64>(0)? as u64, r.get::<_, i64>(1)? as u64)),
        )
        .optional()
        .unwrap_or(None)
    }
}

fn row_to_snapshot(r: &rusqlite::Row<'_>) -> rusqlite::Result<JobPolicySnapshot> {
    Ok(JobPolicySnapshot {
        job_id: r.get(0)?,
        tenant_id: r.get(1)?,
        provider_id: r.get(2)?,
        upstream_key_id: r.get(3)?,
        status: r.get(4)?,
        expires_at: DateTime::parse_from_rfc3339(&r.get::<_, String>(5)?)
            .map(|d| d.with_timezone(&Utc))
            .unwrap_or_else(|_| Utc::now()),
        max_requests: r.get::<_, i64>(6)? as u64,
        requests_used: r.get::<_, i64>(7)? as u64,
        max_tokens_total: r.get::<_, i64>(8)? as u64,
        tokens_used: r.get::<_, i64>(9)? as u64,
    })
}

pub struct NewJob {
    pub job_id: String,
    pub tenant_id: String,
    pub provider_id: String,
    pub upstream_key_id: String,
    pub token_hash: String,
    pub system_prompt: Option<String>,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub max_requests: u64,
    pub max_tokens_total: u64,
    pub status: String,
}

pub struct LogEntry<'a> {
    pub job_id: &'a str,
    pub ts: DateTime<Utc>,
    pub path: &'a str,
    pub model: Option<String>,
    pub request_bytes: usize,
    pub response_bytes: usize,
    pub status_code: Option<u16>,
    pub duration_ms: Option<u64>,
    pub prompt_tokens: Option<u64>,
    pub completion_tokens: Option<u64>,
    pub tokens_estimated: bool,
    pub policy_verdict: &'a str,
    pub block_reason: Option<&'a str>,
}

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("open: {0}")]
    Open(String),
    #[error("sql: {0}")]
    Sql(#[from] rusqlite::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snap(store: &Store, id: &str) -> JobPolicySnapshot {
        store.job_by_id(id).unwrap()
    }

    #[test]
    fn reserve_enforces_request_cap() {
        let s = Store::open_memory().unwrap();
        s.create_job(&NewJob {
            job_id: "j".into(),
            tenant_id: "t".into(),
            provider_id: "mistral".into(),
            upstream_key_id: "k".into(),
            token_hash: "h".into(),
            system_prompt: None,
            created_at: Utc::now(),
            expires_at: Utc::now() + chrono::Duration::minutes(30),
            max_requests: 2,
            max_tokens_total: 100,
            status: "active".into(),
        })
        .unwrap();
        assert!(s.reserve_request("j").is_some());
        assert!(s.reserve_request("j").is_some());
        assert!(s.reserve_request("j").is_none());
        assert_eq!(snap(&s, "j").status, "exhausted");
    }

    #[test]
    fn token_cap_marks_exhausted() {
        let s = Store::open_memory().unwrap();
        s.create_job(&NewJob {
            job_id: "j".into(),
            tenant_id: "t".into(),
            provider_id: "mistral".into(),
            upstream_key_id: "k".into(),
            token_hash: "h".into(),
            system_prompt: None,
            created_at: Utc::now(),
            expires_at: Utc::now() + chrono::Duration::minutes(30),
            max_requests: 10,
            max_tokens_total: 100,
            status: "active".into(),
        })
        .unwrap();
        s.reserve_request("j").unwrap();
        assert!(!s.add_tokens("j", 60)); // 60 < 100: still active
        assert_eq!(snap(&s, "j").status, "active");
        assert!(s.add_tokens("j", 60)); // 120 >= 100: marks exhausted
        assert_eq!(snap(&s, "j").status, "exhausted");
    }

    #[test]
    fn revoke_and_terminality() {
        let s = Store::open_memory().unwrap();
        s.create_job(&NewJob {
            job_id: "j".into(),
            tenant_id: "t".into(),
            provider_id: "mistral".into(),
            upstream_key_id: "k".into(),
            token_hash: "h".into(),
            system_prompt: None,
            created_at: Utc::now(),
            expires_at: Utc::now() + chrono::Duration::minutes(30),
            max_requests: 10,
            max_tokens_total: 100,
            status: "active".into(),
        })
        .unwrap();
        assert!(s.mark_status("j", "revoked", Some("test")));
        assert!(!s.mark_status("j", "active", None)); // terminal: cannot reactivate
        assert_eq!(snap(&s, "j").status, "revoked");
    }
}
