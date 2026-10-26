use std::{
    fs::{File, OpenOptions},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use rusqlite::{Connection, OpenFlags, OptionalExtension, TransactionBehavior, params};

const SCHEMA_VERSION: i64 = 3;

#[derive(Clone)]
pub struct Store {
    inner: Arc<StoreInner>,
}

struct StoreInner {
    connection: Mutex<Connection>,
    // Dropped after the connection, only when the final worker or owner releases it.
    _ownership: Option<File>,
}

pub(crate) struct NewJob {
    pub job_id: String,
    pub tenant_id: String,
    pub upstream_key_id: String,
    pub system_prompt: String,
    pub token_hash: [u8; 32],
    pub created_at: i64,
    pub expires_at: i64,
    pub max_requests: i64,
    pub max_tokens_total: i64,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct AdmittedJob {
    pub request_id: i64,
    pub upstream_key_id: String,
    pub system_prompt: String,
    pub reserved_tokens: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Usage {
    pub prompt_tokens: i64,
    pub completion_tokens: i64,
    pub total_tokens: i64,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum JobAdmission {
    Unauthorized,
    Exhausted,
    TokenLimited,
    Admitted(AdmittedJob),
}

pub(crate) enum InsertError {
    DuplicateJob,
    Storage,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum JobTransition {
    Revoke,
    Done,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum TransitionError {
    NotFound,
    Conflict,
    Storage,
}

impl Store {
    /// Opens with externally managed ownership, migrates, and recovers interrupted requests.
    /// The caller must exclude other live owners for the lifetime of every clone.
    /// Call from startup or a blocking worker, never a Tokio executor thread.
    pub fn open(path: &Path) -> Result<Self> {
        let path = prepare_database(path)?;
        Self::open_owned(&path, None)
    }

    /// Exclusively owns startup and recovery until the final Store clone is dropped.
    /// The persistent sidecar must not be removed or replaced while owners may exist.
    /// Call from startup or a blocking worker, never a Tokio executor thread.
    pub fn open_exclusive(path: &Path) -> Result<Self> {
        let path = prepare_database(path)?;
        let mut lock_name = path.as_os_str().to_os_string();
        lock_name.push(".lock");
        let lock_path = PathBuf::from(lock_name);
        match std::fs::symlink_metadata(&lock_path) {
            Ok(metadata) if !metadata.file_type().is_file() => {
                bail!("database ownership lock must be a regular, non-symlink file");
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error).context("could not inspect database ownership lock"),
        }
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true).truncate(false);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let lock = options
            .open(&lock_path)
            .context("could not open database ownership lock")?;
        let metadata = lock
            .metadata()
            .context("could not inspect ownership lock")?;
        if !metadata.is_file() {
            bail!("database ownership lock must be a regular file");
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if metadata.nlink() != 1 {
                bail!("database ownership lock must not have hard-link aliases");
            }
        }
        lock.try_lock()
            .context("database is already owned or filesystem locking is unavailable")?;
        Self::open_owned(&path, Some(lock))
    }

    fn open_owned(path: &Path, ownership: Option<File>) -> Result<Self> {
        let mut connection = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .context("could not open database")?;
        connection
            .busy_timeout(Duration::from_secs(5))
            .context("could not configure database busy timeout")?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .context("could not begin schema migration")?;
        let version: i64 = transaction
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .context("could not read database schema version")?;
        if version > SCHEMA_VERSION {
            bail!("database schema is newer than this application supports");
        }
        if version < 0 {
            bail!("invalid database schema version");
        }
        if version == 0 {
            transaction.execute_batch(
                "CREATE TABLE jobs (
                    job_id TEXT NOT NULL PRIMARY KEY,
                    tenant_id TEXT NOT NULL,
                    upstream_key_id TEXT NOT NULL,
                    system_prompt TEXT NOT NULL,
                    token_hash BLOB NOT NULL UNIQUE CHECK(typeof(token_hash) = 'blob' AND length(token_hash) = 32),
                    created_at INTEGER NOT NULL,
                    expires_at INTEGER NOT NULL CHECK(expires_at > created_at),
                    max_requests INTEGER NOT NULL CHECK(max_requests > 0),
                    requests_used INTEGER NOT NULL DEFAULT 0 CHECK(requests_used >= 0),
                    max_tokens_total INTEGER NOT NULL CHECK(max_tokens_total > 0),
                    tokens_used INTEGER NOT NULL DEFAULT 0 CHECK(tokens_used >= 0),
                    status TEXT NOT NULL DEFAULT 'active'
                        CHECK(status IN ('active', 'expired', 'revoked', 'exhausted', 'done')),
                    revoked_at INTEGER,
                    revoked_reason TEXT
                );
                PRAGMA user_version = 1;",
            )
            .context("could not migrate database schema")?;
        }
        if version <= 1 {
            transaction
                .execute_batch(
                    "ALTER TABLE jobs ADD COLUMN tokens_reserved INTEGER NOT NULL DEFAULT 0
                        CHECK(typeof(tokens_reserved) = 'integer' AND tokens_reserved >= 0);
                     CREATE TABLE request_records (
                        request_id INTEGER PRIMARY KEY,
                        job_id TEXT NOT NULL REFERENCES jobs(job_id),
                        admitted_at INTEGER NOT NULL,
                        finished_at INTEGER,
                        outcome TEXT NOT NULL DEFAULT 'pending'
                            CHECK(outcome IN ('pending', 'completed', 'rejected', 'upstream_error',
                                              'timeout', 'disconnected', 'cancelled', 'interrupted')),
                        reserved_tokens INTEGER NOT NULL DEFAULT 0
                            CHECK(typeof(reserved_tokens) = 'integer' AND reserved_tokens >= 0),
                        prompt_tokens INTEGER,
                        completion_tokens INTEGER,
                        total_tokens INTEGER,
                        CHECK((prompt_tokens IS NULL AND completion_tokens IS NULL AND total_tokens IS NULL)
                            OR (prompt_tokens IS NOT NULL AND completion_tokens IS NOT NULL
                                AND total_tokens IS NOT NULL
                                AND typeof(prompt_tokens) = 'integer' AND prompt_tokens >= 0
                                AND typeof(completion_tokens) = 'integer' AND completion_tokens >= 0
                                AND typeof(total_tokens) = 'integer' AND total_tokens >= prompt_tokens
                                AND total_tokens - prompt_tokens = completion_tokens))
                     );
                     CREATE INDEX request_records_pending ON request_records(request_id)
                        WHERE outcome = 'pending';
                     PRAGMA user_version = 2;",
                )
                .context("could not migrate request accounting schema")?;
        }
        if version <= 2 {
            transaction
                .execute_batch(
                    "ALTER TABLE jobs ADD COLUMN tokens_unreported INTEGER NOT NULL DEFAULT 0
                        CHECK(typeof(tokens_unreported) = 'integer' AND tokens_unreported >= 0);
                     PRAGMA user_version = 3;",
                )
                .context("could not migrate token budget accounting schema")?;
        }
        let now = epoch_seconds()?;
        let pending = {
            let mut statement = transaction
                .prepare("SELECT request_id FROM request_records WHERE outcome = 'pending'")
                .context("could not find interrupted requests")?;
            statement
                .query_map([], |row| row.get::<_, i64>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?
        };
        for request_id in pending {
            finish_request_in_transaction(&transaction, request_id, "interrupted", None, now)?;
        }
        transaction
            .execute(
                "UPDATE jobs SET status = 'expired'
                 WHERE status = 'active' AND expires_at <= ?1",
                params![now],
            )
            .context("could not expire jobs at startup")?;
        transaction
            .commit()
            .context("could not commit schema migration")?;
        Ok(Self {
            inner: Arc::new(StoreInner {
                connection: Mutex::new(connection),
                _ownership: ownership,
            }),
        })
    }

    /// A single INSERT makes duplicate detection atomic across connections.
    pub(crate) fn insert_job(&self, job: NewJob) -> Result<(), InsertError> {
        let connection = self
            .inner
            .connection
            .lock()
            .map_err(|_| InsertError::Storage)?;
        match connection.execute(
            "INSERT INTO jobs (
                job_id, tenant_id, upstream_key_id, system_prompt, token_hash,
                created_at, expires_at, max_requests, max_tokens_total,
                requests_used, tokens_used, status, revoked_at, revoked_reason
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 0, 0, 'active', NULL, NULL)",
            params![
                job.job_id,
                job.tenant_id,
                job.upstream_key_id,
                job.system_prompt,
                job.token_hash.as_slice(),
                job.created_at,
                job.expires_at,
                job.max_requests,
                job.max_tokens_total,
            ],
        ) {
            Ok(_) => Ok(()),
            Err(rusqlite::Error::SqliteFailure(error, _))
                if error.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_PRIMARYKEY =>
            {
                Err(InsertError::DuplicateJob)
            }
            Err(_) => Err(InsertError::Storage),
        }
    }

    /// Atomically charges admission and escrows every currently available job token.
    /// Temporary escrow shortages charge nothing; the last request keeps its snapshot.
    pub(crate) fn admit_job(&self, token_hash: [u8; 32]) -> Result<JobAdmission> {
        let mut connection = self
            .inner
            .connection
            .lock()
            .map_err(|_| anyhow::anyhow!("database lock failed"))?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .context("could not begin job admission")?;
        let job = admit_job_in_transaction(&transaction, token_hash, epoch_seconds()?)?;
        transaction
            .commit()
            .context("could not commit job admission")?;
        Ok(job)
    }

    /// Shrinks an admitted request's escrow without rechecking the job lifecycle.
    /// The blocking caller must retain its Accounting owner until this returns.
    pub(crate) fn constrain_reservation(
        &self,
        request_id: i64,
        reserved_tokens: i64,
    ) -> Result<()> {
        if reserved_tokens < 0 {
            bail!("request reservation must be nonnegative");
        }
        let mut connection = self
            .inner
            .connection
            .lock()
            .map_err(|_| anyhow::anyhow!("database lock failed"))?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .context("could not begin reservation constraint")?;
        let (job_id, previous, job_reserved): (String, i64, i64) = transaction
            .query_row(
                "SELECT r.job_id, r.reserved_tokens, j.tokens_reserved
                 FROM request_records r JOIN jobs j USING (job_id)
                 WHERE r.request_id = ?1 AND r.outcome = 'pending'",
                params![request_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .context("could not find pending request reservation")?;
        let released = previous
            .checked_sub(reserved_tokens)
            .filter(|released| *released >= 0)
            .context("request reservation cannot grow")?;
        let remaining = job_reserved
            .checked_sub(released)
            .filter(|remaining| *remaining >= 0)
            .context("request reservation exceeds job reservation")?;
        transaction.execute(
            "UPDATE request_records SET reserved_tokens = ?2 WHERE request_id = ?1",
            params![request_id, reserved_tokens],
        )?;
        transaction.execute(
            "UPDATE jobs SET tokens_reserved = ?2 WHERE job_id = ?1",
            params![job_id, remaining],
        )?;
        transaction
            .commit()
            .context("could not commit reservation constraint")
    }

    /// Persists the first outcome, releasing escrow into known or unreported spending once.
    pub(crate) fn finish_request(
        &self,
        request_id: i64,
        outcome: &str,
        usage: Option<Usage>,
    ) -> Result<()> {
        if !matches!(
            outcome,
            "completed" | "rejected" | "upstream_error" | "timeout" | "disconnected" | "cancelled"
        ) {
            bail!("invalid request completion outcome");
        }
        if let Some(usage) = usage
            && (usage.prompt_tokens < 0
                || usage.completion_tokens < 0
                || usage.total_tokens < 0
                || usage.prompt_tokens.checked_add(usage.completion_tokens)
                    != Some(usage.total_tokens))
        {
            bail!("invalid request token usage");
        }
        let mut connection = self
            .inner
            .connection
            .lock()
            .map_err(|_| anyhow::anyhow!("database lock failed"))?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .context("could not begin request completion")?;
        finish_request_in_transaction(&transaction, request_id, outcome, usage, epoch_seconds()?)?;
        transaction
            .commit()
            .context("could not commit request completion")
    }

    /// Materializes expiry before applying a terminal transition. The first
    /// terminal state wins, including across independently opened connections.
    pub(crate) fn transition_job(
        &self,
        job_id: &str,
        transition: JobTransition,
    ) -> Result<(), TransitionError> {
        let mut connection = self
            .inner
            .connection
            .lock()
            .map_err(|_| TransitionError::Storage)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| TransitionError::Storage)?;
        let now = epoch_seconds().map_err(|_| TransitionError::Storage)?;
        transaction
            .execute(
                "UPDATE jobs SET status = 'expired'
                 WHERE job_id = ?1 AND status = 'active' AND expires_at <= ?2",
                params![job_id, now],
            )
            .map_err(|_| TransitionError::Storage)?;
        let (target_status, revoked_at) = match transition {
            JobTransition::Revoke => ("revoked", Some(now)),
            JobTransition::Done => ("done", None),
        };
        let state = transaction
            .query_row(
                "SELECT status = 'active', status = ?2 FROM jobs WHERE job_id = ?1",
                params![job_id, target_status],
                |row| Ok((row.get::<_, bool>(0)?, row.get::<_, bool>(1)?)),
            )
            .optional()
            .map_err(|_| TransitionError::Storage)?;
        let outcome = match state {
            None => Err(TransitionError::NotFound),
            Some((true, _)) => {
                transaction
                    .execute(
                        "UPDATE jobs SET status = ?2, revoked_at = ?3, revoked_reason = NULL
                         WHERE job_id = ?1 AND status = 'active'",
                        params![job_id, target_status, revoked_at],
                    )
                    .map_err(|_| TransitionError::Storage)?;
                Ok(())
            }
            Some((false, true)) => Ok(()),
            Some((false, false)) => Err(TransitionError::Conflict),
        };
        // Even a conflict must persist expiry materialized by this transaction.
        transaction.commit().map_err(|_| TransitionError::Storage)?;
        outcome
    }
}

fn prepare_database(path: &Path) -> Result<PathBuf> {
    if path.as_os_str().is_empty()
        || path == Path::new(":memory:")
        || path.to_string_lossy().starts_with("file:")
    {
        bail!("database_path must identify a persistent filesystem database");
    }
    // Validate an existing target before opening it (notably devices and FIFOs).
    match path.metadata() {
        Ok(metadata) if !metadata.is_file() => {
            bail!("database_path must identify a regular file");
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error).context("could not inspect database file"),
    }
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    match options.open(path) {
        Ok(file) => drop(file),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error).context("could not create database file"),
    }
    if !path
        .metadata()
        .context("could not inspect database file")?
        .is_file()
    {
        bail!("database_path must identify a regular file");
    }
    path.canonicalize()
        .context("could not resolve database path")
}

fn epoch_seconds() -> Result<i64> {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before the Unix epoch")?
        .as_secs();
    i64::try_from(seconds).context("system clock exceeds the supported range")
}

fn finish_request_in_transaction(
    transaction: &rusqlite::Transaction<'_>,
    request_id: i64,
    outcome: &str,
    usage: Option<Usage>,
    now: i64,
) -> Result<()> {
    let pending = transaction
        .query_row(
            "SELECT r.job_id, r.reserved_tokens, j.tokens_reserved, j.tokens_used,
                    j.tokens_unreported, j.max_tokens_total
             FROM request_records r JOIN jobs j ON j.job_id = r.job_id
             WHERE r.request_id = ?1 AND r.outcome = 'pending'",
            params![request_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, i64>(5)?,
                ))
            },
        )
        .optional()
        .context("could not read pending request accounting")?;
    let Some((job_id, reserved, tokens_reserved, tokens_used, tokens_unreported, maximum)) =
        pending
    else {
        return Ok(());
    };
    let remaining = tokens_reserved
        .checked_sub(reserved)
        .filter(|remaining| *remaining >= 0)
        .context("request reservation exceeds job reservation")?;
    let tokens_used = tokens_used
        .checked_add(usage.map_or(0, |usage| usage.total_tokens))
        .context("job token usage overflow")?;
    let tokens_unreported = tokens_unreported
        .checked_add(if usage.is_none() { reserved } else { 0 })
        .context("job unreported token usage overflow")?;
    let exhausted = spent_budget(tokens_used, tokens_unreported, maximum)?;
    transaction
        .execute(
            "UPDATE request_records
             SET finished_at = ?2, outcome = ?3, prompt_tokens = ?4,
                 completion_tokens = ?5, total_tokens = ?6
             WHERE request_id = ?1 AND outcome = 'pending'",
            params![
                request_id,
                now,
                outcome,
                usage.map(|usage| usage.prompt_tokens),
                usage.map(|usage| usage.completion_tokens),
                usage.map(|usage| usage.total_tokens),
            ],
        )
        .context("could not finish request record")?;
    transaction
        .execute(
            "UPDATE jobs SET tokens_reserved = ?2, tokens_used = ?3, tokens_unreported = ?4,
                 status = CASE WHEN status = 'active' AND ?5 THEN 'exhausted' ELSE status END
             WHERE job_id = ?1",
            params![job_id, remaining, tokens_used, tokens_unreported, exhausted],
        )
        .context("could not reconcile job token accounting")?;
    Ok(())
}

fn admit_job_in_transaction(
    transaction: &rusqlite::Transaction<'_>,
    token_hash: [u8; 32],
    now: i64,
) -> Result<JobAdmission> {
    transaction
        .execute(
            "UPDATE jobs SET status = 'expired'
             WHERE token_hash = ?1 AND status = 'active' AND expires_at <= ?2",
            params![token_hash.as_slice(), now],
        )
        .context("could not expire job during admission")?;
    let job = transaction
        .query_row(
            "SELECT job_id, upstream_key_id, system_prompt, status, requests_used,
                    max_requests, max_tokens_total, tokens_used, tokens_unreported, tokens_reserved
             FROM jobs WHERE token_hash = ?1",
            params![token_hash.as_slice()],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, i64>(5)?,
                    row.get::<_, i64>(6)?,
                    row.get::<_, i64>(7)?,
                    row.get::<_, i64>(8)?,
                    row.get::<_, i64>(9)?,
                ))
            },
        )
        .optional()
        .context("could not read job admission budget")?;
    let Some((
        job_id,
        upstream_key_id,
        system_prompt,
        status,
        requests_used,
        max_requests,
        maximum,
        used,
        unreported,
        reserved,
    )) = job
    else {
        return Ok(JobAdmission::Unauthorized);
    };
    if status == "exhausted" {
        return Ok(JobAdmission::Exhausted);
    }
    if status != "active" {
        return Ok(JobAdmission::Unauthorized);
    }
    if requests_used >= max_requests {
        transaction.execute(
            "UPDATE jobs SET status = 'exhausted' WHERE job_id = ?1 AND status = 'active'",
            params![job_id],
        )?;
        return Ok(JobAdmission::Exhausted);
    }
    if spent_budget(used, unreported, maximum)? {
        transaction.execute(
            "UPDATE jobs SET status = 'exhausted' WHERE job_id = ?1 AND status = 'active'",
            params![job_id],
        )?;
        return Ok(JobAdmission::TokenLimited);
    }
    let available = maximum
        .checked_sub(used)
        .context("job token budget subtraction overflow")?
        .max(0)
        .checked_sub(unreported)
        .context("job unreported budget subtraction overflow")?
        .max(0)
        .checked_sub(reserved)
        .context("job reservation subtraction overflow")?
        .max(0);
    if available == 0 {
        return Ok(JobAdmission::TokenLimited);
    }
    let requests_used = requests_used
        .checked_add(1)
        .context("job request count overflow")?;
    let tokens_reserved = reserved
        .checked_add(available)
        .context("job reservation overflow")?;
    transaction
        .execute(
            "UPDATE jobs SET requests_used = ?2, tokens_reserved = ?3,
             status = CASE WHEN ?2 = max_requests THEN 'exhausted' ELSE status END
         WHERE job_id = ?1",
            params![job_id, requests_used, tokens_reserved],
        )
        .context("could not charge job admission")?;
    transaction.execute(
        "INSERT INTO request_records (job_id, admitted_at, reserved_tokens) VALUES (?1, ?2, ?3)",
        params![job_id, now, available],
    ).context("could not record job admission")?;
    Ok(JobAdmission::Admitted(AdmittedJob {
        request_id: transaction.last_insert_rowid(),
        upstream_key_id,
        system_prompt,
        reserved_tokens: available,
    }))
}

fn spent_budget(used: i64, unreported: i64, maximum: i64) -> Result<bool> {
    if used >= maximum {
        return Ok(true);
    }
    Ok(unreported
        >= maximum
            .checked_sub(used)
            .context("job token budget overflow")?)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn insert_job(store: &Store, job_id: &str, token: u8, expires_at: i64) {
        assert!(
            store
                .insert_job(NewJob {
                    job_id: job_id.to_owned(),
                    tenant_id: "tenant".to_owned(),
                    upstream_key_id: "upstream".to_owned(),
                    system_prompt: "system".to_owned(),
                    token_hash: [token; 32],
                    created_at: 1,
                    expires_at,
                    max_requests: 10,
                    max_tokens_total: 1_000,
                })
                .is_ok()
        );
    }

    #[test]
    fn exclusive_worker_retains_ownership_and_known_usage_after_root_drop() {
        use std::sync::mpsc;

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("jobs.sqlite3");
        let store = Store::open_exclusive(&path).unwrap();
        insert_job(&store, "job", 1, i64::MAX);
        let JobAdmission::Admitted(admitted) = store.admit_job([1; 32]).unwrap() else {
            panic!("request must be admitted");
        };
        let worker_store = store.clone();
        let observer = Connection::open(&path).unwrap();
        // An actual SQLite write transaction blocks reconciliation deterministically.
        observer.execute_batch("BEGIN IMMEDIATE").unwrap();
        let (started_tx, started_rx) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            let connection = worker_store.inner.connection.lock().unwrap();
            started_tx.send(()).unwrap();
            drop(connection);
            worker_store
                .finish_request(
                    admitted.request_id,
                    "completed",
                    Some(Usage {
                        prompt_tokens: 7,
                        completion_tokens: 5,
                        total_tokens: 12,
                    }),
                )
                .unwrap();
        });
        started_rx.recv().unwrap();
        drop(store);
        assert!(Store::open_exclusive(&path).is_err());
        let outcome: String = observer
            .query_row("SELECT outcome FROM request_records", [], |row| row.get(0))
            .unwrap();
        assert_eq!(outcome, "pending");
        observer.execute_batch("COMMIT").unwrap();
        worker.join().unwrap();
        let reopened = Store::open_exclusive(&path).unwrap();
        let record: (String, Option<i64>, i64, i64) = observer
            .query_row(
                "SELECT outcome, total_tokens, tokens_used, tokens_reserved
             FROM request_records JOIN jobs USING (job_id)",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(record, ("completed".to_owned(), Some(12), 12, 0));
        drop(reopened);
        assert!(directory.path().join("jobs.sqlite3.lock").is_file());
    }

    #[test]
    fn exclusive_clone_blocks_recovery_until_last_owner_drops() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("jobs.sqlite3");
        let store = Store::open_exclusive(&path).unwrap();
        insert_job(&store, "job", 1, i64::MAX);
        assert!(matches!(
            store.admit_job([1; 32]).unwrap(),
            JobAdmission::Admitted(_)
        ));
        let clone = store.clone();
        drop(store);
        assert!(Store::open_exclusive(&path).is_err());
        drop(clone);
        let reopened = Store::open_exclusive(&path).unwrap();
        let connection = reopened.inner.connection.lock().unwrap();
        let record: (String, Option<i64>, i64) = connection
            .query_row(
                "SELECT outcome, total_tokens, tokens_reserved
             FROM request_records JOIN jobs USING (job_id)",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(record, ("interrupted".to_owned(), None, 0));
        assert_eq!(budget_state(&connection, "job"), (0, 0, 1_000));
        assert_eq!(quota_state(&connection, "job"), ("exhausted".to_owned(), 1));
    }

    #[test]
    fn sidecar_does_not_lock_sqlite_bytes_and_relative_aliases_contend() {
        let directory = tempfile::tempdir_in(".").unwrap();
        let path = directory.path().join("jobs.sqlite3");
        let store = Store::open_exclusive(&path).unwrap();
        let absolute = path.canonicalize().unwrap();
        assert!(Store::open_exclusive(&absolute).is_err());
        let observer = Connection::open(&path).unwrap();
        observer.execute_batch(
            "BEGIN IMMEDIATE; CREATE TABLE lock_probe (value INTEGER); INSERT INTO lock_probe VALUES (1); COMMIT;"
        ).unwrap();
        assert_eq!(
            observer
                .query_row("SELECT value FROM lock_probe", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            1
        );
        let database = File::open(&path).unwrap();
        database.try_lock().unwrap();
        drop(database);
        let sidecar = directory.path().join("jobs.sqlite3.lock");
        #[cfg(unix)]
        {
            use std::os::unix::fs::{MetadataExt, PermissionsExt};
            assert_ne!(
                path.metadata().unwrap().ino(),
                sidecar.metadata().unwrap().ino()
            );
            assert_eq!(
                sidecar.metadata().unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
        drop(store);
        assert!(Store::open_exclusive(&absolute).is_ok());
    }

    #[cfg(unix)]
    #[test]
    fn symlink_aliases_share_the_canonical_sidecar() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("jobs.sqlite3");
        let store = Store::open_exclusive(&path).unwrap();
        let alias = directory.path().join("alias.sqlite3");
        std::os::unix::fs::symlink(&path, &alias).unwrap();
        assert!(Store::open_exclusive(&alias).is_err());
        assert!(!directory.path().join("alias.sqlite3.lock").exists());
        drop(store);
        assert!(Store::open_exclusive(&alias).is_ok());
    }

    #[test]
    fn exclusive_open_rejects_invalid_paths_before_creating_files() {
        assert!(Store::open_exclusive(Path::new("")).is_err());
        assert!(Store::open_exclusive(Path::new(":memory:")).is_err());
        assert!(Store::open_exclusive(Path::new("file:invalid.sqlite3")).is_err());
        let directory = tempfile::tempdir().unwrap();
        assert!(Store::open_exclusive(directory.path()).is_err());
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
    }

    #[cfg(unix)]
    #[test]
    fn exclusive_open_rejects_sidecar_aliases_and_special_files() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("jobs.sqlite3");
        drop(Store::open(&path).unwrap());
        let sidecar = directory.path().join("jobs.sqlite3.lock");
        std::os::unix::fs::symlink(&path, &sidecar).unwrap();
        assert!(Store::open_exclusive(&path).is_err());
        std::fs::remove_file(&sidecar).unwrap();
        std::fs::hard_link(&path, &sidecar).unwrap();
        assert!(Store::open_exclusive(&path).is_err());
        std::fs::remove_file(&sidecar).unwrap();
        std::fs::create_dir(&sidecar).unwrap();
        assert!(Store::open_exclusive(&path).is_err());
        // A device must be rejected without attempting creation, SQLite, or locking.
        assert!(Store::open_exclusive(Path::new("/dev/null")).is_err());
        let connection = Connection::open(&path).unwrap();
        assert_eq!(
            connection
                .pragma_query_value(None, "user_version", |row| row.get::<_, i64>(0))
                .unwrap(),
            SCHEMA_VERSION
        );
    }

    fn job_state(connection: &Connection, job_id: &str) -> (String, Option<i64>, Option<String>) {
        connection
            .query_row(
                "SELECT status, revoked_at, revoked_reason FROM jobs WHERE job_id = ?1",
                params![job_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap()
    }

    fn quota_state(connection: &Connection, job_id: &str) -> (String, i64) {
        connection
            .query_row(
                "SELECT status, requests_used FROM jobs WHERE job_id = ?1",
                params![job_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap()
    }

    #[test]
    fn admission_expires_at_the_exact_boundary() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("jobs.sqlite3");
        let store = Store::open(&path).unwrap();
        insert_job(&store, "before", 1, 999);
        insert_job(&store, "boundary", 2, 1_000);
        insert_job(&store, "after", 3, 1_001);

        let mut connection = store.inner.connection.lock().unwrap();
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        assert_eq!(
            admit_job_in_transaction(&transaction, [1; 32], 1_000).unwrap(),
            JobAdmission::Unauthorized
        );
        assert_eq!(
            admit_job_in_transaction(&transaction, [2; 32], 1_000).unwrap(),
            JobAdmission::Unauthorized
        );
        assert_eq!(
            admit_job_in_transaction(&transaction, [3; 32], 1_000).unwrap(),
            JobAdmission::Admitted(AdmittedJob {
                request_id: 1,
                upstream_key_id: "upstream".to_owned(),
                system_prompt: "system".to_owned(),
                reserved_tokens: 1_000,
            })
        );
        transaction.commit().unwrap();
        drop(connection);

        let observer = Connection::open(&path).unwrap();
        assert_eq!(job_state(&observer, "before").0, "expired");
        assert_eq!(job_state(&observer, "boundary").0, "expired");
        assert_eq!(job_state(&observer, "after").0, "active");
    }

    #[test]
    fn admission_commits_expiry_only_for_the_matching_job() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("jobs.sqlite3");
        let store = Store::open(&path).unwrap();
        insert_job(&store, "overdue", 1, 2);
        insert_job(&store, "unrelated", 2, 2);
        insert_job(&store, "future", 3, i64::MAX);

        assert_eq!(
            store.admit_job([1; 32]).unwrap(),
            JobAdmission::Unauthorized
        );
        assert_eq!(
            store.admit_job([9; 32]).unwrap(),
            JobAdmission::Unauthorized
        );
        assert_eq!(
            store.admit_job([3; 32]).unwrap(),
            JobAdmission::Admitted(AdmittedJob {
                request_id: 1,
                upstream_key_id: "upstream".to_owned(),
                system_prompt: "system".to_owned(),
                reserved_tokens: 1_000,
            })
        );

        let observer = Connection::open(&path).unwrap();
        assert_eq!(
            job_state(&observer, "overdue"),
            ("expired".to_owned(), None, None)
        );
        assert_eq!(job_state(&observer, "unrelated").0, "active");
        assert_eq!(job_state(&observer, "future").0, "active");
        drop(store);
        let reopened = Store::open(&path).unwrap();
        assert_eq!(
            reopened.admit_job([1; 32]).unwrap(),
            JobAdmission::Unauthorized
        );
        assert_eq!(job_state(&observer, "overdue").0, "expired");
    }

    #[test]
    fn startup_persists_expiry_without_overwriting_terminal_states() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("jobs.sqlite3");
        let store = Store::open(&path).unwrap();
        insert_job(&store, "overdue", 1, 2);
        insert_job(&store, "future", 2, i64::MAX);
        let observer = Connection::open(&path).unwrap();
        let terminal_states = ["revoked", "done", "exhausted", "expired"];
        for (index, status) in terminal_states.iter().enumerate() {
            insert_job(&store, status, index as u8 + 3, 2);
            observer
                .execute(
                    "UPDATE jobs SET status = ?2 WHERE job_id = ?1",
                    params![status, status],
                )
                .unwrap();
        }
        observer
            .execute(
                "UPDATE jobs SET revoked_at = 2, revoked_reason = 'original reason'
                 WHERE job_id = 'revoked'",
                [],
            )
            .unwrap();
        let snapshots: Vec<_> = terminal_states
            .iter()
            .map(|status| job_state(&observer, status))
            .collect();
        drop(store);

        drop(Store::open(&path).unwrap());
        assert_eq!(
            job_state(&observer, "overdue"),
            ("expired".to_owned(), None, None)
        );
        assert_eq!(job_state(&observer, "future").0, "active");
        for (status, snapshot) in terminal_states.iter().zip(snapshots) {
            assert_eq!(job_state(&observer, status), snapshot);
        }
        drop(Store::open(&path).unwrap());
        assert_eq!(job_state(&observer, "overdue").0, "expired");
    }

    #[test]
    fn terminal_transitions_are_durable_idempotent_and_conflicting() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("jobs.sqlite3");
        let store = Store::open(&path).unwrap();
        let observer = Connection::open(&path).unwrap();
        let cases = [
            ("revoked", 1, JobTransition::Revoke, JobTransition::Done),
            ("done", 2, JobTransition::Done, JobTransition::Revoke),
        ];
        let mut snapshots = Vec::new();
        for (job_id, token, transition, other_transition) in cases {
            insert_job(&store, job_id, token, i64::MAX);
            assert_eq!(store.transition_job(job_id, transition), Ok(()));
            let state = job_state(&observer, job_id);
            assert_eq!(state.0, job_id);
            assert_eq!(state.2, None);
            observer
                .execute(
                    "UPDATE jobs SET expires_at = 2 WHERE job_id = ?1",
                    params![job_id],
                )
                .unwrap();
            if transition == JobTransition::Revoke {
                assert!(state.1.is_some_and(|timestamp| timestamp > 0));
                // A fixed earlier timestamp makes accidental rewrites visible
                // without sleeping across a wall-clock second.
                observer
                    .execute(
                        "UPDATE jobs SET revoked_at = 2, revoked_reason = 'original reason'
                         WHERE job_id = ?1",
                        params![job_id],
                    )
                    .unwrap();
            } else {
                assert_eq!(state.1, None);
            }
            let snapshot = job_state(&observer, job_id);
            assert_eq!(store.transition_job(job_id, transition), Ok(()));
            assert_eq!(
                store.transition_job(job_id, other_transition),
                Err(TransitionError::Conflict)
            );
            assert_eq!(
                store.admit_job([token; 32]).unwrap(),
                JobAdmission::Unauthorized
            );
            assert_eq!(job_state(&observer, job_id), snapshot);
            snapshots.push((job_id, snapshot));
        }
        drop(store);

        let reopened = Store::open(&path).unwrap();
        for (job_id, snapshot) in snapshots {
            assert_eq!(job_state(&observer, job_id), snapshot);
            let transition = if job_id == "revoked" {
                JobTransition::Revoke
            } else {
                JobTransition::Done
            };
            assert_eq!(reopened.transition_job(job_id, transition), Ok(()));
            assert_eq!(job_state(&observer, job_id), snapshot);
        }
    }

    #[test]
    fn transitions_persist_expiry_and_distinguish_missing_jobs() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("jobs.sqlite3");
        let store = Store::open(&path).unwrap();
        let observer = Connection::open(&path).unwrap();
        for (job_id, token, transition) in [
            ("overdue_revoke", 1, JobTransition::Revoke),
            ("overdue_done", 2, JobTransition::Done),
        ] {
            insert_job(&store, job_id, token, 2);
            assert_eq!(
                store.transition_job(job_id, transition),
                Err(TransitionError::Conflict)
            );
            assert_eq!(
                job_state(&observer, job_id),
                ("expired".to_owned(), None, None)
            );
            assert_eq!(
                store.transition_job("missing", transition),
                Err(TransitionError::NotFound)
            );
        }

        insert_job(&store, "exhausted", 3, i64::MAX);
        observer
            .execute(
                "UPDATE jobs SET status = 'exhausted' WHERE job_id = 'exhausted'",
                [],
            )
            .unwrap();
        for transition in [JobTransition::Revoke, JobTransition::Done] {
            assert_eq!(
                store.transition_job("exhausted", transition),
                Err(TransitionError::Conflict)
            );
        }
        assert_eq!(job_state(&observer, "exhausted").0, "exhausted");
    }

    #[test]
    fn failed_transition_leaves_the_job_active() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("jobs.sqlite3");
        let store = Store::open(&path).unwrap();
        insert_job(&store, "job", 1, i64::MAX);
        let observer = Connection::open(&path).unwrap();
        observer
            .execute_batch(
                "CREATE TRIGGER reject_terminal_transition BEFORE UPDATE OF status ON jobs
                 WHEN NEW.status IN ('revoked', 'done')
                 BEGIN SELECT RAISE(ABORT, 'transition rejected'); END;",
            )
            .unwrap();
        for transition in [JobTransition::Revoke, JobTransition::Done] {
            assert_eq!(
                store.transition_job("job", transition),
                Err(TransitionError::Storage)
            );
            assert_eq!(
                job_state(&observer, "job"),
                ("active".to_owned(), None, None)
            );
        }
    }

    #[test]
    fn independent_connections_serialize_competing_transitions() {
        use std::sync::Barrier;

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("jobs.sqlite3");
        let first = Store::open(&path).unwrap();
        let second = Store::open(&path).unwrap();
        let observer = Connection::open(&path).unwrap();
        let cases = [
            (
                "both_revoke",
                1,
                JobTransition::Revoke,
                JobTransition::Revoke,
                i64::MAX,
            ),
            (
                "both_done",
                2,
                JobTransition::Done,
                JobTransition::Done,
                i64::MAX,
            ),
            (
                "competing",
                3,
                JobTransition::Revoke,
                JobTransition::Done,
                i64::MAX,
            ),
            ("overdue", 4, JobTransition::Revoke, JobTransition::Done, 2),
        ];
        for (job_id, token, left_transition, right_transition, expires_at) in cases {
            insert_job(&first, job_id, token, expires_at);
            let barrier = Barrier::new(2);
            let (left_result, right_result) = std::thread::scope(|scope| {
                let left = scope.spawn(|| {
                    barrier.wait();
                    first.transition_job(job_id, left_transition)
                });
                let right = scope.spawn(|| {
                    barrier.wait();
                    second.transition_job(job_id, right_transition)
                });
                (left.join().unwrap(), right.join().unwrap())
            });

            if expires_at == 2 {
                assert_eq!(left_result, Err(TransitionError::Conflict));
                assert_eq!(right_result, Err(TransitionError::Conflict));
                assert_eq!(job_state(&observer, job_id).0, "expired");
                continue;
            }
            let winner = if left_transition == right_transition {
                assert_eq!(left_result, Ok(()));
                assert_eq!(right_result, Ok(()));
                left_transition
            } else {
                match (left_result, right_result) {
                    (Ok(()), Err(TransitionError::Conflict)) => left_transition,
                    (Err(TransitionError::Conflict), Ok(())) => right_transition,
                    results => panic!("expected exactly one terminal winner, got {results:?}"),
                }
            };
            let status = match winner {
                JobTransition::Revoke => "revoked",
                JobTransition::Done => "done",
            };
            assert_eq!(job_state(&observer, job_id).0, status);
            assert_eq!(
                first.admit_job([token; 32]).unwrap(),
                JobAdmission::Unauthorized
            );
            assert_eq!(second.transition_job(job_id, winner), Ok(()));
        }
    }

    #[test]
    fn independent_connections_charge_only_the_remaining_allowance() {
        use std::sync::Barrier;

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("jobs.sqlite3");
        let store = Store::open(&path).unwrap();
        insert_job(&store, "job", 1, i64::MAX);
        let observer = Connection::open(&path).unwrap();
        observer
            .execute("UPDATE jobs SET requests_used = 7 WHERE job_id = 'job'", [])
            .unwrap();
        let contenders: Vec<_> = (0..12).map(|_| Store::open(&path).unwrap()).collect();
        let barrier = Barrier::new(contenders.len());
        let results = std::thread::scope(|scope| {
            let handles: Vec<_> = contenders
                .iter()
                .map(|contender| {
                    let barrier = &barrier;
                    scope.spawn(move || {
                        barrier.wait();
                        loop {
                            match contender.admit_job([1; 32]).unwrap() {
                                JobAdmission::Admitted(job) => {
                                    contender.constrain_reservation(job.request_id, 1).unwrap();
                                    break JobAdmission::Admitted(job);
                                }
                                JobAdmission::TokenLimited => std::thread::yield_now(),
                                other => break other,
                            }
                        }
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|handle| handle.join().unwrap())
                .collect::<Vec<_>>()
        });
        assert_eq!(
            results
                .iter()
                .filter(|result| matches!(result, JobAdmission::Admitted(_)))
                .count(),
            3
        );
        assert_eq!(
            results
                .iter()
                .filter(|result| **result == JobAdmission::Exhausted)
                .count(),
            9
        );
        assert_eq!(quota_state(&observer, "job"), ("exhausted".to_owned(), 10));
        drop(contenders);
        drop(store);

        let reopened = Store::open(&path).unwrap();
        assert_eq!(
            reopened.admit_job([1; 32]).unwrap(),
            JobAdmission::Exhausted
        );
        assert_eq!(quota_state(&observer, "job"), ("exhausted".to_owned(), 10));
    }

    #[test]
    fn final_slot_returns_the_snapshot_and_exhaustion_wins_over_expiry_and_lifecycle() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("jobs.sqlite3");
        let store = Store::open(&path).unwrap();
        insert_job(&store, "job", 1, i64::MAX);
        let observer = Connection::open(&path).unwrap();
        observer
            .execute(
                "UPDATE jobs SET max_requests = 1, upstream_key_id = 'chosen-key',
                 system_prompt = 'chosen system prompt' WHERE job_id = 'job'",
                [],
            )
            .unwrap();
        let admitted = store.admit_job([1; 32]).unwrap();
        assert_eq!(quota_state(&observer, "job"), ("exhausted".to_owned(), 1));
        observer
            .execute("UPDATE jobs SET expires_at = 2 WHERE job_id = 'job'", [])
            .unwrap();
        for transition in [JobTransition::Revoke, JobTransition::Done] {
            assert_eq!(
                store.transition_job("job", transition),
                Err(TransitionError::Conflict)
            );
        }
        assert_eq!(
            admitted,
            JobAdmission::Admitted(AdmittedJob {
                request_id: 1,
                upstream_key_id: "chosen-key".to_owned(),
                system_prompt: "chosen system prompt".to_owned(),
                reserved_tokens: 1_000,
            })
        );
        drop(store);

        let reopened = Store::open(&path).unwrap();
        assert_eq!(
            reopened.admit_job([1; 32]).unwrap(),
            JobAdmission::Exhausted
        );
        assert_eq!(quota_state(&observer, "job"), ("exhausted".to_owned(), 1));
    }

    #[test]
    fn failed_admission_rolls_back_the_charge_and_terminal_status() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("jobs.sqlite3");
        let store = Store::open(&path).unwrap();
        insert_job(&store, "job", 1, i64::MAX);
        let observer = Connection::open(&path).unwrap();
        observer
            .execute_batch(
                "UPDATE jobs SET max_requests = 1 WHERE job_id = 'job';
                 CREATE TRIGGER reject_charge AFTER UPDATE OF requests_used ON jobs
                 BEGIN SELECT RAISE(FAIL, 'charge rejected'); END;",
            )
            .unwrap();
        assert!(store.admit_job([1; 32]).is_err());
        assert_eq!(quota_state(&observer, "job"), ("active".to_owned(), 0));
        drop(store);

        let reopened = Store::open(&path).unwrap();
        assert_eq!(quota_state(&observer, "job"), ("active".to_owned(), 0));
        observer
            .execute_batch("DROP TRIGGER reject_charge;")
            .unwrap();
        assert!(matches!(
            reopened.admit_job([1; 32]).unwrap(),
            JobAdmission::Admitted(_)
        ));
        assert_eq!(quota_state(&observer, "job"), ("exhausted".to_owned(), 1));
    }

    #[test]
    fn signed_integer_maximum_is_a_safe_final_charge() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("jobs.sqlite3");
        let store = Store::open(&path).unwrap();
        insert_job(&store, "job", 1, i64::MAX);
        let observer = Connection::open(&path).unwrap();
        observer
            .execute(
                "UPDATE jobs SET max_requests = ?1, requests_used = ?2 WHERE job_id = 'job'",
                params![i64::MAX, i64::MAX - 1],
            )
            .unwrap();
        assert!(matches!(
            store.admit_job([1; 32]).unwrap(),
            JobAdmission::Admitted(_)
        ));
        assert_eq!(store.admit_job([1; 32]).unwrap(), JobAdmission::Exhausted);
        assert_eq!(
            quota_state(&observer, "job"),
            ("exhausted".to_owned(), i64::MAX)
        );
    }

    #[test]
    fn terminal_and_unknown_credentials_never_charge() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("jobs.sqlite3");
        let store = Store::open(&path).unwrap();
        let observer = Connection::open(&path).unwrap();
        for (token, status) in [(1, "revoked"), (2, "done"), (3, "expired")] {
            insert_job(&store, status, token, i64::MAX);
            observer
                .execute(
                    "UPDATE jobs SET status = ?1 WHERE job_id = ?1",
                    params![status],
                )
                .unwrap();
            assert_eq!(
                store.admit_job([token; 32]).unwrap(),
                JobAdmission::Unauthorized
            );
            assert_eq!(quota_state(&observer, status), (status.to_owned(), 0));
        }
        insert_job(&store, "overdue", 4, 2);
        assert_eq!(
            store.admit_job([4; 32]).unwrap(),
            JobAdmission::Unauthorized
        );
        assert_eq!(quota_state(&observer, "overdue"), ("expired".to_owned(), 0));
        assert_eq!(
            store.admit_job([9; 32]).unwrap(),
            JobAdmission::Unauthorized
        );
    }

    #[test]
    fn final_admission_and_lifecycle_have_exactly_one_terminal_winner() {
        use std::sync::Barrier;

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("jobs.sqlite3");
        let first = Store::open(&path).unwrap();
        let second = Store::open(&path).unwrap();
        let observer = Connection::open(&path).unwrap();
        for (token, transition, status) in [
            (1, JobTransition::Revoke, "revoked"),
            (2, JobTransition::Done, "done"),
        ] {
            insert_job(&first, status, token, i64::MAX);
            observer
                .execute(
                    "UPDATE jobs SET max_requests = 1 WHERE job_id = ?1",
                    params![status],
                )
                .unwrap();
            let barrier = Barrier::new(2);
            let (admission, lifecycle) = std::thread::scope(|scope| {
                let admission = scope.spawn(|| {
                    barrier.wait();
                    first.admit_job([token; 32]).unwrap()
                });
                let lifecycle = scope.spawn(|| {
                    barrier.wait();
                    second.transition_job(status, transition)
                });
                (admission.join().unwrap(), lifecycle.join().unwrap())
            });
            match (admission, lifecycle) {
                (JobAdmission::Admitted(_), Err(TransitionError::Conflict)) => {
                    assert_eq!(quota_state(&observer, status), ("exhausted".to_owned(), 1));
                }
                (JobAdmission::Unauthorized, Ok(())) => {
                    assert_eq!(quota_state(&observer, status), (status.to_owned(), 0));
                }
                results => panic!("expected exactly one terminal winner, got {results:?}"),
            }
        }
    }

    fn admitted_request(store: &Store, token: u8) -> i64 {
        match store.admit_job([token; 32]).unwrap() {
            JobAdmission::Admitted(job) => job.request_id,
            other => panic!("expected admission, got {other:?}"),
        }
    }

    fn token_state(connection: &Connection, job_id: &str) -> (i64, i64) {
        connection
            .query_row(
                "SELECT tokens_reserved, tokens_used FROM jobs WHERE job_id = ?1",
                params![job_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap()
    }

    fn budget_state(connection: &Connection, job_id: &str) -> (i64, i64, i64) {
        connection
            .query_row(
                "SELECT tokens_reserved, tokens_used, tokens_unreported FROM jobs WHERE job_id = ?1",
                params![job_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap()
    }

    #[derive(Debug, Eq, PartialEq)]
    struct RequestState {
        outcome: String,
        finished_at: Option<i64>,
        usage: Option<Usage>,
    }

    fn request_state(connection: &Connection, request_id: i64) -> RequestState {
        connection
            .query_row(
                "SELECT outcome, finished_at, prompt_tokens, completion_tokens, total_tokens
                 FROM request_records WHERE request_id = ?1",
                params![request_id],
                |row| {
                    let prompt_tokens: Option<i64> = row.get(2)?;
                    let completion_tokens: Option<i64> = row.get(3)?;
                    let total_tokens: Option<i64> = row.get(4)?;
                    assert_eq!(prompt_tokens.is_some(), completion_tokens.is_some());
                    assert_eq!(prompt_tokens.is_some(), total_tokens.is_some());
                    Ok(RequestState {
                        outcome: row.get(0)?,
                        finished_at: row.get(1)?,
                        usage: prompt_tokens.map(|prompt_tokens| Usage {
                            prompt_tokens,
                            completion_tokens: completion_tokens.unwrap(),
                            total_tokens: total_tokens.unwrap(),
                        }),
                    })
                },
            )
            .unwrap()
    }

    #[test]
    fn request_records_distinguish_unknown_zero_and_known_usage() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("jobs.sqlite3");
        let store = Store::open(&path).unwrap();
        insert_job(&store, "job", 1, i64::MAX);
        let observer = Connection::open(&path).unwrap();
        let zero = Usage {
            prompt_tokens: 0,
            completion_tokens: 0,
            total_tokens: 0,
        };
        let known = Usage {
            prompt_tokens: 7,
            completion_tokens: 11,
            total_tokens: 18,
        };
        for (index, usage) in [None, Some(zero), Some(known)].into_iter().enumerate() {
            let request_id = admitted_request(&store, 1);
            store.constrain_reservation(request_id, 20).unwrap();
            assert_eq!(
                request_state(&observer, request_id),
                RequestState {
                    outcome: "pending".to_owned(),
                    finished_at: None,
                    usage: None
                }
            );
            let (admitted_at, reserved): (i64, i64) = observer
                .query_row(
                    "SELECT admitted_at, reserved_tokens FROM request_records WHERE request_id = ?1",
                    params![request_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap();
            assert!(admitted_at > 0);
            assert_eq!(reserved, 20);
            store
                .finish_request(request_id, "completed", usage)
                .unwrap();
            let snapshot = request_state(&observer, request_id);
            assert_eq!(snapshot.outcome, "completed");
            assert!(
                snapshot
                    .finished_at
                    .is_some_and(|finished| finished >= admitted_at)
            );
            assert_eq!(snapshot.usage, usage);
            store
                .finish_request(request_id, "cancelled", Some(known))
                .unwrap();
            assert_eq!(request_state(&observer, request_id), snapshot);
            assert_eq!(
                token_state(&observer, "job"),
                (0, if index == 2 { 18 } else { 0 })
            );
            assert_eq!(budget_state(&observer, "job").2, 20);
        }
        assert_eq!(quota_state(&observer, "job"), ("active".to_owned(), 3));
        drop(store);
        drop(Store::open(&path).unwrap());
        assert_eq!(token_state(&observer, "job"), (0, 18));
    }

    #[test]
    fn completion_releases_reservations_without_changing_terminal_jobs() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("jobs.sqlite3");
        let store = Store::open(&path).unwrap();
        let observer = Connection::open(&path).unwrap();
        for (token, status, transition) in [
            (1, "done", Some(JobTransition::Done)),
            (2, "revoked", Some(JobTransition::Revoke)),
            (3, "exhausted", None),
            (4, "expired", None),
        ] {
            insert_job(&store, status, token, i64::MAX);
            if status == "exhausted" {
                observer
                    .execute(
                        "UPDATE jobs SET max_requests = 1 WHERE job_id = ?1",
                        params![status],
                    )
                    .unwrap();
            }
            let request_id = admitted_request(&store, token);
            assert_eq!(token_state(&observer, status), (1_000, 0));
            if let Some(transition) = transition {
                store.transition_job(status, transition).unwrap();
            }
            if status == "expired" {
                observer
                    .execute(
                        "UPDATE jobs SET expires_at = 2 WHERE job_id = ?1",
                        params![status],
                    )
                    .unwrap();
                assert_eq!(
                    store.admit_job([token; 32]).unwrap(),
                    JobAdmission::Unauthorized
                );
            }
            assert_eq!(token_state(&observer, status), (1_000, 0));
            store.constrain_reservation(request_id, 25).unwrap();
            let snapshot = job_state(&observer, status);
            let usage = Usage {
                prompt_tokens: 500,
                completion_tokens: 700,
                total_tokens: 1_200,
            };
            store
                .finish_request(request_id, "completed", Some(usage))
                .unwrap();
            store
                .finish_request(request_id, "completed", Some(usage))
                .unwrap();
            assert_eq!(token_state(&observer, status), (0, 1_200));
            assert_eq!(job_state(&observer, status), snapshot);
            assert_eq!(quota_state(&observer, status).1, 1);
        }
    }

    #[test]
    fn independent_connections_complete_a_request_exactly_once() {
        use std::sync::Barrier;

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("jobs.sqlite3");
        let first = Store::open(&path).unwrap();
        // Open both before admitting: Store::open performs startup recovery.
        let second = Store::open(&path).unwrap();
        insert_job(&first, "job", 1, i64::MAX);
        let request_id = admitted_request(&first, 1);
        let observer = Connection::open(&path).unwrap();
        first.constrain_reservation(request_id, 10).unwrap();
        let barrier = Barrier::new(2);
        let usage = Usage {
            prompt_tokens: 2,
            completion_tokens: 3,
            total_tokens: 5,
        };
        std::thread::scope(|scope| {
            let left = scope.spawn(|| {
                barrier.wait();
                first.finish_request(request_id, "completed", Some(usage))
            });
            let right = scope.spawn(|| {
                barrier.wait();
                second.finish_request(request_id, "timeout", None)
            });
            left.join().unwrap().unwrap();
            right.join().unwrap().unwrap();
        });
        let state = request_state(&observer, request_id);
        assert!(state.finished_at.is_some());
        match state.outcome.as_str() {
            "completed" => {
                assert_eq!(state.usage, Some(usage));
                assert_eq!(token_state(&observer, "job"), (0, 5));
                assert_eq!(budget_state(&observer, "job").2, 0);
            }
            "timeout" => {
                assert_eq!(state.usage, None);
                assert_eq!(token_state(&observer, "job"), (0, 0));
                assert_eq!(budget_state(&observer, "job").2, 10);
            }
            other => panic!("unexpected outcome {other}"),
        }
    }

    #[test]
    fn startup_recovers_pending_requests_as_unknown_exactly_once() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("jobs.sqlite3");
        let store = Store::open(&path).unwrap();
        insert_job(&store, "job", 1, i64::MAX);
        let completed = admitted_request(&store, 1);
        let zero = Usage {
            prompt_tokens: 0,
            completion_tokens: 0,
            total_tokens: 0,
        };
        store
            .finish_request(completed, "completed", Some(zero))
            .unwrap();
        let first = admitted_request(&store, 1);
        store.constrain_reservation(first, 10).unwrap();
        let second = admitted_request(&store, 1);
        let observer = Connection::open(&path).unwrap();
        store.constrain_reservation(second, 20).unwrap();
        store.transition_job("job", JobTransition::Done).unwrap();
        drop(store);

        let reopened = Store::open(&path).unwrap();
        let snapshots: Vec<_> = [first, second]
            .into_iter()
            .map(|id| {
                let state = request_state(&observer, id);
                assert_eq!(state.outcome, "interrupted");
                assert!(state.finished_at.is_some());
                assert_eq!(state.usage, None);
                state
            })
            .collect();
        assert_eq!(budget_state(&observer, "job"), (0, 0, 30));
        assert_eq!(quota_state(&observer, "job"), ("done".to_owned(), 3));
        assert_eq!(request_state(&observer, completed).usage, Some(zero));
        reopened
            .finish_request(first, "completed", Some(zero))
            .unwrap();
        drop(reopened);
        drop(Store::open(&path).unwrap());
        for (id, snapshot) in [first, second].into_iter().zip(snapshots) {
            assert_eq!(request_state(&observer, id), snapshot);
        }
        assert_eq!(budget_state(&observer, "job"), (0, 0, 30));
    }

    #[test]
    fn invalid_usage_and_overflow_leave_accounting_pending() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("jobs.sqlite3");
        let store = Store::open(&path).unwrap();
        insert_job(&store, "job", 1, i64::MAX);
        let request_id = admitted_request(&store, 1);
        let observer = Connection::open(&path).unwrap();
        for usage in [
            Usage {
                prompt_tokens: -1,
                completion_tokens: 2,
                total_tokens: 1,
            },
            Usage {
                prompt_tokens: 1,
                completion_tokens: -1,
                total_tokens: 0,
            },
            Usage {
                prompt_tokens: 1,
                completion_tokens: 1,
                total_tokens: -1,
            },
            Usage {
                prompt_tokens: 1,
                completion_tokens: 1,
                total_tokens: 3,
            },
            Usage {
                prompt_tokens: i64::MAX,
                completion_tokens: 1,
                total_tokens: i64::MAX,
            },
        ] {
            assert!(
                store
                    .finish_request(request_id, "completed", Some(usage))
                    .is_err()
            );
        }
        assert!(store.finish_request(request_id, "pending", None).is_err());
        assert!(store.finish_request(request_id, "invented", None).is_err());
        observer
            .execute(
                "UPDATE jobs SET tokens_reserved = 10, tokens_used = ?1 WHERE job_id = 'job'",
                params![i64::MAX],
            )
            .unwrap();
        observer
            .execute(
                "UPDATE request_records SET reserved_tokens = 10 WHERE request_id = ?1",
                params![request_id],
            )
            .unwrap();
        let one = Usage {
            prompt_tokens: 1,
            completion_tokens: 0,
            total_tokens: 1,
        };
        assert!(
            store
                .finish_request(request_id, "completed", Some(one))
                .is_err()
        );
        assert_eq!(token_state(&observer, "job"), (10, i64::MAX));
        assert_eq!(
            request_state(&observer, request_id),
            RequestState {
                outcome: "pending".to_owned(),
                finished_at: None,
                usage: None
            }
        );
        let kind: String = observer
            .query_row("SELECT typeof(tokens_used) FROM jobs", [], |row| row.get(0))
            .unwrap();
        assert_eq!(kind, "integer");
        store
            .finish_request(request_id, "upstream_error", None)
            .unwrap();
        assert_eq!(token_state(&observer, "job"), (0, i64::MAX));
    }

    #[test]
    fn accounting_write_failures_roll_back_records_and_job_counters() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("jobs.sqlite3");
        let store = Store::open(&path).unwrap();
        insert_job(&store, "job", 1, i64::MAX);
        let observer = Connection::open(&path).unwrap();
        observer
            .execute_batch(
                "CREATE TRIGGER reject_request BEFORE INSERT ON request_records
             BEGIN SELECT RAISE(FAIL, 'record rejected'); END;",
            )
            .unwrap();
        assert!(store.admit_job([1; 32]).is_err());
        assert_eq!(quota_state(&observer, "job"), ("active".to_owned(), 0));
        let count: i64 = observer
            .query_row("SELECT count(*) FROM request_records", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 0);
        observer
            .execute_batch("DROP TRIGGER reject_request;")
            .unwrap();
        let request_id = admitted_request(&store, 1);
        store.constrain_reservation(request_id, 10).unwrap();
        observer
            .execute_batch(
                "CREATE TRIGGER reject_accounting BEFORE UPDATE OF tokens_used ON jobs
             BEGIN SELECT RAISE(FAIL, 'accounting rejected'); END;",
            )
            .unwrap();
        let usage = Usage {
            prompt_tokens: 2,
            completion_tokens: 3,
            total_tokens: 5,
        };
        assert!(
            store
                .finish_request(request_id, "completed", Some(usage))
                .is_err()
        );
        assert_eq!(token_state(&observer, "job"), (10, 0));
        assert_eq!(request_state(&observer, request_id).outcome, "pending");
        assert_eq!(request_state(&observer, request_id).finished_at, None);
        observer
            .execute_batch("DROP TRIGGER reject_accounting;")
            .unwrap();
        store
            .finish_request(request_id, "completed", Some(usage))
            .unwrap();
        assert_eq!(token_state(&observer, "job"), (0, 5));
    }

    #[test]
    fn all_terminal_outcomes_are_durable_and_denials_create_no_records() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("jobs.sqlite3");
        let store = Store::open(&path).unwrap();
        insert_job(&store, "job", 1, i64::MAX);
        let observer = Connection::open(&path).unwrap();
        for outcome in [
            "completed",
            "rejected",
            "upstream_error",
            "timeout",
            "disconnected",
            "cancelled",
        ] {
            let request_id = admitted_request(&store, 1);
            store.constrain_reservation(request_id, 10).unwrap();
            store.finish_request(request_id, outcome, None).unwrap();
            assert_eq!(request_state(&observer, request_id).outcome, outcome);
        }
        observer
            .execute(
                "UPDATE jobs SET max_requests = 6, status = 'exhausted' WHERE job_id = 'job'",
                [],
            )
            .unwrap();
        assert_eq!(store.admit_job([1; 32]).unwrap(), JobAdmission::Exhausted);
        assert_eq!(
            store.admit_job([9; 32]).unwrap(),
            JobAdmission::Unauthorized
        );
        insert_job(&store, "overdue", 2, 2);
        assert_eq!(
            store.admit_job([2; 32]).unwrap(),
            JobAdmission::Unauthorized
        );
        insert_job(&store, "revoked", 3, i64::MAX);
        store
            .transition_job("revoked", JobTransition::Revoke)
            .unwrap();
        assert_eq!(
            store.admit_job([3; 32]).unwrap(),
            JobAdmission::Unauthorized
        );
        let count: i64 = observer
            .query_row("SELECT count(*) FROM request_records", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 6);
        assert_eq!(quota_state(&observer, "job"), ("exhausted".to_owned(), 6));
        assert_eq!(token_state(&observer, "job"), (0, 0));
        assert_eq!(budget_state(&observer, "job").2, 60);
    }

    fn create_legacy_database(connection: &Connection) {
        connection.execute_batch(
            "CREATE TABLE jobs (
                job_id TEXT NOT NULL PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                upstream_key_id TEXT NOT NULL,
                system_prompt TEXT NOT NULL,
                token_hash BLOB NOT NULL UNIQUE CHECK(typeof(token_hash) = 'blob' AND length(token_hash) = 32),
                created_at INTEGER NOT NULL,
                expires_at INTEGER NOT NULL CHECK(expires_at > created_at),
                max_requests INTEGER NOT NULL CHECK(max_requests > 0),
                requests_used INTEGER NOT NULL DEFAULT 0 CHECK(requests_used >= 0),
                max_tokens_total INTEGER NOT NULL CHECK(max_tokens_total > 0),
                tokens_used INTEGER NOT NULL DEFAULT 0 CHECK(tokens_used >= 0),
                status TEXT NOT NULL DEFAULT 'active'
                    CHECK(status IN ('active', 'expired', 'revoked', 'exhausted', 'done')),
                revoked_at INTEGER,
                revoked_reason TEXT
             );
             PRAGMA user_version = 1;",
        ).unwrap();
        connection
            .execute(
                "INSERT INTO jobs VALUES ('legacy', 'tenant', 'key', 'prompt', ?1,
             1, ?2, 10, 3, 1000, 57, 'revoked', 12, 'legacy reason')",
                params![[1_u8; 32].as_slice(), i64::MAX],
            )
            .unwrap();
    }

    #[test]
    fn version_one_migration_preserves_all_legacy_job_data() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("jobs.sqlite3");
        let observer = Connection::open(&path).unwrap();
        create_legacy_database(&observer);
        let legacy_columns = "job_id, tenant_id, upstream_key_id, system_prompt, token_hash,
                              created_at, expires_at, max_requests, requests_used,
                              max_tokens_total, tokens_used, status, revoked_at, revoked_reason";
        let snapshot = || {
            let mut statement = observer
                .prepare(&format!("SELECT {legacy_columns} FROM jobs"))
                .unwrap();
            statement
                .query_row([], |row| {
                    (0..14)
                        .map(|index| row.get::<_, rusqlite::types::Value>(index))
                        .collect::<rusqlite::Result<Vec<_>>>()
                })
                .unwrap()
        };
        let before = snapshot();
        let store = Store::open(&path).unwrap();
        assert_eq!(snapshot(), before);
        assert_eq!(token_state(&observer, "legacy"), (0, 57));
        assert_eq!(
            observer
                .pragma_query_value(None, "user_version", |row| row.get::<_, i64>(0))
                .unwrap(),
            SCHEMA_VERSION
        );
        let count: i64 = observer
            .query_row("SELECT count(*) FROM request_records", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 0);
        assert_eq!(
            store.admit_job([1; 32]).unwrap(),
            JobAdmission::Unauthorized
        );
        drop(store);
        drop(Store::open(&path).unwrap());
        assert_eq!(snapshot(), before);
    }

    #[test]
    fn failed_version_two_migration_rolls_back_the_added_column() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("jobs.sqlite3");
        let observer = Connection::open(&path).unwrap();
        create_legacy_database(&observer);
        observer
            .execute_batch("CREATE TABLE request_records (existing TEXT);")
            .unwrap();
        assert!(Store::open(&path).is_err());
        assert_eq!(
            observer
                .pragma_query_value(None, "user_version", |row| row.get::<_, i64>(0))
                .unwrap(),
            1
        );
        assert!(
            observer
                .prepare("SELECT tokens_reserved FROM jobs")
                .is_err()
        );
        assert!(
            observer
                .prepare("SELECT existing FROM request_records")
                .is_ok()
        );
        assert_eq!(quota_state(&observer, "legacy"), ("revoked".to_owned(), 3));
    }

    #[test]
    fn migrations_survive_reopen_and_reject_newer_versions() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("jobs.sqlite3");
        drop(Store::open(&path).unwrap());
        let connection = Connection::open(&path).unwrap();
        assert_eq!(
            connection
                .pragma_query_value(None, "user_version", |row| row.get::<_, i64>(0))
                .unwrap(),
            SCHEMA_VERSION
        );
        drop(Store::open(&path).unwrap());
        connection
            .pragma_update(None, "user_version", SCHEMA_VERSION + 1)
            .unwrap();
        assert!(Store::open(&path).is_err());
        assert_eq!(
            connection
                .pragma_query_value(None, "user_version", |row| row.get::<_, i64>(0))
                .unwrap(),
            SCHEMA_VERSION + 1
        );
    }

    #[test]
    fn migrations_are_transactional() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("jobs.sqlite3");
        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch("CREATE TABLE jobs (existing TEXT);")
            .unwrap();
        assert!(Store::open(&path).is_err());
        assert_eq!(
            connection
                .pragma_query_value(None, "user_version", |row| row.get::<_, i64>(0))
                .unwrap(),
            0
        );
        assert!(connection.prepare("SELECT existing FROM jobs").is_ok());
    }

    #[test]
    fn transient_databases_are_rejected() {
        for path in ["", ":memory:", "file::memory:", "file:jobs?mode=memory"] {
            assert!(Store::open(Path::new(path)).is_err());
        }
    }

    #[cfg(unix)]
    #[test]
    fn new_database_has_private_permissions() {
        use std::os::unix::fs::PermissionsExt;
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("jobs.sqlite3");
        drop(Store::open(&path).unwrap());
        assert_eq!(
            std::fs::metadata(path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }

    #[test]
    fn startup_recovery_failure_rolls_back_every_pending_request() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("jobs.sqlite3");
        let store = Store::open(&path).unwrap();
        insert_job(&store, "first", 1, i64::MAX);
        insert_job(&store, "second", 2, i64::MAX);
        let first = admitted_request(&store, 1);
        let second = admitted_request(&store, 2);
        let observer = Connection::open(&path).unwrap();
        observer
            .execute_batch(
                "UPDATE jobs SET tokens_reserved = 10;
             UPDATE request_records SET reserved_tokens = 10;",
            )
            .unwrap();
        observer
            .execute(
                "UPDATE request_records SET reserved_tokens = 20 WHERE request_id = ?1",
                params![second],
            )
            .unwrap();
        drop(store);
        assert!(Store::open(&path).is_err());
        for (request_id, job_id) in [(first, "first"), (second, "second")] {
            assert_eq!(token_state(&observer, job_id), (10, 0));
            assert_eq!(budget_state(&observer, job_id).2, 0);
            assert_eq!(
                request_state(&observer, request_id),
                RequestState {
                    outcome: "pending".to_owned(),
                    finished_at: None,
                    usage: None
                }
            );
        }
        observer
            .execute(
                "UPDATE jobs SET tokens_reserved = 20 WHERE job_id = 'second'",
                [],
            )
            .unwrap();
        drop(Store::open(&path).unwrap());
        for (request_id, job_id) in [(first, "first"), (second, "second")] {
            assert_eq!(token_state(&observer, job_id), (0, 0));
            assert_eq!(request_state(&observer, request_id).outcome, "interrupted");
        }
    }

    #[test]
    fn independent_connections_atomically_escrow_all_available_tokens() {
        use std::sync::Barrier;

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("jobs.sqlite3");
        let store = Store::open(&path).unwrap();
        let contenders: Vec<_> = (0..8).map(|_| Store::open(&path).unwrap()).collect();
        insert_job(&store, "job", 1, i64::MAX);
        let observer = Connection::open(&path).unwrap();
        let barrier = Barrier::new(contenders.len());
        let results = std::thread::scope(|scope| {
            let workers: Vec<_> = contenders
                .iter()
                .map(|contender| {
                    let barrier = &barrier;
                    scope.spawn(move || {
                        barrier.wait();
                        contender.admit_job([1; 32]).unwrap()
                    })
                })
                .collect();
            workers
                .into_iter()
                .map(|worker| worker.join().unwrap())
                .collect::<Vec<_>>()
        });
        let admitted: Vec<_> = results
            .iter()
            .filter_map(|result| match result {
                JobAdmission::Admitted(job) => Some(job),
                other => {
                    assert_eq!(*other, JobAdmission::TokenLimited);
                    None
                }
            })
            .collect();
        assert_eq!(admitted.len(), 1);
        assert_eq!(admitted[0].reserved_tokens, 1_000);
        assert_eq!(budget_state(&observer, "job"), (1_000, 0, 0));
        assert_eq!(quota_state(&observer, "job"), ("active".to_owned(), 1));
        assert_eq!(
            observer
                .query_row(
                    "SELECT count(*), sum(reserved_tokens) FROM request_records",
                    [],
                    |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
                )
                .unwrap(),
            (1, 1_000)
        );

        store
            .constrain_reservation(admitted[0].request_id, 400)
            .unwrap();
        let JobAdmission::Admitted(second) = contenders[0].admit_job([1; 32]).unwrap() else {
            panic!("narrowing must make the remaining budget available");
        };
        assert_eq!(second.reserved_tokens, 600);
        assert_eq!(budget_state(&observer, "job"), (1_000, 0, 0));
        assert_eq!(quota_state(&observer, "job"), ("active".to_owned(), 2));
    }

    #[test]
    fn reservation_constraints_are_shrink_only_and_release_exactly_once() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("jobs.sqlite3");
        let store = Store::open(&path).unwrap();
        insert_job(&store, "job", 1, i64::MAX);
        let observer = Connection::open(&path).unwrap();
        let request = admitted_request(&store, 1);
        store.constrain_reservation(request, 300).unwrap();
        store.constrain_reservation(request, 300).unwrap();
        assert!(store.constrain_reservation(request, 301).is_err());
        assert!(store.constrain_reservation(request, -1).is_err());
        assert!(store.constrain_reservation(i64::MAX, 0).is_err());
        assert_eq!(budget_state(&observer, "job"), (300, 0, 0));
        let second = admitted_request(&store, 1);
        store.constrain_reservation(second, 200).unwrap();
        store.constrain_reservation(request, 100).unwrap();
        assert_eq!(budget_state(&observer, "job"), (300, 0, 0));
        let zero = Usage {
            prompt_tokens: 0,
            completion_tokens: 0,
            total_tokens: 0,
        };
        store
            .finish_request(request, "rejected", Some(zero))
            .unwrap();
        store.finish_request(request, "timeout", None).unwrap();
        assert!(store.constrain_reservation(request, 0).is_err());
        assert_eq!(budget_state(&observer, "job"), (200, 0, 0));
        store.finish_request(second, "timeout", None).unwrap();
        store
            .finish_request(second, "completed", Some(zero))
            .unwrap();
        assert_eq!(budget_state(&observer, "job"), (0, 0, 200));
        assert_eq!(request_state(&observer, second).usage, None);
        assert_eq!(quota_state(&observer, "job"), ("active".to_owned(), 2));
        let JobAdmission::Admitted(third) = store.admit_job([1; 32]).unwrap() else {
            panic!("known zero must refund escrow without refunding request charges");
        };
        assert_eq!(third.reserved_tokens, 800);
    }

    #[test]
    fn known_and_unknown_spending_exhaust_the_budget_without_fabricating_usage() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("jobs.sqlite3");
        let store = Store::open(&path).unwrap();
        insert_job(&store, "job", 1, i64::MAX);
        let observer = Connection::open(&path).unwrap();
        let first = admitted_request(&store, 1);
        store.constrain_reservation(first, 300).unwrap();
        store.finish_request(first, "timeout", None).unwrap();
        let second = admitted_request(&store, 1);
        let usage = Usage {
            prompt_tokens: 200,
            completion_tokens: 500,
            total_tokens: 700,
        };
        store
            .finish_request(second, "completed", Some(usage))
            .unwrap();
        assert_eq!(budget_state(&observer, "job"), (0, 700, 300));
        assert_eq!(request_state(&observer, first).usage, None);
        assert_eq!(request_state(&observer, second).usage, Some(usage));
        assert_eq!(quota_state(&observer, "job"), ("exhausted".to_owned(), 2));
        assert_eq!(store.admit_job([1; 32]).unwrap(), JobAdmission::Exhausted);
        drop(store);
        let reopened = Store::open(&path).unwrap();
        assert_eq!(budget_state(&observer, "job"), (0, 700, 300));
        assert_eq!(
            reopened.admit_job([1; 32]).unwrap(),
            JobAdmission::Exhausted
        );
    }

    #[test]
    fn reported_usage_over_the_reservation_is_recorded_truthfully_and_stops_admission() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("jobs.sqlite3");
        let store = Store::open(&path).unwrap();
        insert_job(&store, "job", 1, i64::MAX);
        let observer = Connection::open(&path).unwrap();
        let first = admitted_request(&store, 1);
        store.constrain_reservation(first, 10).unwrap();
        let usage = Usage {
            prompt_tokens: 1_000,
            completion_tokens: 50,
            total_tokens: 1_050,
        };
        store
            .finish_request(first, "completed", Some(usage))
            .unwrap();
        assert_eq!(budget_state(&observer, "job"), (0, 1_050, 0));
        assert_eq!(request_state(&observer, first).usage, Some(usage));
        assert_eq!(store.admit_job([1; 32]).unwrap(), JobAdmission::Exhausted);
        assert_eq!(quota_state(&observer, "job"), ("exhausted".to_owned(), 1));
    }

    #[test]
    fn maximum_integer_budget_never_promotes_accounting_to_sqlite_real() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("jobs.sqlite3");
        let store = Store::open(&path).unwrap();
        insert_job(&store, "job", 1, i64::MAX);
        let observer = Connection::open(&path).unwrap();
        observer
            .execute(
                "UPDATE jobs SET max_tokens_total = ?1, tokens_used = ?2, tokens_unreported = 1",
                params![i64::MAX, i64::MAX - 3],
            )
            .unwrap();
        let JobAdmission::Admitted(request) = store.admit_job([1; 32]).unwrap() else {
            panic!("the last two tokens must remain usable");
        };
        assert_eq!(request.reserved_tokens, 2);
        assert_eq!(
            store.admit_job([1; 32]).unwrap(),
            JobAdmission::TokenLimited
        );
        store
            .finish_request(request.request_id, "timeout", None)
            .unwrap();
        assert_eq!(budget_state(&observer, "job"), (0, i64::MAX - 3, 3));
        assert_eq!(quota_state(&observer, "job"), ("exhausted".to_owned(), 1));
        let kinds: (String, String, String) = observer.query_row(
            "SELECT typeof(tokens_used), typeof(tokens_unreported), typeof(tokens_reserved) FROM jobs",
            [], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        ).unwrap();
        assert_eq!(
            kinds,
            (
                "integer".to_owned(),
                "integer".to_owned(),
                "integer".to_owned()
            )
        );
    }

    #[test]
    fn version_two_migration_preserves_legacy_unknown_zero_reservations() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("jobs.sqlite3");
        let store = Store::open(&path).unwrap();
        insert_job(&store, "job", 1, i64::MAX);
        let known = admitted_request(&store, 1);
        let usage = Usage {
            prompt_tokens: 2,
            completion_tokens: 3,
            total_tokens: 5,
        };
        store
            .finish_request(known, "completed", Some(usage))
            .unwrap();
        let unknown = admitted_request(&store, 1);
        store.constrain_reservation(unknown, 0).unwrap();
        store.finish_request(unknown, "timeout", None).unwrap();
        let pending = admitted_request(&store, 1);
        store.constrain_reservation(pending, 0).unwrap();
        drop(store);
        let observer = Connection::open(&path).unwrap();
        observer
            .execute_batch(
                "ALTER TABLE jobs DROP COLUMN tokens_unreported; PRAGMA user_version = 2;",
            )
            .unwrap();
        let reopened = Store::open(&path).unwrap();
        assert_eq!(budget_state(&observer, "job"), (0, 5, 0));
        assert_eq!(request_state(&observer, known).usage, Some(usage));
        assert_eq!(request_state(&observer, unknown).usage, None);
        assert_eq!(request_state(&observer, pending).usage, None);
        assert_eq!(request_state(&observer, pending).outcome, "interrupted");
        assert_eq!(quota_state(&observer, "job"), ("active".to_owned(), 3));
        let JobAdmission::Admitted(request) = reopened.admit_job([1; 32]).unwrap() else {
            panic!("legacy unknown records cannot be retroactively estimated");
        };
        assert_eq!(request.reserved_tokens, 995);
    }

    #[test]
    fn failed_version_three_recovery_rolls_back_the_schema_and_unknown_spending() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("jobs.sqlite3");
        let store = Store::open(&path).unwrap();
        insert_job(&store, "job", 1, i64::MAX);
        let request = admitted_request(&store, 1);
        drop(store);
        let observer = Connection::open(&path).unwrap();
        observer
            .execute_batch(
                "UPDATE jobs SET tokens_reserved = 0;
             ALTER TABLE jobs DROP COLUMN tokens_unreported;
             PRAGMA user_version = 2;",
            )
            .unwrap();
        assert!(Store::open(&path).is_err());
        assert_eq!(
            observer
                .pragma_query_value(None, "user_version", |row| row.get::<_, i64>(0))
                .unwrap(),
            2
        );
        assert!(
            observer
                .prepare("SELECT tokens_unreported FROM jobs")
                .is_err()
        );
        assert_eq!(request_state(&observer, request).outcome, "pending");
        observer
            .execute("UPDATE jobs SET tokens_reserved = 1000", [])
            .unwrap();
        drop(Store::open(&path).unwrap());
        assert_eq!(budget_state(&observer, "job"), (0, 0, 1_000));
        assert_eq!(request_state(&observer, request).outcome, "interrupted");
    }
}
