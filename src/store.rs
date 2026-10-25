use std::{
    fs::OpenOptions,
    path::Path,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use rusqlite::{Connection, OpenFlags, OptionalExtension, TransactionBehavior, params};

const SCHEMA_VERSION: i64 = 1;

#[derive(Clone)]
pub struct Store {
    connection: Arc<Mutex<Connection>>,
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
    pub upstream_key_id: String,
    pub system_prompt: String,
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
    /// Opens a persistent database, migrates its schema, and expires overdue jobs.
    /// Call from startup or a blocking worker, never a Tokio executor thread.
    pub fn open(path: &Path) -> Result<Self> {
        if path.as_os_str().is_empty()
            || path == Path::new(":memory:")
            || path.to_string_lossy().starts_with("file:")
        {
            bail!("database_path must identify a persistent filesystem database");
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
        transaction
            .execute(
                "UPDATE jobs SET status = 'expired'
                 WHERE status = 'active' AND expires_at <= ?1",
                params![epoch_seconds()?],
            )
            .context("could not expire jobs at startup")?;
        transaction
            .commit()
            .context("could not commit schema migration")?;
        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
        })
    }

    /// A single INSERT makes duplicate detection atomic across connections.
    pub(crate) fn insert_job(&self, job: NewJob) -> Result<(), InsertError> {
        let connection = self.connection.lock().map_err(|_| InsertError::Storage)?;
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

    /// Commits admission on a blocking worker, materializing expiry before lookup.
    /// Admitted requests do not recheck state while their upstream call is running.
    pub(crate) fn admit_job(&self, token_hash: [u8; 32]) -> Result<Option<AdmittedJob>> {
        let mut connection = self
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

    /// Materializes expiry before applying a terminal transition. The first
    /// terminal state wins, including across independently opened connections.
    pub(crate) fn transition_job(
        &self,
        job_id: &str,
        transition: JobTransition,
    ) -> Result<(), TransitionError> {
        let mut connection = self
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

fn epoch_seconds() -> Result<i64> {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before the Unix epoch")?
        .as_secs();
    i64::try_from(seconds).context("system clock exceeds the supported range")
}

fn admit_job_in_transaction(
    transaction: &rusqlite::Transaction<'_>,
    token_hash: [u8; 32],
    now: i64,
) -> Result<Option<AdmittedJob>> {
    transaction
        .execute(
            "UPDATE jobs SET status = 'expired'
             WHERE token_hash = ?1 AND status = 'active' AND expires_at <= ?2",
            params![token_hash.as_slice(), now],
        )
        .context("could not expire job during admission")?;
    transaction
        .query_row(
            "SELECT upstream_key_id, system_prompt FROM jobs
             WHERE token_hash = ?1 AND status = 'active' AND expires_at > ?2",
            params![token_hash.as_slice(), now],
            |row| {
                Ok(AdmittedJob {
                    upstream_key_id: row.get(0)?,
                    system_prompt: row.get(1)?,
                })
            },
        )
        .optional()
        .context("could not look up job")
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

    fn job_state(connection: &Connection, job_id: &str) -> (String, Option<i64>, Option<String>) {
        connection
            .query_row(
                "SELECT status, revoked_at, revoked_reason FROM jobs WHERE job_id = ?1",
                params![job_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
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

        let mut connection = store.connection.lock().unwrap();
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        assert_eq!(
            admit_job_in_transaction(&transaction, [1; 32], 1_000).unwrap(),
            None
        );
        assert_eq!(
            admit_job_in_transaction(&transaction, [2; 32], 1_000).unwrap(),
            None
        );
        assert_eq!(
            admit_job_in_transaction(&transaction, [3; 32], 1_000).unwrap(),
            Some(AdmittedJob {
                upstream_key_id: "upstream".to_owned(),
                system_prompt: "system".to_owned(),
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

        assert_eq!(store.admit_job([1; 32]).unwrap(), None);
        assert_eq!(store.admit_job([9; 32]).unwrap(), None);
        assert_eq!(
            store.admit_job([3; 32]).unwrap(),
            Some(AdmittedJob {
                upstream_key_id: "upstream".to_owned(),
                system_prompt: "system".to_owned(),
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
        assert_eq!(reopened.admit_job([1; 32]).unwrap(), None);
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
            assert_eq!(store.admit_job([token; 32]).unwrap(), None);
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
            assert_eq!(first.admit_job([token; 32]).unwrap(), None);
            assert_eq!(second.transition_job(job_id, winner), Ok(()));
        }
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
}
