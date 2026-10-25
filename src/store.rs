use std::{
    fs::OpenOptions,
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use rusqlite::{Connection, OpenFlags, TransactionBehavior, params};

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

pub(crate) enum InsertError {
    DuplicateJob,
    Storage,
}

impl Store {
    /// Opens a persistent database and applies all pending schema migrations.
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
}

#[cfg(test)]
mod tests {
    use super::*;

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
