use anyhow::Context;
use axum_server::tls_rustls::RustlsConfig;
use rustgate::{config::Config, runtime, service, store::Store};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::load()?;
    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|_| anyhow::anyhow!("could not initialize TLS cryptography"))?;
    let tls = RustlsConfig::from_pem_file(&config.tls_cert_path, &config.tls_key_path)
        .await
        .map_err(|_| anyhow::anyhow!("could not load TLS certificate and private key"))?;
    let database_path = config.database_path.clone();
    let (store, _database_lock) = tokio::task::spawn_blocking(move || {
        // Recovery may only interrupt requests from a dead service. Hold an OS
        // lock on the database inode until shutdown; a crash releases it too.
        let mut options = std::fs::OpenOptions::new();
        options.read(true).write(true).create(true).truncate(false);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let lock = options
            .open(&database_path)
            .context("could not open database ownership lock")?;
        lock.try_lock()
            .context("database is already owned or filesystem locking is unavailable")?;
        let store = Store::open(&database_path)?;
        Ok::<_, anyhow::Error>((store, lock))
    })
    .await
    .context("database startup task failed")??;
    let listener = TcpListener::bind(config.listen_addr)
        .await
        .context("could not bind service listener")?;
    eprintln!("rustgate listening on https://{}", listener.local_addr()?);
    runtime::serve_tls(listener, service::router(config, store)?, tls)
        .await
        .context("service failed")
}
