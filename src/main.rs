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
    let store = tokio::task::spawn_blocking(move || Store::open(&database_path))
        .await
        .context("database startup task failed")??;
    let listener = TcpListener::bind(config.listen_addr)
        .await
        .context("could not bind service listener")?;
    eprintln!("rustgate listening on https://{}", listener.local_addr()?);
    runtime::serve_tls(listener, service::router(config, store), tls)
        .await
        .context("service failed")
}
