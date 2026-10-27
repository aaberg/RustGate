use anyhow::Context;
use axum_server::tls_rustls::RustlsConfig;
use rustgate::{config::Config, runtime, service, store::Store};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> std::process::ExitCode {
    if std::env::args_os()
        .nth(1)
        .is_some_and(|arg| arg == "--version" || arg == "-V")
    {
        println!("rustgate {}", env!("CARGO_PKG_VERSION"));
        return std::process::ExitCode::SUCCESS;
    }

    match run().await {
        Ok(()) => {
            eprintln!("{}", serde_json::json!({"event":"stopped"}));
            std::process::ExitCode::SUCCESS
        }
        Err(_) => {
            // Dependency errors may contain URLs, credentials or stored content.
            eprintln!(
                "{}",
                serde_json::json!({"event":"failure", "message":"Service failed; inspect the last startup phase and operator runbook"})
            );
            std::process::ExitCode::FAILURE
        }
    }
}

async fn run() -> anyhow::Result<()> {
    eprintln!(
        "{}",
        serde_json::json!({"event":"startup", "phase":"configuration"})
    );
    let config = Config::load()?;
    eprintln!("{}", serde_json::json!({"event":"startup", "phase":"tls"}));
    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|_| anyhow::anyhow!("could not initialize TLS cryptography"))?;
    let tls = RustlsConfig::from_pem_file(&config.tls_cert_path, &config.tls_key_path)
        .await
        .map_err(|_| anyhow::anyhow!("could not load TLS certificate and private key"))?;
    let database_path = config.database_path.clone();
    eprintln!(
        "{}",
        serde_json::json!({"event":"startup", "phase":"database"})
    );
    let store = tokio::task::spawn_blocking(move || Store::open_exclusive(&database_path))
        .await
        .context("database startup task failed")??;
    eprintln!(
        "{}",
        serde_json::json!({"event":"startup", "phase":"listener"})
    );
    let listener = TcpListener::bind(config.listen_addr)
        .await
        .context("could not bind service listener")?;
    let app = service::router(config, store)?;
    eprintln!(
        "{}",
        serde_json::json!({"event":"listening", "address":listener.local_addr()?.to_string()})
    );
    runtime::serve_tls(listener, app, tls)
        .await
        .context("service failed")
}
