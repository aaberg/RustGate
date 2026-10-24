use rustgate::config::Config;
use rustgate::router::router;
use rustgate::secrets::KeyPool;
use rustgate::state::AppState;
use rustgate::store::Store;
use std::sync::Arc;

fn main() {
    // Structured JSON logs. No request bodies are ever logged.
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cfg = Config::load().expect("configuration");
    let store = Store::open(&cfg.sqlite_path).expect("sqlite store");
    let recovered = store.recover_orphaned_reservations();
    if recovered > 0 {
        tracing::info!(count = recovered, "recovered orphaned reservations from previous process");
    }
    let pool = KeyPool::load(&cfg).unwrap_or_default();
    if pool.ids().is_empty() {
        tracing::warn!("no upstream keys loaded; job issuance will fail until a key pool is provided");
    }
    let state = Arc::new(AppState::build(cfg.clone(), store, pool));

    let addr = format!("{}:{}", cfg.listen_addr, cfg.listen_port);
    tracing::info!(%addr, "rustgate listening");

    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    rt.block_on(async move {
        // Periodic expiry sweep (auth path also checks expiry lazily per job).
        let st = state.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(30));
            loop {
                tick.tick().await;
                let n = st.store.sweep_expired();
                if n > 0 {
                    tracing::info!(count = n, "swept expired jobs");
                }
            }
        });

        if let (Some(cert), Some(key)) = (cfg.tls_cert_path.clone(), cfg.tls_key_path.clone()) {
            let tls = axum_server::tls_rustls::RustlsConfig::from_pem_file(cert, key)
                .await
                .expect("tls config");
            let app = router(state);
            let sock_addr: std::net::SocketAddr = addr.parse().expect("parse listen addr");
            axum_server::bind_rustls(sock_addr, tls)
                .serve(app.into_make_service())
                .await
                .expect("serve");
        } else {
            tracing::warn!("TLS not configured; serving plain HTTP (dev only)");
            let listener = tokio::net::TcpListener::bind(&addr).await.expect("bind");
            axum::serve(listener, router(state)).await.expect("serve");
        }
    });
}
