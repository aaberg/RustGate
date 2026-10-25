use axum::Router;
use tokio::net::TcpListener;

/// Stop accepting connections on shutdown and finish requests already admitted.
pub async fn serve(listener: TcpListener, router: Router) -> std::io::Result<()> {
    // Register before serving so signal-handler setup failures fail startup.
    #[cfg(unix)]
    let shutdown = {
        use tokio::signal::unix::{SignalKind, signal};
        let mut interrupt = signal(SignalKind::interrupt())?;
        let mut terminate = signal(SignalKind::terminate())?;
        async move {
            tokio::select! {
                _ = interrupt.recv() => {},
                _ = terminate.recv() => {},
            }
        }
    };

    #[cfg(not(unix))]
    let shutdown = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            eprintln!("shutdown signal failed: {error}");
        }
    };

    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown)
        .await
}
