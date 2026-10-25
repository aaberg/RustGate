use axum::Router;
use axum_server::{Handle, tls_rustls::RustlsConfig};
use tokio::net::TcpListener;

/// Plain HTTP is used only by the loopback-only mock upstream.
pub async fn serve(listener: TcpListener, router: Router) -> std::io::Result<()> {
    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal()?)
        .await
}

/// Serve TLS only, draining admitted requests when a shutdown signal arrives.
pub async fn serve_tls(
    listener: TcpListener,
    router: Router,
    tls: RustlsConfig,
) -> std::io::Result<()> {
    let shutdown = shutdown_signal()?;
    let handle = Handle::new();
    let server = axum_server::from_tcp_rustls(listener.into_std()?, tls)?
        .handle(handle.clone())
        .serve(router.into_make_service());
    tokio::pin!(server);
    tokio::select! {
        result = &mut server => result,
        _ = shutdown => {
            handle.graceful_shutdown(None);
            server.await
        }
    }
}

fn shutdown_signal() -> std::io::Result<impl Future<Output = ()> + Send + 'static> {
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

    Ok(shutdown)
}
