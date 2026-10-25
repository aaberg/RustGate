use std::{env, net::SocketAddr};

use anyhow::{Context, Result, bail};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<()> {
    let listen_addr: SocketAddr = match env::var("RUSTGATE_MOCK_LISTEN_ADDR") {
        Ok(value) => value
            .parse()
            .context("RUSTGATE_MOCK_LISTEN_ADDR must be a valid socket address")?,
        Err(env::VarError::NotPresent) => SocketAddr::from(([127, 0, 0, 1], 9090)),
        Err(env::VarError::NotUnicode(_)) => {
            bail!("RUSTGATE_MOCK_LISTEN_ADDR must be a valid socket address")
        }
    };
    if !listen_addr.ip().is_loopback() {
        bail!("RUSTGATE_MOCK_LISTEN_ADDR must use a loopback address");
    }

    let listener = TcpListener::bind(listen_addr)
        .await
        .context("failed to bind local mock upstream")?;
    let bound_addr = listener
        .local_addr()
        .context("failed to read local mock upstream address")?;
    println!("mock upstream listening on http://{bound_addr}");
    rustgate::runtime::serve(listener, rustgate::mock::router())
        .await
        .context("local mock upstream server failed")
}
