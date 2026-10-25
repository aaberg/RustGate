use anyhow::Context;
use rustgate::{config::Config, runtime, service};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::load()?;
    let listener = TcpListener::bind(config.listen_addr)
        .await
        .context("could not bind service listener")?;
    eprintln!("rustgate listening on http://{}", listener.local_addr()?);
    // The upstream setting is validated at startup; forwarding arrives in step 3.
    runtime::serve(listener, service::router())
        .await
        .context("service failed")
}
