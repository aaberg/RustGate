# RustGate

RustGate is a job-scoped HTTPS proxy for Mistral-compatible LLM APIs. It gives build containers short-lived job tokens instead of upstream API keys, enforces per-job request and token budgets, and inserts a builder-supplied system prompt into chat requests. Job state and usage are stored in SQLite.

## Prerequisites

- A current stable Rust toolchain with Cargo (Rust 2024 edition).
- A C compiler, linker, and standard development headers to build bundled SQLite and TLS dependencies (for example, `build-essential` on Debian or Ubuntu).
- OpenSSL for generating a local TLS certificate and `curl` for checking the server.
- Free loopback ports `8443` (RustGate) and `9090` (mock upstream).

## Run locally

These steps use the bundled mock upstream. No Mistral account or real API key is needed. Run all commands from the repository root.

1. Build both binaries and start the mock upstream. Leave it running in the first terminal.

   ```sh
   cargo build --locked --bins
   ./target/debug/mock-upstream
   ```

2. In a second terminal, generate a development-only certificate if you don't already have `cert.pem` and `key.pem`:

   ```sh
   (umask 077; openssl req -x509 -newkey rsa:3072 -nodes \
     -keyout key.pem -out cert.pem -days 7 -subj '/CN=localhost' \
     -addext 'subjectAltName=DNS:localhost,IP:127.0.0.1')
   ```

   Start RustGate in the same terminal with the sample configuration and fake upstream credential:

   ```sh
   export RUSTGATE_CONTROL_PLANE_TOKEN="$(openssl rand -hex 32)"
   export RUSTGATE_UPSTREAM_KEYS='{"local":"fake-upstream-credential"}'
   export RUSTGATE_CONFIG=rustgate.example.toml
   ./target/debug/rustgate
   ```

3. Once RustGate logs its `listening` event, check it from a third terminal. RustGate requires HTTPS even on loopback, so pass the development certificate to `curl`:

   ```sh
   curl --fail --silent --show-error --cacert cert.pem https://localhost:8443/healthz
   ```

   Expected response: `{"status":"ok"}`. This checks liveness only. To exercise the proxy, [issue a job token](DESIGN.md#issuing-and-inspecting-a-job) using the control token from step 2 and the upstream key ID `local`, then [send a chat request](DESIGN.md#proxying-a-chat-request) using the returned job token.

Stop both servers with `Ctrl+C` in their respective terminals. Local job state stays in `rustgate.sqlite3` across restarts.

## Deployment

See the [production runbook](DESIGN.md#production-deployment-and-operations) for a private HTTPS deployment with trusted TLS and real upstream credentials, or the [Docker Compose guide](DESIGN.md#container-images-and-docker-compose) for containerized local and production setups. Do not use the mock or development certificate in production.
