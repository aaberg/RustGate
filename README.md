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

## Run against Mistral

For a local test with a real upstream, [create an API key in Mistral Studio](https://docs.mistral.ai/getting-started/quickstarts/studio/activate-and-generate-api-key). This example uses Bash or Zsh, plus `jq` and `uuidgen`. RustGate needs outbound HTTPS access to `api.mistral.ai:443`; the test chat request may incur Mistral charges.

1. From the repository root, run `cargo build --locked --bin rustgate` and generate `cert.pem` and `key.pem` as in step 2 of [Run locally](#run-locally). Leave port `8443` free and skip the mock upstream.

2. In a trusted terminal with shell tracing off, paste the Mistral key at the hidden prompt. Start RustGate on loopback with a separate SQLite database and a key-pool entry named `mistral`:

   ```bash
   printf 'Mistral API key: ' >&2
   IFS= read -rs mistral_key
   printf '\n' >&2
   control_token="$(openssl rand -hex 32)"

   RUSTGATE_CONFIG=rustgate.example.toml \
     RUSTGATE_UPSTREAM_BASE_URL=https://api.mistral.ai \
     RUSTGATE_DATABASE_PATH=rustgate.sqlite3.mistral \
     RUSTGATE_CONTROL_PLANE_TOKEN="$control_token" \
     RUSTGATE_UPSTREAM_KEYS="$(printf '%s' "$mistral_key" | jq -Rs '{mistral: .}')" \
     ./target/debug/rustgate &
   rustgate_pid=$!
   unset mistral_key
   ```

   Set the upstream URL without `/v1`: RustGate appends the API path. The client base URL is `https://localhost:8443/v1`.

3. After RustGate logs `listening`, use the same terminal to issue a short-lived job. `upstream_key_id` selects `mistral` from the key pool; it is not the API key.

   ```bash
   job_json="$(
     printf 'Authorization: Bearer %s\n' "$control_token" |
       curl -fsS --cacert cert.pem -H @- -H 'Content-Type: application/json' \
         -d "$(jq -n --arg id "$(uuidgen)" '{
           job_id: $id, tenant_id: "local-builder", upstream_key_id: "mistral",
           system_prompt: "Answer briefly.", ttl_seconds: 300, max_requests: 3,
           max_tokens_total: 2048
         }')" \
         https://localhost:8443/v1/jobs
   )"
   job_token="$(jq -er '.token' <<< "$job_json")"
   unset job_json
   ```

4. Send a text chat request using only the job token. A successful response contains the Mistral completion in `choices[0].message.content`. If the example model isn't available to your workspace, use a chat model ID returned by `GET /v1/models` with the same job token.

   ```bash
   printf 'Authorization: Bearer %s\n' "$job_token" |
     curl -fsS --cacert cert.pem -H @- -H 'Content-Type: application/json' \
       -d '{"model":"mistral-small-latest","messages":[{"role":"user","content":"Say hello."}],"max_tokens":32,"stream":false}' \
       https://localhost:8443/v1/chat/completions
   ```

   Give build containers only the job token and the client base URL, never the Mistral key or the control-plane token. To stop the local server, run `kill "$rustgate_pid"; wait "$rustgate_pid"` and `unset control_token job_token`.

## Use RustGate with OpenCode

This example uses [OpenCode 2](https://opencode.ai/docs/) and the local mock, so it does not require a Mistral API key. For OpenCode 1.x, see the [older provider configuration](DESIGN.md#testing-with-opencode).

1. [Start RustGate and the mock](#run-locally), then [issue a job token](DESIGN.md#issuing-and-inspecting-a-job) with upstream key ID `local`. Use a fresh `job_id` for each new token.

2. In the client terminal, from the RustGate checkout, load **only the job token** and point OpenCode at the development certificate. Do not give OpenCode the control-plane token or upstream API key.

   ```sh
   export NODE_EXTRA_CA_CERTS="$PWD/cert.pem"
   ```

   Set `RG_JOB_TOKEN` to the issued job token in that terminal. For example, in Bash or Zsh, with shell tracing off:

   ```bash
   printf 'RustGate job token: ' >&2
   IFS= read -rs RG_JOB_TOKEN
   printf '\n' >&2
   export RG_JOB_TOKEN
   ```

3. Switch to the project directory where you want to use OpenCode. Add the following to `opencode.json`, merging it into your existing configuration if you have one:

   ```json
   {
     "$schema": "https://opencode.ai/config.json",
     "model": "rustgate/mock-mistral",
     "providers": {
       "rustgate": {
         "name": "RustGate",
         "env": ["RG_JOB_TOKEN"],
         "package": "@opencode/ai/providers/openai-compatible",
         "settings": {
           "baseURL": "https://localhost:8443/v1"
         },
         "models": {
           "mock-mistral": {
             "name": "RustGate mock",
             "capabilities": { "input": ["text"], "output": ["text"] },
             "limit": { "context": 32768, "output": 4096 }
           }
         }
       }
     }
   }
   ```

4. Run a test prompt from the same terminal and project directory:

   ```sh
   opencode run --standalone --model rustgate/mock-mistral "Say hello. Do not use tools."
   ```

   The mock should reply `Hello from mock.`. For the interactive UI, run `opencode --standalone`. Standalone mode starts a private OpenCode server that inherits the token and certificate settings from this shell.

To use [real Mistral](#run-against-mistral), issue a job for upstream key ID `mistral` and replace `mock-mistral` in the model entry and selection with an available Mistral chat model ID, such as `mistral-small-latest`. Set the model limits for the selected model and issue a job with enough [request and token budget](DESIGN.md#hard-token-budgets) for OpenCode's prompts, tools, and multiple turns; the small curl test job above is not sized for this. If RustGate runs on another host, use the job's returned `proxy_url` as `baseURL` (including `/v1`) and trust that deployment's CA instead of the development certificate.

## Deployment

The [production configuration](deploy/rustgate.toml) already points to `https://api.mistral.ai`. Follow the [production runbook](DESIGN.md#production-deployment-and-operations) to provision the `mistral` key-pool entry on the RustGate host and set up a private listener with trusted TLS. Do not deploy the mock or the local certificate.

For containers, use the [Docker Compose guide](DESIGN.md#container-images-and-docker-compose) and its production configuration without the mock override.
