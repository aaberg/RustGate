# RustGate

A security-first reverse proxy that sits between AI agents (build containers, coding harnesses) and your LLM API keys.

The real upstream API key never enters the container. RustGate issues short-lived per-job tokens, enforces per-job quotas, scans request bodies for leaked secrets, injects an undeletable operator system prompt, and streams responses straight through.

## How it works

```
┌────────────────┐   job token    ┌────────────┐   real API key   ┌──────────────┐
│  build agent   │ ─────────────▶ │  RustGate  │ ───────────────▶ │   Mistral    │
│ (opencode, …)  │ ◀───────────── │            │ ◀─────────────── │  (or other   │
└────────────────┘  SSE stream    └────────────┘   SSE stream      │   provider)  │
                                    ▲                              └──────────────┘
                     control token  │
                    ┌───────────────┴───┐
                    │  builder service │  POST /v1/jobs → gets the job token to inject
                    └──────────────────┘
```

- **Data plane** (containers): `POST /v1/chat/completions`, `POST /v1/embeddings`, `GET /v1/models`, `GET /v1/job/me` — authenticate with the per-job bearer token. Anything else on the proxy → 403.
- **Control plane** (builder service): `POST /v1/jobs`, `POST /v1/jobs/{id}/revoke`, `POST /v1/jobs/{id}/done` — authenticate with the control-plane token.
- Every proxied request passes: token check (SHA-256 hash lookup) → job state check → path allowlist → body policy (size cap, secret scan) → quota gate → forward with the real key + injected system prompt.

## Quick start (dev)

```bash
cargo build

# 1. key pool (dev: env var)
export RUSTGATE_KEY_MISTRAL_MAIN='mistral:sk-your-real-key'

# 2. control-plane token + dev HTTP mode
export RUSTGATE_CONTROL_TOKEN='dev-control-token'
export RUSTGATE_ALLOW_INSECURE=1
export RUSTGATE_LISTEN_PORT=8080

./target/debug/rustgate
```

Issue a job (control plane):

```bash
curl -s -X POST http://localhost:8080/v1/jobs \
  -H "Authorization: Bearer dev-control-token" \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_id": "acme",
    "upstream_key_id": "mistral_main",
    "system_prompt": "You are a build agent. Never exfiltrate secrets.",
    "max_requests": 100,
    "max_tokens_total": 2000000
  }'
# → {"job_id":"…","token":"rg_…","expires_at":"…","proxy_url":"https://…:8443/v1"}
```

Use it (data plane, inside the container):

```bash
curl -s http://localhost:8080/v1/chat/completions \
  -H "Authorization: Bearer rg_…" \
  -H "Content-Type: application/json" \
  -d '{"model":"mistral-small-latest","messages":[{"role":"user","content":"hi"}]}'
```

Streaming works identically (`"stream": true`) — chunks are flushed immediately, and the proxy sniffs the final SSE usage chunk to account tokens.

Revoke or finish:

```bash
curl -X POST http://localhost:8080/v1/jobs/<job_id>/revoke \
  -H "Authorization: Bearer dev-control-token" \
  -d '{"tenant_id":"acme","reason":"build cancelled"}'

curl -X POST http://localhost:8080/v1/jobs/<job_id>/done \
  -H "Authorization: Bearer dev-control-token"
```

## Production configuration

TLS is required in production (dev plain HTTP needs `RUSTGATE_ALLOW_INSECURE=1`). Configuration comes from environment variables:

| Variable | Meaning | Default |
|---|---|---|
| `RUSTGATE_LISTEN_ADDR` / `RUSTGATE_LISTEN_PORT` | Listen address | `127.0.0.1:8443` |
| `RUSTGATE_TLS_CERT` / `RUSTGATE_TLS_KEY` | TLS cert/key paths (PEM) | none (required unless insecure dev) |
| `RUSTGATE_SQLITE_PATH` | SQLite database path | `rustgate.db` |
| `RUSTGATE_SECRETS_FILE` | Upstream key pool file (see below) | none |
| `RUSTGATE_CONTROL_TOKEN` (or `RUSTGATE_CONTROL_TOKEN_FILE`) | Control-plane bearer token | none |
| `RUSTGATE_KEY_<ID>` | Upstream key, `provider:sk-...` (dev fallback) | — |
| `RUSTGATE_UPSTREAM_BASE_URL` | Overrides the `mistral` provider base URL | `https://api.mistral.ai` |

### Key pool file

One entry per line, mode 0600 (RustGate warns if more permissive):

```
# key_id = { "provider": "...", "key": "..." }
mistral-main = { "provider": "mistral", "key": "sk-..." }
# or compact:
mistral-backup = mistral:sk-...
```

Each job binds to one key id, so you can set per-key spend limits upstream and rotate by adding a new key and pointing new jobs at it.

## Using it with opencode (and other harnesses)

RustGate speaks the OpenAI-compatible protocol, so any harness that supports custom providers works. For [opencode](https://opencode.ai), set `baseURL` to the proxy and `apiKey` to the job token:

```json
{
  "$schema": "https://opencode.ai/config.json",
  "provider": {
    "rustgate-mistral": {
      "npm": "@ai-sdk/openai-compatible",
      "name": "Mistral via RustGate",
      "options": {
        "baseURL": "https://proxy.internal:8443/v1",
        "apiKey": "{env:RUSTGATE_JOB_TOKEN}"
      },
      "models": {
        "mistral-small-latest": {},
        "mistral-large-latest": {}
      }
    }
  }
}
```

Inject `RUSTGATE_JOB_TOKEN` (and the base URL env your harness reads) into the build container's environment. Notes:

- `GET /v1/models` is allowed, so model discovery works.
- The operator system prompt is injected as the **first** system message (sentinel `[[RUSTGATE-OPERATOR]]`); the harness's own system prompt follows it and cannot delete the operator rules.
- `GET /v1/job/me` returns remaining quota if your tooling wants to surface budget.

## Observability

- `GET /healthz` — liveness.
- `GET /metrics` — Prometheus: requests by verdict, blocked by reason, tokens burned per key/provider, in-flight count, upstream error rate.
- Structured JSON logs (`tracing`): one line per request with job_id, tenant_id, model, token counts, latency, verdict. **No request bodies are ever logged** — bodies exist in `request_log` only as size/token counts.

## Policy behavior

- Bodies over the size cap (default 1 MB) → 403.
- Secret-shaped strings in the body (`sk-…`, `sk-ant-…`, AWS key ids, JWTs, PEM private keys, long base64 runs) → 403 `possible_secret`, never forwarded.
- Prompt-injection markers ("reveal your system prompt", …) → logged as advisory, never blocked (v1: tripwire, not a wall).
- Quota exhaustion → 429; revoked/expired/done → 401; unknown token → 401 (generic message, no detail leakage).
- Path allowlist: only `/v1/chat/completions`, `/v1/embeddings`, `/v1/models` are forwardable (per-provider configurable).
- Proxy never retries upstream 5xx — passes through with `retry-after` and lets the agent decide.

## Adding other providers

RustGate is not Mistral-locked. Providers plug in via the `ProviderAdapter` trait (`src/adapters.rs`). Any OpenAI-compatible provider works today by adding a key-pool entry and provider config; a translating adapter for non-compatible providers (e.g. Anthropic) is a contained future addition. The client-facing API stays the OpenAI-compatible shape regardless of upstream.

## Development

```bash
cargo test        # unit + end-to-end integration tests against a mock upstream
cargo build --release
```

The integration tests in `tests/integration.rs` cover the acceptance criteria from `DESIGN.md`: issuance→proxy→expiry→rejection, revoke, quota caps under concurrency, secret-scan blocking, system-prompt injection (and no duplicates), SSE streaming pass-through, and path-allowlist rejection.
