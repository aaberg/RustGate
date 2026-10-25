# Model Proxy Service — Specification
## Purpose

A job-scoped HTTP reverse proxy that sits between build containers (running AI agents) and the upstream LLM API (Mistral). It ensures the real upstream API key never enters a build container, enforces per-job quotas, provides observability, and injects authoritative system prompts.
## Boundaries and network position

* Listens on an internal interface only (private network / WireGuard), HTTPS with its own cert (mTLS optional; at minimum server-auth TLS since clients are on a trusted network segment).
* Upstream: configurable base URL (e.g. https://api.mistral.ai), egress from the proxy box to the upstream is the only internet egress this service makes.
* Build containers' egress is firewalled to allow only this proxy's address and port. The container is told (via env/config) the proxy URL and its per-job token. It never sees an upstream API key.

## Data model

Persist in an embedded SQLite store.

### jobs

* job_id (uuid, PK)
* tenant_id
* upstream_key_id (reference to which real key pool entry serves this job)
* token_hash (sha256 of the per-job bearer token; never store the token itself)
* system_prompt (authoritative operator prompt supplied by the builder, persisted verbatim)
* created_at, expires_at (issue + 30 min, configurable)
* max_requests, requests_used
* max_tokens_total (sum of request+response tokens across the job), tokens_used
* status: active | expired | revoked | exhausted | done
* revoked_at, revoked_reason

request_log (append-only, one row per proxied request)

* job_id, ts, path, model, request_bytes, response_bytes, status_code (upstream), duration_ms, prompt_tokens, completion_tokens (when upstream reports), policy_verdict (allowed / blocked), block_reason

## API surface

1. POST /v1/jobs — issue a job token (called by the builder service)

* Authentication: the proxy and the builder service authenticate to each other with mTLS client certs, or a shared static internal bearer token over the private network. Not with job tokens — this is the control-plane endpoint.
* Request body: { "job_id": "<uuid>", "tenant_id": "...", "upstream_key_id": "...", "system_prompt": "<authoritative product rules>", "ttl_seconds": 1800, "max_requests": 100, "max_tokens_total": 2000000 }
* The proxy generates the job's bearer token itself (random 32 bytes, crypto-secure RNG) and returns it in the issuance response; the proxy stores only its hash. The builder receives the token over the mutually authenticated channel and injects it into the build container.
  * Decision: the proxy is the token generator, not the builder. Rationale: the proxy owns the data-plane auth domain, token format and entropy requirements are enforced in one place, and the builder never needs logic for token generation. The trade-off is that the proxy momentarily sees the raw token at issuance time (before hashing) — accepted, since the proxy is trusted infrastructure. To keep the builder from having to trust the proxy's channel alone, issuance responses are sent only over mTLS or the shared internal token.
  * To mitigate token leakage on the proxy side, the raw token exists only in memory of the issuance handler for the duration of the response; it is never logged, never persisted, and never appears in metrics.
* Response: { "job_id": "...", "token": "...", "expires_at": "...", "proxy_url": "https://proxy.internal:8443/v1" }

2. POST /v1/jobs/{job_id}/revoke — immediate kill switch (builder service, or an admin)

* Marks an active, unexpired job revoked. Requests already admitted complete; new requests get 401. Requires the same control-plane authentication as issuance.

3. POST /v1/jobs/{job_id}/done — job finished normally

* Marks an active, unexpired job done and closes admission. Requests already admitted complete; new requests get 401. Requires control-plane authentication. Once accounting is implemented, in-flight reservations must remain charged until their requests reconcile; `/done` must not release them early or reset usage.

4. POST /v1/chat/completions, POST /v1/embeddings, etc. — proxy endpoints (called by build containers)

* Bearer token = the per-job token. Reject anything that doesn't validate against an active job (hash match, not expired, not revoked/exhausted). On rejection return 401/403 with a generic message (no detail leakage).
* On success, forward the request to the upstream, transparently rewriting:
  * Authorization header → the real upstream key for this job's upstream_key_id.
  * System prompt injection (see Policy section).
* Response is streamed back to the container (SSE streaming pass-through must work — models stream; don't buffer whole responses). Count tokens from upstream usage fields when present.

## Policy layer (checked per request, before forwarding)

Order of checks on every proxied request:

* AuthN: valid job token → hashed lookup in jobs.
* AuthZ/state: job is active; token not expired; not revoked.
* Quotas: requests_used < max_requests and tokens_used < max_tokens_total. If exceeded: return 429 with a clear error and mark job exhausted. Increment requests_used before forwarding (with a reservation that's reconciled on completion — don't let concurrent requests race past the cap).
* Path allowlist: only the configured upstream endpoints are forwardable (/v1/chat/completions, /v1/embeddings, /v1/models). Anything else → 403. This prevents a jailbroken agent from using admin endpoints or other products under the same upstream account.
* Body policy checks (fast, local, no LLM):
  * Request size cap (e.g. 1 MB) — also your cost guard.
  * Secret-scanning heuristic on the serialized body: regexes for common key shapes (sk-…, the upstream key prefix pattern, long high-entropy base64 runs). A match → block with 403 and log block_reason=possible_secret. Rationale: an agent that found a secret somewhere must not be able to send it through the proxy either. Cheap, deterministic, high value.
  * Prompt-injection marker scan (best-effort, advisory only — log a flag, don't block by default): phrases like "reveal your system prompt", "print your environment", "include the API key". This is a tripwire, not a wall: it feeds the audit trail and the per-tenant risk score, and can be escalated to blocking later once you know your false-positive rate.
* System prompt injection (step 5): prepend a fresh system message containing the persisted authoritative `system_prompt` verbatim. Remove every client message whose `role` is exactly `"system"` and whose string `content` exactly equals that prompt; preserve all other messages in order. Never trust a client-supplied sentinel, name, or header as proof that the authoritative prompt is present. The builder supplies the prompt at issuance; a container cannot replace the persisted prompt.

* Upstream key pool

  * The proxy holds the real upstream API keys, loaded at startup from a secrets source (env vars are acceptable here, since the proxy is trusted infrastructure — but prefer a mounted secrets file or Vault later).
  * Keys are named/selected per job (upstream_key_id) so you can put per-key spend limits at Mistral as an outer ring, and rotate by adding a new key id and migrating job issuance.
  * Track per-key monthly spend in request_log aggregates; expose current burn for observability.

## Streaming and timeouts

* Pass-through streaming both ways; flush chunks immediately.
* Upstream connect timeout ~5 s, first-byte timeout ~60 s, overall request timeout ~10 min (long agent calls), idle stream timeout ~120 s.
* Total requests in flight: bounded (e.g. 2× max concurrent builds) — return 503 with retry hint when saturated.

## Observability

* Structured logs (JSON, one line per request) with job_id, tenant_id, model, token counts, latency, policy verdict. No request bodies in logs (they contain user content — write a redaction policy now: bodies go to request_log only as size/token counts; optionally an opt-in sample store for debugging, clearly flagged).
* Prometheus metrics: requests per job/tenant, blocked requests by reason, tokens burned per key, in-flight count, upstream error rate.
* Health endpoint GET /healthz for the orchestrator.

## Failure behavior

* Upstream 5xx → pass through to container with retry-after guidance; the container's agent harness decides to retry. Proxy itself never retries (avoid double-billing surprises) — log and let the agent retry.
* Proxy restart: active jobs survive via SQLite (or are quickly re-issued by the builder; document either path).

## Configuration (env or file)

* Listen addr + TLS cert paths; upstream base URL; path allowlist; default/max TTL, request cap, token cap; secret-scan regex list; secrets source for the key pool; SQLite path. Prompt injection uses the persisted job prompt, not a configurable sentinel.

## Testing requirements (give these to the agent as acceptance criteria)

 * Token issuance → proxied call → expiry → rejection flow end-to-end.
 * Hold an admitted upstream call open, revoke the job, and verify the held call completes while subsequent requests immediately fail. Apply the same admission boundary to done and expiry.
 * Quota exhaustion: request cap and token cap both enforced under concurrency (spawn parallel requests; assert the cap holds).
 * Secret-scan: a request body containing a fake sk- key is blocked; a benign body is not.
 * System-prompt injection: absent, spoofed, already-present, and duplicated operator messages produce exactly one authoritative system-content match at index zero. Preserve other messages, metadata, and chat fields; exercise verbatim whitespace/Unicode, non-system matches, malformed message arrays, job isolation, and restart.
 * Streaming: SSE response streams incrementally end-to-end.
 * Path allowlist rejects /v1/admin style paths.

## Implemented service: steps 1 through 5

The service implements HTTPS, configuration, health, graceful shutdown, authenticated job issuance, SQLite persistence, job-authenticated non-streaming chat proxying, durable expiry/revoke/done transitions, and authoritative prompt injection. The separate mock remains a loopback-only HTTP fixture. Quota enforcement, streaming, and full resource bounds are **not implemented yet**; persisted caps are issuance metadata until their roadmap increments. Do not connect real build containers yet.

### Running

With a stable Rust toolchain supporting edition 2024 and OpenSSL for local certificate generation:

```sh
# Development only; production needs a certificate trusted by the builder.
openssl req -x509 -newkey rsa:3072 -nodes \
  -keyout key.pem -out cert.pem -days 7 -subj '/CN=localhost' \
  -addext 'subjectAltName=DNS:localhost,IP:127.0.0.1'
chmod 600 key.pem

export RUSTGATE_CONTROL_PLANE_TOKEN="$(openssl rand -hex 32)"
export RUSTGATE_UPSTREAM_KEYS='{"local":"fake-upstream-credential"}'
export RUSTGATE_CONFIG=rustgate.example.toml
cargo run --bin rustgate
# In a terminal with the same environment:
curl --cacert cert.pem https://localhost:8443/healthz
# {"status":"ok"}

# Separate terminal:
cargo run --bin mock-upstream
```

The production service accepts TLS only, including on loopback. Bind an internal interface and enforce the network boundary described above; TLS does not make a public listener safe. Server authentication plus the static control-plane bearer is implemented; mTLS is not. The configured `proxy_url` is advertised verbatim, so its hostname and port must match the client-facing deployment and certificate. Port `0` selects an ephemeral listener; configure a usable advertised URL separately.

Both executables handle SIGINT and, on Unix, SIGTERM by stopping new connections and finishing admitted requests. No forced drain deadline is imposed.

### Configuration

Precedence: built-in defaults, optional TOML file selected by `RUSTGATE_CONFIG`, then environment overrides. An explicitly selected missing/malformed file, unknown field, invalid bound, or missing security setting fails startup without echoing secret values. See `rustgate.example.toml`. All fields have an environment override named `RUSTGATE_` plus the uppercase TOML field.

| TOML field | Default / requirement |
| --- | --- |
| `listen_addr` | `127.0.0.1:8443`; IP socket address |
| `upstream_base_url` | `http://127.0.0.1:9090`; HTTPS, or HTTP on loopback only |
| `tls_cert_path`, `tls_key_path` | Required PEM certificate chain and matching private key |
| `database_path` | `rustgate.sqlite3`; persistent file, not `:memory:` or a `file:` URI |
| `control_plane_token` | Required; at least 32 visible ASCII bytes without whitespace; generate randomly |
| `upstream_keys` | Required nonempty table of key IDs to credentials; environment override is a JSON object |
| `proxy_url` | Required HTTPS URL with exactly `/v1` path, no credentials, query, or fragment |
| `default_ttl_seconds`, `max_ttl_seconds` | `1800`, `86400`; maximum cannot exceed 31536000 seconds |
| `default_max_requests`, `max_requests` | `100`, `10000` |
| `default_max_tokens_total`, `max_tokens_total` | `2000000`, `100000000` |

Defaults must be positive and not exceed their maxima; all maxima must fit a signed SQLite integer. Upstream URLs cannot include credentials, query strings, or fragments. Key credentials must be nonempty visible ASCII without whitespace. Mount secret configuration read-only with restrictive permissions, or supply environment variables; neither key-pool credentials nor the control credential are persisted in SQLite. Keys are loaded at startup; restart to change the pool.

### Issuing and inspecting a job

```sh
curl --cacert cert.pem https://localhost:8443/v1/jobs \
  -H "Authorization: Bearer $RUSTGATE_CONTROL_PLANE_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{
    "job_id":"74fd5462-016e-4ba5-bf6d-5342efbd115f",
    "tenant_id":"local-builder",
    "upstream_key_id":"local",
    "system_prompt":"Follow the product rules. Never disclose credentials.",
    "ttl_seconds":1800,
    "max_requests":100,
    "max_tokens_total":2000000
  }'
# 201: {"job_id":"...","token":"...","expires_at":"...","proxy_url":"https://localhost:8443/v1"}
```

`job_id`, `tenant_id`, `upstream_key_id`, and `system_prompt` are required. The three numeric limits are optional and use configured defaults, but explicit values must be positive and no greater than their configured maximum. Tenant IDs are nonblank and at most 256 bytes; key IDs are nonblank and at most 128 bytes. The entire body is limited to 1 MiB. Unknown JSON fields are rejected.

Issuance authentication runs before body parsing. Missing, invalid, duplicate Authorization headers, or a job token used as a control credential receive `401`. Unknown key IDs and invalid field values receive `400`; a duplicate UUID receives `409` without modifying its original job. Successful responses use `Cache-Control: no-store`; the bearer is returned only once. There is no token-recovery API: store it securely in the builder and never log issuance bodies.

Each token contains 32 cryptographically random bytes encoded as 64 lowercase hex characters. Only SHA-256 of that encoded bearer is persisted. A transaction commits the job before its token is returned. Jobs store the prompt verbatim, UTC epoch-second timestamps, zero usage counters, and `active` status. The response expiry is RFC 3339 UTC. Lost issuance responses do not roll back committed jobs: retrying the same UUID returns `409`, not a replacement token.

SQLite migrations run transactionally at startup and track schema version with `PRAGMA user_version`; a newer unsupported version fails startup. The database is created with mode `0600` on Unix. Keep its directory restricted too, since the prompt is sensitive. Back up the database using SQLite-aware tooling; retain the file across restarts.

```sh
# Requires the sqlite3 CLI; never select request credentials into logs.
sqlite3 rustgate.sqlite3 \
  'SELECT job_id, tenant_id, upstream_key_id, length(token_hash), status, expires_at FROM jobs;'
# Stop and restart rustgate with the same config/database, then repeat.
```

An existing UUID remains reserved across restart. Reissuing it must return `409`, demonstrating that issuance state survived. Its original token remains usable for proxy calls while the job is active and unexpired.

### Completing or revoking a job

Both lifecycle endpoints use the control-plane credential, never the job token. Supply the issued UUID as `JOB_ID`; no request body is required or interpreted:

```sh
export JOB_ID='<job_id returned at issuance>'
curl --cacert cert.pem -X POST \
  "https://localhost:8443/v1/jobs/$JOB_ID/revoke" \
  -H "Authorization: Bearer $RUSTGATE_CONTROL_PLANE_TOKEN"
# 204, empty body. For normal completion, use /done instead of /revoke.
```

Authentication runs before path extraction or body reads. UUIDs are normalized exactly as at issuance. Success returns `204` with no body; malformed UUIDs and invalid UTF-8 job paths (for example, `%FF`) receive generic JSON `400` responses with an `error.message` field, unknown jobs `404`, and storage failures generic `500`. Path-extraction diagnostics are not exposed. Repeating the same transition returns `204` without changing metadata. A different terminal state (`expired`, `revoked`, `exhausted`, or `done`) receives `409`: the first terminal state wins, and jobs never reactivate. Responses use `Cache-Control: no-store`; invalid control credentials receive generic `401` with `WWW-Authenticate: Bearer`.

Revoke persists `status = revoked` and the UTC epoch-second `revoked_at`. Done persists `status = done`. No reason-body API is provided; `revoked_reason` remains null. Neither endpoint resets usage, creates accounting records, or cancels admitted requests. Reservation and request-log machinery belongs to the later accounting increments; when added, it must reconcile admitted work normally even after `/done`.

Expiry is effective when `expires_at <= now`, using UTC epoch seconds sampled after obtaining the SQLite transaction lock. Startup persists `expired` for every overdue active job. During service operation, admission and lifecycle operations materialize expiry for the job they access; there is no background expiry sweep. An untouched overdue row can therefore still read `active` directly from SQLite until access or restart, but its token cannot admit a new call. An overdue active job becomes `expired` before a revoke/done attempt, which returns `409`.

Admission and lifecycle transitions serialize in SQLite Immediate transactions, including across independently opened store connections. Admission commits a snapshot of the eligible job's key ID and authoritative prompt before reading the request body or contacting upstream. A request admitted before revoke, done, or expiry continues using that snapshot without rechecking status, even if its body or upstream response is still pending. Once a transition completes, subsequent admission fails with `401`. Terminal state and revocation metadata survive restart; an unexpired active job's original token continues to work.

### Proxying a chat request

Use the returned job token, not the control-plane credential:

```sh
export JOB_TOKEN='<token returned at issuance>'
curl --cacert cert.pem https://localhost:8443/v1/chat/completions \
  -H "Authorization: Bearer $JOB_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"model":"mock-mistral","messages":[{"role":"user","content":"Hello"}],"stream":false}'
# 200: mock completion with "Hello from mock." and usage

# Local fixture only; inspect with fake credentials:
curl http://127.0.0.1:9090/__mock/requests
# authorization is "Bearer fake-upstream-credential", never the job token
```

Admission hashes the bearer and looks up the persisted job on every request, requiring `status = active` and `expires_at` strictly later than the current epoch second. Missing, malformed, duplicate, unknown, expired, or inactive job credentials receive generic `401` with `WWW-Authenticate: Bearer`. Control credentials are not job credentials. A job whose key ID has been removed from the startup key pool also receives `401`; storage failures receive generic `500`. Expiry and revoke/done ordering follow the lifecycle contract above.

After authentication, the fixed allowlist permits only exact `POST /v1/chat/completions`. Other methods and paths receive `403`, including trailing slashes, encoded alternatives, embeddings, models, and admin paths. The separate health, issuance, and lifecycle routes retain their own contracts. Authentication and route checks precede body reads.

The body must be a JSON object with a JSON content type and at most 1 MiB, including unknown-length/chunked bodies. `messages` must be an array (an empty array is permitted); missing or non-array values receive `400`. `stream` must be omitted or literal `false`; streaming, invalid JSON, non-object bodies, and invalid content types receive `400`, while oversized bodies receive `413`. Message-entry schemas and other provider-specific chat fields are left to upstream validation. JSON is parsed and reserialized before forwarding so duplicate keys cannot give the proxy and upstream different interpretations.

For supported chat requests, prompt injection removes all messages whose `role` is exactly `"system"` and whose string `content` equals the stored prompt, then prepends a new `{"role":"system","content":<stored prompt>}` message. This yields exactly one authoritative system-content match, at index zero, even if the client supplied several copies or placed them later in the conversation. Duplicate messages' client metadata is discarded; it is not attached to the authoritative message.

Equality is exact on decoded strings: no trimming, case folding, Unicode normalization, sentinel detection, or conversion of content-part arrays. The stored prompt is injected verbatim, including whitespace and Unicode. Other system messages, developer/user/assistant/tool messages, structured content, and all their fields retain their order and values; all other top-level chat fields remain unchanged. Names, sentinels, and headers never suppress injection. This guarantees payload placement, not model obedience or resistance to instructions in preserved client messages.

The 1 MiB cap applies to the incoming body before injection; the transformed upstream body may be larger. The local mock has its own independent 1 MiB request limit and can therefore reject a transformed near-limit request with `413`. No prompt transformation is applied to health, issuance, or lifecycle requests; embeddings and model listing remain forbidden until step 11.

The configured upstream base path is a prefix: `https://host/gateway/` targets `/gateway/v1/chat/completions`. The admitted request's query is preserved, but its host and path cannot select another destination. A fresh upstream request contains the selected key as bearer authorization and JSON content type, without forwarding client authorization, cookies, proxy credentials, or arbitrary client headers. Redirects and automatic retries are disabled, as are environment-selected HTTP proxies.

Responses are buffered in this increment. Upstream status and body pass through, along with `Content-Type`, `Content-Encoding`, and `Retry-After` when present; other upstream headers, including redirects' `Location` and cookies, are not forwarded. Compressed bodies are not decompressed by the proxy: all `Content-Encoding` values are preserved in order so clients can decode the unchanged bytes. Proxy responses use `Cache-Control: no-store`. Connection and response-read failures receive generic `502` without underlying diagnostics or credentials. Upstream failures are never retried; the client decides whether to retry. Usage in a completion is returned but not accounted, and requests do not yet consume or enforce persisted quotas.

The response buffer, upstream timeouts, disconnect handling, and in-flight concurrency are not yet bounded by the step 7 policy. SSE is not forwarded incrementally.

### Local mock upstream

`mock-upstream` binds `127.0.0.1:9090` by default; override with `RUSTGATE_MOCK_LISTEN_ADDR` (loopback IP socket address only). It also exposes `GET /healthz`. Port `0` selects an ephemeral port; both executables print their actual bound address.

Send JSON to `POST /v1/chat/completions`. The fixed assistant reply is `Hello from mock.`, with usage of 5 prompt tokens, 3 completion tokens, and 8 total tokens.

| Query control | Behavior |
| --- | --- |
| `mode=normal` (default) | Immediate JSON completion |
| `mode=delayed` | JSON completion after `delay_ms` |
| `mode=sse` | Immediate first SSE chunk, second chunk after `delay_ms`, then a usage chunk and `[DONE]` |
| `mode=failure` | HTTP 503 with a structured error and `Retry-After: 1` |
| `delay_ms` | Default 100; accepted range 0–10000 |

```sh
curl http://127.0.0.1:9090/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model":"mock-mistral","messages":[{"role":"user","content":"Hello"}]}'

curl -N 'http://127.0.0.1:9090/v1/chat/completions?mode=sse&delay_ms=500' \
  -H 'Content-Type: application/json' -d '{}'
```

`GET /__mock/requests` returns an array of captured requests with `method`, `path` (without query), `authorization` (or `null`), and parsed JSON `body`. `DELETE /__mock/requests` clears it and returns 204. Captures are in memory, isolated per fixture, and retain only the newest 100 requests. JSON request bodies are limited to 1 MiB; malformed JSON and invalid mode/delay controls are rejected without capture. Valid requests are captured before waiting or returning a controlled failure.

**The capture API deliberately exposes request bodies and credentials without authentication.** Use fake credentials only; nothing captured is written to logs or disk. This fixture is not part of the production service.

### Checks

```sh
cargo test --all-targets
cargo clippy --all-targets -- -D warnings
```
