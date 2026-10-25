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
* Quotas: atomically require requests_used < max_requests and permanently increment requests_used at admission, before route/body checks. The final permitted admission marks the job exhausted; subsequent attempts return 429. Admitted rejections and upstream failures are charged without refund. Step 8 reconciles reported token usage; conservative token reservations and enforcement of max_tokens_total belong to step 9.
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

* Forward upstream response bytes incrementally, including SSE; never buffer a whole response or synthesize completion events. Chat request JSON remains bounded and buffered for authoritative prompt injection.
* Configurable upstream connect timeout defaults to 5 s, first-byte timeout to 60 s, overall request timeout to 10 min, and idle stream timeout to 120 s.
* Bound requests in flight per service process (default 64); return 503 with `Retry-After: 1` when saturated, without queueing or charging request allowance.

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

## Implemented service: steps 1 through 8

The service implements HTTPS, configuration, health, graceful shutdown, authenticated job issuance, SQLite persistence, job-authenticated chat proxying with incremental SSE responses, durable expiry/revoke/done transitions, authoritative prompt injection, concurrent request quotas, bounded concurrency/timeouts, and durable token usage accounting. The separate mock remains a loopback-only HTTP fixture. Hard token-budget enforcement is **not implemented yet**; persisted token caps remain issuance metadata. Do not connect real build containers yet.

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
| `connect_timeout_seconds` | `5`; TCP connect and TLS handshake deadline |
| `first_byte_timeout_seconds` | `60`; upstream dispatch through first nonempty body chunk or clean EOF |
| `idle_timeout_seconds` | `120`; wait for the next nonempty upstream body chunk or EOF |
| `overall_timeout_seconds` | `600`; admission, incoming body read, dispatch, and response relay |
| `max_in_flight_requests` | `64`; per process, accepted range 1–65536 |

Defaults must be positive and not exceed their maxima; all maxima must fit a signed SQLite integer. Upstream URLs cannot include credentials, query strings, or fragments. Key credentials must be nonempty visible ASCII without whitespace. Mount secret configuration read-only with restrictive permissions, or supply environment variables; neither key-pool credentials nor the control credential are persisted in SQLite. Keys are loaded at startup; restart to change the pool.

Each timeout must be an integer between 1 and 86400 seconds. The deadlines are independent: the earliest applicable deadline wins. Size concurrency for the deployment's expected parallel builds; processes sharing SQLite still have independent resource limits.

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

Run only one service process per database. Before migration or recovery, `Store::open_exclusive` acquires a nonblocking exclusive OS lock on `<canonical_database_path>.lock`, a persistent sidecar created with mode `0600` on Unix. It never locks SQLite's data bytes, avoiding Windows byte-range-lock conflicts. Relative and symlink aliases resolve to the same sidecar; do not use hard-link aliases for the database. The filesystem must support locking, and the service needs write access to the database directory. Do not remove or replace the database or sidecar while owners may exist; the service never unlinks the sidecar.

The shared `Store` owns the lock until its final clone is dropped, including queued/running admission and reconciliation workers after the listener and async `main` have finished. A replacement instance fails startup before touching pending records until those workers release ownership; an OS process crash also releases the lock. Library callers should use `Store::open_exclusive` and `Store::clone`; the lower-level `Store::open` requires externally managed exclusive ownership lasting through every clone and worker.

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

Revoke persists `status = revoked` and the UTC epoch-second `revoked_at`. Done persists `status = done`. No reason-body API is provided; `revoked_reason` remains null. Neither endpoint resets usage, creates accounting records, cancels admitted requests, or releases their reservations. Admitted requests reconcile normally even after `/done`, revoke, expiry, or exhaustion.

Expiry is effective when `expires_at <= now`, using UTC epoch seconds sampled after obtaining the SQLite transaction lock. Startup persists `expired` for every overdue active job. During service operation, admission and lifecycle operations materialize expiry for the job they access; there is no background expiry sweep. An untouched overdue row can therefore still read `active` directly from SQLite until access or restart, but its token cannot admit a new call. An overdue active job becomes `expired` before a revoke/done attempt, which returns `409`.

Admission and lifecycle transitions serialize in SQLite Immediate transactions, including across independently opened store connections. Admission commits one request charge and a snapshot of the eligible job's key ID and authoritative prompt before reading the request body or contacting upstream. A request admitted before revoke, done, expiry, or exhaustion continues using that snapshot without rechecking status, even if its body or upstream response is still pending. Once a transition completes, subsequent admission fails with `401`, or `429` for exhaustion. Terminal state, usage, and revocation metadata survive restart; an unexpired active job's original token continues to work within its remaining allowance.

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

When capacity is available, admission hashes the bearer and looks up the persisted job on every request, requiring `status = active`, `expires_at` strictly later than the current epoch second, and remaining request allowance. Missing, malformed, or duplicate credentials receive generic `401` before capacity checks; unknown, expired, revoked, or done job credentials receive generic `401` after capacity checks, with `WWW-Authenticate: Bearer`. Exhausted jobs receive `429` as specified below. Control credentials are not job credentials. A job whose key ID has been removed from the startup key pool also receives `401` after its admission is charged; storage failures receive generic `500`. Expiry and revoke/done ordering follow the lifecycle contract above.

After charged admission, the fixed allowlist permits only exact `POST /v1/chat/completions`. Other methods and paths receive `403`, including trailing slashes, encoded alternatives, embeddings, models, and admin paths. The separate health, issuance, and lifecycle routes retain their own contracts and do not consume job request allowance. Authentication, quota admission, and route checks precede body reads.

The body must be a JSON object with a JSON content type and at most 1 MiB, including unknown-length/chunked bodies. `messages` must be an array (an empty array is permitted); missing or non-array values receive `400`. `stream` may be omitted or a literal boolean, including `true` for SSE; other stream types, invalid JSON, non-object bodies, and invalid content types receive `400`, while oversized bodies receive `413`. Message-entry schemas and other provider-specific chat fields are left to upstream validation. JSON is parsed and reserialized before forwarding so duplicate keys cannot give the proxy and upstream different interpretations.

For supported chat requests, prompt injection removes all messages whose `role` is exactly `"system"` and whose string `content` equals the stored prompt, then prepends a new `{"role":"system","content":<stored prompt>}` message. This yields exactly one authoritative system-content match, at index zero, even if the client supplied several copies or placed them later in the conversation. Duplicate messages' client metadata is discarded; it is not attached to the authoritative message.

Equality is exact on decoded strings: no trimming, case folding, Unicode normalization, sentinel detection, or conversion of content-part arrays. The stored prompt is injected verbatim, including whitespace and Unicode. Other system messages, developer/user/assistant/tool messages, structured content, and all their fields retain their order and values. Other top-level chat fields remain unchanged except streaming usage options described below. Names, sentinels, and headers never suppress injection. This guarantees payload placement, not model obedience or resistance to instructions in preserved client messages.

The 1 MiB cap applies to the incoming body before injection; the transformed upstream body may be larger. The local mock has its own independent 1 MiB request limit and can therefore reject a transformed near-limit request with `413`. No prompt transformation is applied to health, issuance, or lifecycle requests; embeddings and model listing remain forbidden until step 11.

The configured upstream base path is a prefix: `https://host/gateway/` targets `/gateway/v1/chat/completions`. The admitted request's query is preserved, but its host and path cannot select another destination. A fresh upstream request contains the selected key as bearer authorization and JSON content type, without forwarding client authorization, cookies, proxy credentials, or arbitrary client headers. Redirects and automatic retries are disabled, as are environment-selected HTTP proxies.

Responses are forwarded incrementally, whether SSE, ordinary JSON, or an upstream error. Upstream status and body pass through, along with `Content-Type`, `Content-Encoding`, and `Retry-After` when present; other upstream headers, including redirects' `Location` and cookies, are not forwarded. Compressed bodies are not decompressed by the proxy: all `Content-Encoding` values are preserved in order so clients can decode the unchanged bytes. Proxy responses use `Cache-Control: no-store`. Upstream failures are never retried; the client decides whether to retry. Valid reported usage is accounted as described below; token caps are not yet enforced.

### Streaming and resource bounds

```sh
curl --no-buffer --cacert cert.pem \
  'https://localhost:8443/v1/chat/completions?mode=sse&delay_ms=1000' \
  -H "Authorization: Bearer $JOB_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"model":"mock-mistral","messages":[{"role":"user","content":"Hello"}],"stream":true}'
# First "Hello " event arrives immediately; remaining events arrive later.
# Query controls apply only to the local mock.
```

A syntactically valid bearer first acquires an in-flight permit without waiting, before SQLite admission or body reads. Saturation returns generic JSON `503` with `Retry-After: 1` and `Cache-Control: no-store`, consuming no job request allowance and making no upstream attempt. Consequently saturation takes precedence over unknown-token, inactive-job, exhausted-job, route, and body checks for well-formed bearers. Health and control-plane routes do not use this limit.

The permit spans admission, bounded incoming body parsing, upstream dispatch, response relay, and durable accounting reconciliation. Rejections, failures, clean EOF, timeouts, and client disconnects release capacity after cleanup. Revoke/done/expiry do not cancel admitted work; disconnects and deadlines cancel further body processing and forwarding. Blocking SQLite work cannot be cancelled: its worker owns the permit while queued or running, even after its waiting request disconnects or times out. An abandoned admission may still commit a permanent request charge and record, but no upstream request then starts. Its cancellation guard reconciles that record without releasing capacity early.

Upstream-result delivery does not wait for durable reconciliation: pre-commit `502`/`504` responses, empty EOF, and committed-response EOF/transport errors are delivered independently of SQLite busy waits or mutex contention. The request record may still be `pending` after the client observes completion or failure, and capacity remains charged until its worker finishes. Background finalization transfers the intended outcome and ownership directly to a blocking worker before returning, rather than relying on an intermediate async task that shutdown could cancel before its first poll. Consumers inspecting accounting must wait for terminal records independently of HTTP body termination.

Connect timeout includes TCP connection establishment and the TLS handshake. First-byte timing starts at upstream dispatch and covers both response headers and the first nonempty body chunk, or clean EOF for an empty response; headers alone do not satisfy it. Idle timing bounds each subsequent wait for nonempty upstream bytes or EOF. Overall timing starts before SQLite admission and incoming body reads and never resets as chunks arrive. It also bounds a relay blocked by a slow downstream consumer.

Before committing a response, transport/read failures return sanitized JSON `502`; connect/first-byte/overall timeouts return sanitized JSON `504`. Once the first chunk is available, upstream status and headers are committed. A later read error, idle timeout, or overall timeout aborts the response body at the transport layer: clients must treat it as incomplete, even if they already received `200`. No diagnostic SSE event, synthetic `[DONE]`, replacement JSON, or retry hides an incomplete response.

The relay keeps only one queued chunk plus its current chunk and applies downstream backpressure. Accounting additionally retains at most 1 MiB of ordinary JSON or one bounded SSE event; exceeding that accounting limit does not truncate the forwarded response. Its deadline/cancellation task runs independently of body polling, so a client that stops reading cannot hold an upstream connection indefinitely. Dropping/disconnecting the downstream closes the upstream response promptly, both before the first byte and after response commitment. HTTP chunk boundaries need not match SSE event boundaries; bytes are preserved unchanged.

### Concurrent request quotas

`requests_used` counts admitted attempts, not successful upstream completions. SQLite atomically checks the remaining allowance, increments the counter, creates a pending request record, and returns the admitted snapshot in one Immediate transaction. No upstream work starts before that transaction commits. The final slot also persists `status = exhausted`, without cancelling the request that consumed it. Concurrent admissions cannot spend the same slot twice.

Every admitted attempt retains its charge: forbidden routes, missing configured upstream keys, malformed/unsupported/oversized bodies, body-read failures, upstream HTTP errors, connect/response-read failures, cancellation, and restart after admission. Invalid credentials, inactive/expired jobs, quota denials, and failed admission transactions do not increment the counter or create request records. This conservative attempted-request policy avoids refunds when forwarding or billing is uncertain; a client retry needs another slot. Completion reconciles token accounting but never refunds request allowance.

An exhausted token receives `429` with `{"error":{"message":"Request quota exhausted"}}` and `Cache-Control: no-store`, before route checks or body reads. It receives neither `Retry-After` nor `WWW-Authenticate`: waiting cannot restore that job's allowance. Issue a new job through the control plane if more work is authorized. The first terminal state still wins: exhaustion is retained after TTL expiry/restart and makes `/done` and `/revoke` return `409`; a job already done, revoked, or expired remains `401`. Neither lifecycle transitions nor request completion reset or refund the counter.

### Token usage accounting and recovery

Schema version 2 preserves existing jobs and counters, adds `jobs.tokens_reserved`, and creates `request_records`. Legacy requests are not reconstructed. Each new admitted attempt has a durable integer `request_id`, job ID, admission/finish epoch-second timestamps, terminal outcome, reservation amount, and nullable prompt/completion/total token counts. Bodies, prompts, bearer tokens, and upstream credentials are not copied into these records.

For `"stream": true`, RustGate sets `stream_options.include_usage = true`, overriding client `false` while preserving other option fields. Omitted options become an object; a supplied non-object receives `400` after admission. This requests the final cumulative usage chunk from compatible providers; it does not guarantee that they return usage.

Accounting requires both the response envelope and its `usage` report to be JSON objects, with named `prompt_tokens`, `completion_tokens`, and `total_tokens` fields. Positional arrays at either level are malformed and remain unknown. All three counts must be nonnegative signed 64-bit integers, and prompt plus completion must equal total without overflow. Ordinary JSON (`application/json` or `application/*+json`) requires clean upstream EOF and a complete body no larger than 1 MiB. SSE (`text/event-stream`) is parsed incrementally across arbitrary transport boundaries, including multiline data and CR/LF/CRLF. Only blank-line-terminated events count. Malformed or usage-null chunks, comments, and `[DONE]` do not add tokens or replace a preceding valid report; the last valid cumulative report is counted once, not summed per chunk. A complete SSE usage report remains known if the connection subsequently fails.

Missing, malformed, partial, unsupported-media-type, non-identity-encoded, or oversized ordinary JSON usage is **unknown**, stored as SQL `NULL` in all three usage columns. SSE events/lines exceeding 64 KiB are discarded for accounting until the next event boundary; later valid reports can still be counted. These limits never change forwarded bytes. Known zero is stored as integer zero. Local rejections and cancellations before dispatch have known zero usage; after dispatch, transport failures, timeouts, and disconnects without a complete usage report remain unknown.

Terminal outcomes are `completed`, `rejected`, `upstream_error`, `timeout`, `disconnected`, `cancelled`, and `interrupted`; `pending` denotes admitted unfinished work. Cancellation before response relay, including an overall deadline that drops the handler, records `cancelled`. Finalization atomically persists the first terminal outcome, adds known total usage to `jobs.tokens_used`, and releases that request's reservation exactly once. Repeated/concurrent finalization cannot double-charge or double-release, and never changes terminal job status. Response-stream termination is independent of this persistence step; known usage and the in-flight permit remain owned by reconciliation while it waits for SQLite. If persistence fails, a sanitized diagnostic is emitted and the pending row remains available for startup recovery.

Startup, under exclusive ownership, reconciles every pending request as `interrupted` with unknown usage and releases its reservation in the migration transaction. Repeated restart does not change already finalized records or counters. Request charges remain spent. Usage observed only in memory before a crash cannot be reconstructed.

`tokens_used` is the sum of known reported totals plus preserved legacy accounting, **not** proof of zero spend for unknown requests. Step 8 creates zero-sized reservations; step 9 will atomically reserve conservative budgets, enforce token caps, and define how unknown usage consumes budget. This accounting release is not a hard-cap guarantee.

```sql
SELECT request_id, job_id, outcome, prompt_tokens, completion_tokens, total_tokens
FROM request_records ORDER BY request_id;
SELECT job_id, requests_used, tokens_used, tokens_reserved FROM jobs;
SELECT job_id, count(*) AS unknown_requests
FROM request_records WHERE outcome != 'pending' AND total_tokens IS NULL GROUP BY job_id;
```


### Local mock upstream

`mock-upstream` binds `127.0.0.1:9090` by default; override with `RUSTGATE_MOCK_LISTEN_ADDR` (loopback IP socket address only). It also exposes `GET /healthz`. Port `0` selects an ephemeral port; both executables print their actual bound address.

Send JSON to `POST /v1/chat/completions`. The fixed assistant reply is `Hello from mock.`, with usage of 5 prompt tokens, 3 completion tokens, and 8 total tokens.

| Query control | Behavior |
| --- | --- |
| `mode` omitted | SSE when the body contains literal `"stream": true`; otherwise immediate JSON |
| `mode=normal` | Force immediate JSON, even for a streaming request |
| `mode=delayed` | JSON completion after `delay_ms` |
| `mode=sse` | Immediate first SSE chunk, second chunk after `delay_ms`, then a usage chunk and `[DONE]` |
| `mode=failure` | HTTP 503 with a structured error and `Retry-After: 1` |
| `delay_ms` | Default 100; accepted range 0–10000 |

Explicit modes override the body for deterministic fixture scenarios. Ordinary OpenAI-compatible clients need no query parameters: their `stream` field selects the response format. This also allows streaming chat and non-streaming auxiliary calls to share the mock without forcing SSE for every call. Each SSE reply includes a choice with `finish_reason: "stop"` before its usage-only chunk and `[DONE]`.

```sh
curl http://127.0.0.1:9090/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model":"mock-mistral","messages":[{"role":"user","content":"Hello"}]}'

curl -N 'http://127.0.0.1:9090/v1/chat/completions?mode=sse&delay_ms=500' \
  -H 'Content-Type: application/json' -d '{}'
```

`GET /__mock/requests` returns an array of captured requests with `method`, `path` (without query), `authorization` (or `null`), and parsed JSON `body`. `DELETE /__mock/requests` clears it and returns 204. Captures are in memory, isolated per fixture, and retain only the newest 100 requests. JSON request bodies are limited to 1 MiB; malformed JSON and invalid mode/delay controls are rejected without capture. Valid requests are captured before waiting or returning a controlled failure.

**The capture API deliberately exposes request bodies and credentials without authentication.** Use fake credentials only; nothing captured is written to logs or disk. This fixture is not part of the production service.

### Testing with OpenCode

Start the mock and HTTPS service and issue a job as above. Use only the fake upstream credential with the mock. Before changing to a disposable OpenCode working directory, export the issued job token and the absolute certificate path:

```sh
export RG_JOB_TOKEN="$JOB_TOKEN"
export NODE_EXTRA_CA_CERTS="$PWD/cert.pem"
```

In that working directory, configure `opencode.json`:

```json
{
  "$schema": "https://opencode.ai/config.json",
  "enabled_providers": ["rustgate"],
  "model": "rustgate/mock-mistral",
  "small_model": "rustgate/mock-mistral",
  "provider": {
    "rustgate": {
      "npm": "@ai-sdk/openai-compatible",
      "name": "RustGate local",
      "options": {
        "baseURL": "https://localhost:8443/v1",
        "apiKey": "{env:RG_JOB_TOKEN}"
      },
      "models": {
        "mock-mistral": {
          "name": "RustGate streaming mock",
          "limit": {"context": 32768, "output": 4096}
        }
      }
    }
  }
}
```

Run `opencode run --model rustgate/mock-mistral "Say hello. Do not use tools."`, or launch the TUI with `opencode --model rustgate/mock-mistral`. The expected reply is `Hello from mock.`. OpenCode 1.19.4 was exercised end to end against RustGate over HTTPS: it completed with finish reason `stop`, without query options. Provider-specific query handling is not needed for this setup. The default mock pause is only 100 ms; use curl with `?delay_ms=3000` and `"stream": true` to make incremental arrival obvious. The mock is not a coding model or tool-calling emulator.

If OpenCode reports `OpenAI Chat stream ended without finish_reason`, first rebuild/restart the mock and remove any forced `mode=normal` or `mode=sse` query option. Before this compatibility fix, a request with `"stream": true` but no mode query received JSON; the corrected mock selects SSE from the body. Keep the proxy's real stream errors visible rather than synthesizing a finish reason. OpenCode may retry failed generations; RustGate itself does not retry, and each admitted client retry consumes another request slot.


### Checks

```sh
cargo test --all-targets
cargo clippy --all-targets -- -D warnings
```
