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
* max_tokens_total, tokens_used (known usage), tokens_unreported (conservative uncertain consumption), tokens_reserved (unfinished work)
* status: active | expired | revoked | exhausted | done
* revoked_at

### request_records

One durable row per admitted attempt, including locally rejected attempts: request_id, job_id, admitted_at, finished_at, outcome, reserved_tokens, nullable prompt_tokens/completion_tokens/total_tokens, policy_verdict, block_reason, and prompt_injection. No bodies, raw paths, query strings, headers, or client model strings are recorded. See the accounting and redaction contracts below.

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

4. POST /v1/chat/completions, POST /v1/embeddings, GET /v1/models — proxy endpoints (called by build containers)

* Bearer token = the per-job token. Reject anything that doesn't validate against an active job (hash match, not expired, not revoked/exhausted). On rejection return 401/403 with a generic message (no detail leakage).
* On success, forward the request to the upstream, transparently rewriting:
  * Authorization header → the real upstream key for this job's upstream_key_id.
  * System prompt injection (see Policy section).
* Response is streamed back to the container (SSE streaming pass-through must work — models stream; don't buffer whole responses). Count tokens from upstream usage fields when present.

## Policy layer (checked per request, before forwarding)

Order of checks on every proxied request:

* AuthN: valid job token → hashed lookup in jobs.
* AuthZ/state: job is active; token not expired; not revoked.
* Quotas: atomically require remaining request and token allowance, reserve available tokens, and permanently increment requests_used at admission before route/body checks. The final permitted request marks the job exhausted; later attempts return 429. Admitted rejections and upstream failures retain their request charge. Before forwarding, narrow the reservation to the endpoint's conservative bound: transformed prompt plus constrained generation for chat, input-only for embeddings, or zero for model listing. Only final known usage releases unused tokens; unknown usage or nonfinal snapshots conservatively consume the reservation.
* Method-and-path allowlist: only exact POST /v1/chat/completions, POST /v1/embeddings, and GET /v1/models are forwardable. Anything else → 403. This prevents a jailbroken agent from using admin endpoints or other products under the same upstream account.
* Body policy checks (fast, local, no LLM):
  * Request size cap (e.g. 1 MB) — also your cost guard.
  * Configurable local regex heuristics scan canonical serialized client JSON and decoded string values/object keys, including metadata. Default patterns flag common `sk-` key shapes and long base64-like runs; they do not prove entropy or cover every secret. A match blocks with `403` and persists only `block_reason = possible_secret`, never the matching text or regex. JSON escapes cannot hide matching decoded text.
  * Prompt-injection marker scan is advisory only: default case-insensitive patterns flag "reveal your system prompt", "print your environment", and "include the API key". Persist a boolean flag, never block solely for this match. This is a tripwire, not a wall; risk scoring and escalation remain deferred.
* System prompt injection (step 5): prepend a fresh system message containing the persisted authoritative `system_prompt` verbatim. Remove every client message whose `role` is exactly `"system"` and whose string `content` exactly equals that prompt; preserve all other messages in order. Never trust a client-supplied sentinel, name, or header as proof that the authoritative prompt is present. The builder supplies the prompt at issuance; a container cannot replace the persisted prompt.

* Upstream key pool

  * The proxy holds the real upstream API keys, loaded at startup from a secrets source (env vars are acceptable here, since the proxy is trusted infrastructure — but prefer a mounted secrets file or Vault later).
  * Keys are named/selected per job (upstream_key_id) so you can put per-key spend limits at Mistral as an outer ring, and rotate by adding a new key id and migrating job issuance.
  * Expose cumulative per-key admitted requests, known tokens, unreported tokens, and outstanding reservations. No monetary spend reporting is implemented: there is no explicit pricing model, and token counts do not establish currency cost.

## Streaming and timeouts

* Forward upstream response bytes incrementally, including SSE; never buffer a whole response or synthesize completion events. Chat request JSON remains bounded and buffered for authoritative prompt injection.
* Configurable upstream connect timeout defaults to 5 s, first-byte timeout to 60 s, overall request timeout to 10 min, and idle stream timeout to 120 s.
* Bound requests in flight per service process (default 64); return 503 with `Retry-After: 1` when saturated, without queueing or charging request allowance.

## Observability

* Structured JSON request events contain fixed route/method categories, response status, duration, accounting and redacted policy metadata. Raw client `model` text is deliberately represented as `null`, never copied into logs or labels. Operator job/tenant/key IDs are intentional metadata and must not contain secrets.
* Authenticated `GET /metrics` exposes Prometheus process HTTP/latency/upstream counters, in-flight capacity, and durable per-job, per-tenant, per-key, outcome and block-reason accounting. Authenticated `GET /v1/usage` exposes a consistent SQLite JSON snapshot. Both require the existing control-plane bearer, not job credentials.
* `GET /healthz` is unauthenticated and reports liveness only, not upstream reachability or token availability. All endpoints remain on the same private HTTPS listener; never publish monitoring endpoints.

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

## Implemented service: steps 1 through 12

The service implements HTTPS, configuration, health, graceful shutdown, authenticated job issuance, SQLite persistence, job-authenticated chat proxying with incremental SSE responses, embeddings and model listing, durable expiry/revoke/done transitions, authoritative prompt injection, concurrent request and token quotas, bounded concurrency/timeouts, durable usage accounting, local body-policy checks with redacted audit decisions, structured operational logs, authenticated usage/Prometheus endpoints, and a hardened single-host deployment baseline. The separate mock remains a loopback-only HTTP fixture. Token-budget enforcement supports text-only chat and text/token-array embeddings against a trusted compatible upstream; its tokenizer, framing, and completion-cap assumptions are explicit below. No monetary pricing or spend reporting is implemented.

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
| `secret_scan_patterns` | Regex string array; defaults detect common `sk-` keys and long base64-like runs |
| `prompt_injection_patterns` | Regex string array; default case-insensitive advisory phrases described below |

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

Expiry is effective when `expires_at <= now`, using UTC epoch seconds sampled after obtaining the SQLite transaction lock. Startup persists `expired` for every overdue active job. During service operation, admission, lifecycle operations, and request finalization materialize expiry for the job they access; there is no background expiry sweep. An untouched overdue row can therefore still read `active` directly from SQLite until access or restart, but its token cannot admit a new call. An overdue active job becomes `expired` before a revoke/done attempt, which returns `409`. Finalization and startup recovery reconcile all counters but materialize effective expiry before introducing token exhaustion; an already persisted terminal state, including earlier exhaustion, still wins.

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

After charged admission, the fixed allowlist permits only exact `POST /v1/chat/completions`, `POST /v1/embeddings`, and `GET /v1/models`. Other methods and paths receive `403`, including `HEAD /v1/models`, trailing slashes, encoded alternatives, and admin paths. The separate health, issuance, and lifecycle routes retain their own contracts and do not consume job request allowance. Authentication, quota admission, and route checks precede body reads.

The body must be a JSON object with a JSON content type and at most 1 MiB, including unknown-length/chunked bodies. `messages` must be an array (an empty array is permitted); missing or non-array values receive `400`. `stream` may be omitted or a literal boolean, including `true` for SSE; other stream types, invalid JSON, non-object bodies, and invalid content types receive `400`, while oversized bodies receive `413`. The text-only budget policy additionally validates content shape, generation limits, and choice count as described below. Other provider-specific chat validation remains upstream's responsibility. JSON is parsed and reserialized before forwarding so duplicate keys cannot give the proxy and upstream different interpretations.

For supported chat requests, prompt injection removes all messages whose `role` is exactly `"system"` and whose string `content` equals the stored prompt, then prepends a new `{"role":"system","content":<stored prompt>}` message. This yields exactly one authoritative system-content match, at index zero, even if the client supplied several copies or placed them later in the conversation. Duplicate messages' client metadata is discarded; it is not attached to the authoritative message.

Equality is exact on decoded strings: no trimming, case folding, Unicode normalization, sentinel detection, or conversion of content-part arrays. The stored prompt is injected verbatim, including whitespace and Unicode. Supported text content, other system messages, developer/user/assistant/tool messages, and their metadata retain their order and values. Other top-level chat fields remain unchanged except enforced generation limits and streaming usage options described below. Names, sentinels, and headers never suppress injection. This guarantees payload placement, not model obedience or resistance to instructions in preserved client messages.

The 1 MiB cap applies to the incoming body before injection; the transformed upstream body may be larger. The local mock has its own independent 1 MiB request limit and can therefore reject a transformed near-limit request with `413`. Prompt transformation applies only to chat, never embeddings, model listing, health, issuance, or lifecycle requests.

The configured upstream base path is a prefix: `https://host/gateway/` targets `/gateway/v1/chat/completions`, `/gateway/v1/embeddings`, or `/gateway/v1/models`. The admitted request's query is preserved, but its host and path cannot select another destination. A fresh upstream request contains the selected key as bearer authorization and, for JSON POST requests, JSON content type, without forwarding client authorization, cookies, proxy credentials, or arbitrary client headers. Redirects and automatic retries are disabled, as are environment-selected HTTP proxies.

Responses are forwarded incrementally, whether SSE, ordinary JSON, or an upstream error. Upstream status and body pass through, along with `Content-Type`, `Content-Encoding`, and `Retry-After` when present; other upstream headers, including redirects' `Location` and cookies, are not forwarded. Compressed bodies are not decompressed by the proxy: all `Content-Encoding` values are preserved in order so clients can decode the unchanged bytes. Proxy responses use `Cache-Control: no-store`. Upstream failures are never retried; the client decides whether to retry. Known usage and unknown-budget charges are reconciled as described below.

### Proxying embeddings and listing models

```sh
curl --cacert cert.pem https://localhost:8443/v1/embeddings \
  -H "Authorization: Bearer $JOB_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"model":"mock-embed","input":["First text","Second text"]}'

curl --cacert cert.pem https://localhost:8443/v1/models \
  -H "Authorization: Bearer $JOB_TOKEN"
```

Embeddings require a JSON object with a nonblank string `model` and an `input` containing one nonempty string, a nonempty array of nonempty strings, one nonempty token array, or a nonempty array of nonempty token arrays. Token IDs must be unsigned 32-bit integers; mixed shapes, negative/fractional/overflowing IDs, empty inputs, and multimodal objects receive `400`. Optional `encoding_format` must be `"float"` or `"base64"`, `dimensions` a positive signed 64-bit integer, and `user` a string. Chat-only options `stream`, `stream_options`, `max_tokens`, `max_completion_tokens`, and `n` are rejected, even if `stream` is false. Other provider fields are preserved; provider-specific validation remains upstream's responsibility.

Non-object bodies for either embeddings or chat receive `400` with `Request body must be a JSON object`. The chat-specific `Chat stream must be a boolean` error applies only to an object-shaped chat request with an invalid `stream` value.

The same JSON content-type requirement, 1 MiB body bound, secret blocking, and advisory audit apply to embeddings. Client JSON values are forwarded unchanged after parsing/reserialization: no system prompt, generation limit, or streaming usage option is inserted, and even a client `messages` field is not treated as chat. The reservation is the compact serialized request's UTF-8 byte length plus 256 tokens and 32 tokens per input (a flat token array is one input). There is no generation reservation. A body exceeding available escrow receives charged `429` with token escrow refunded as known zero. A compatible upstream must bound its embedding tokenization/framing within this estimate and perform no additional billable generation or external-input retrieval. Final known usage releases unused escrow; missing or malformed usage retains the entire reservation as unreported spending.

Model listing requires only the job bearer, not a JSON content type. RustGate neither reads nor forwards any client body, and applies no body scan or chat transformation. It uses the job's configured upstream key, narrows the token reservation to zero before dispatch, and records known zero token usage even on upstream errors, timeouts, disconnections, or unexpected upstream usage reports. It still consumes one admitted request and obeys the same active-job, expiry, request/token-quota, in-flight, and timeout gates. In particular, model listing cannot bypass an exhausted job. Generic startup recovery still applies to requests interrupted by a process crash, including admission escrow not yet narrowed before the crash.

### Body policy and redacted audit

After admission, route/key checks, bounded JSON parsing, and object/stream validation, RustGate scans the original chat or embeddings client JSON before prompt transformation, token-reservation narrowing, or upstream dispatch. Scans include metadata and object keys, not just chat messages or embedding inputs. Both canonical serialized JSON and decoded strings are inspected: JSON escape sequences cannot hide a secret; duplicate keys use the same last-value semantics as the forwarded JSON. The trusted stored operator prompt is not scanned unless the client includes it in its own body. Model listing, issuance, lifecycle, and health routes are outside this policy.

A secret match returns generic JSON `403` with `Cache-Control: no-store`. The admitted attempt remains charged, while its token reservation is released as known zero upstream usage. An advisory match alone does not block or alter client messages. The service persists the scan decision before forwarding; audit persistence failure fails closed with generic `500`. Blocking SQLite audit work retains the in-flight permit through cancellation, like admission and reservation work.

Schema version 4 adds `request_records.policy_verdict` (`allowed` / `blocked`, or `NULL` if not scanned), `block_reason` (`possible_secret` only, otherwise `NULL`), and `prompt_injection` (boolean integer). A blocked body can also have the advisory flag. Decisions are immutable and survive finalization/restart; legacy records remain unscanned. `allowed` means the scan found no configured secret pattern, not that later endpoint/budget validation or upstream processing succeeded. No bodies, matching excerpts, pattern text, or credentials enter these fields or service logs. The already-persisted authoritative job prompt remains sensitive; protect SQLite and backups.

`secret_scan_patterns` and `prompt_injection_patterns` replace their respective defaults; an explicit empty array disables that scan. TOML values are string arrays; `RUSTGATE_SECRET_SCAN_PATTERNS` and `RUSTGATE_PROMPT_INJECTION_PATTERNS` accept JSON string arrays. Regexes compile once during router startup, and malformed or over-limit configurations fail startup without echoing their values. Use Rust `regex` syntax, including inline flags such as `(?i)` for case-insensitive custom patterns. Restart to apply changes. Defaults are heuristics: benign encoded data can match, and secrets outside configured shapes can pass. Add provider-specific patterns for the deployed key formats; do not treat this scanner as a complete exfiltration boundary.

Default secret regexes are `sk-[A-Za-z0-9_-]{20,}` and `[A-Za-z0-9+/]{48,}[0-9+/][A-Za-z0-9+/]{48,}={0,2}`. The latter requires at least 97 base64-alphabet characters with a digit, `+`, or `/` at least 48 characters from either end; it is not an entropy calculation. Advisory defaults are case-insensitive and allow whitespace between the words in the three phrases above. Each pattern searches the serialized body and individual decoded strings/keys independently; separate string values are never concatenated.

Each list permits at most 64 patterns, 4096 bytes per pattern, and 65536 aggregate pattern bytes. Each compiled set has a 1 MiB compiled-size limit, 1 MiB DFA cache limit, and nesting limit 64. These bounds supplement the existing 1 MiB client-body cap.

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

Every admitted attempt retains its charge: forbidden routes, missing configured upstream keys, malformed/unsupported/oversized bodies, body-dependent token-budget rejections, body-read failures, upstream HTTP errors, connect/response-read failures, cancellation, and restart after admission. Invalid credentials, inactive/expired jobs, pre-body quota denials, and failed admission transactions do not increment the counter or create request records. This conservative attempted-request policy avoids refunds when forwarding or billing is uncertain; a client retry needs another slot. Completion reconciles token accounting but never refunds request allowance.

An exhausted token receives `429` with `{"error":{"message":"Request quota exhausted"}}` and `Cache-Control: no-store`, before route checks or body reads. This existing error is also used when persisted exhaustion resulted from token spending. It receives neither `Retry-After` nor `WWW-Authenticate`: waiting cannot restore that job's allowance. Issue a new job through the control plane if more work is authorized. The first terminal state still wins: exhaustion is retained after TTL expiry/restart and makes `/done` and `/revoke` return `409`; a job already done, revoked, or expired remains `401`. Neither lifecycle transitions nor request completion reset or refund the counter.

### Hard token budgets

Available budget is `max_tokens_total - tokens_used - tokens_unreported - tokens_reserved`. Admission atomically escrows **all currently available tokens** in the new pending record, alongside its request charge. This avoids reading an untrusted body before charged admission and prevents competing requests from reserving the same tokens. A request still uploading or validating its body can temporarily reserve all remaining job capacity. Reservations alone do not mark a job exhausted: normal reconciliation may refund them.

After authoritative prompt injection and streaming-option transformation, the conservative prompt bound is the UTF-8 byte length of compact serialized JSON **excluding generation-limit fields**, plus 256 tokens and 32 tokens per message, including the injected system message. All other fields, including tool schemas, metadata, Unicode, and escaped strings, participate in that byte count. The service counts serialization bytes without allocating another request-body buffer.

Supply at most one of `max_tokens` and `max_completion_tokens`; its value must be a positive signed 64-bit integer. Both fields, zero, negative, fractional, null, and overflowing values receive `400`. If neither is supplied, RustGate inserts `max_tokens` with a requested default of 4096. Optional `n` must also be a positive signed 64-bit integer and defaults to one. Each choice must fit at least one generation token. The forwarded generation limit is the smaller of the requested limit and `floor((escrow - prompt_bound) / n)`. Before any upstream dispatch, a second transaction shrinks the pending reservation to `prompt_bound + n * forwarded_limit` and releases only the difference. Narrowing cannot grow a reservation, cannot modify finalized records, and does not recheck lifecycle for already-admitted work.

An active job whose available tokens are held in refundable reservations receives `429`, `{"error":{"message":"Token budget unavailable"}}`, `Cache-Control: no-store`, and `Retry-After: 1`, without reading the body, charging a request, or creating a record. If spending itself has reached the token cap, admission instead persists exhaustion and returns the permanent-exhaustion response without `Retry-After` on the very first denial, including for migrated legacy jobs. A body whose prompt and choices cannot fit receives the temporary-budget response **after charged admission**; it forwards nothing, refunds token escrow as known zero, and retains its request charge. Retrying an unchanged oversized prompt need not succeed; reduce the prompt/choice count or issue a larger authorized budget.

Supported messages are objects with absent/null/string content or arrays of `{"type":"text","text":<string>}` parts. Text tool calls/results and function tools remain supported. Images, audio, video, files, unknown content-part types, provider-managed tools, and web-search options receive `400` before forwarding; byte length cannot conservatively price external or multimodal inputs. If `modalities` is supplied, every entry must be `"text"`.

**Upstream trust boundary:** this bound requires a provider whose text tokenization and chat/tool framing fit within the byte bound plus stated overhead, and whose selected generation-limit field bounds all generated tokens, including hidden reasoning, for every choice. Verify those properties for deployed models; this is not a universal tokenizer guarantee. Do not enable provider extensions or models that violate them. Reports above the reservation are recorded truthfully, not clamped: a provider violating its bound can exceed the real-world cap, which a proxy cannot undo after generation. Subsequent admission still checks actual spending. External network enforcement remains required to prevent bypassing RustGate.

### Token usage accounting and recovery

Schema version 3 introduced `jobs.tokens_unreported` alongside `jobs.tokens_reserved`; version 4 preserves this accounting while adding the redacted policy fields described above. Legacy requests are not reconstructed; pre-enforcement unknown usage with zero-sized reservations cannot be retroactively priced. Each admitted attempt has a durable integer `request_id`, job ID, admission/finish epoch-second timestamps, terminal outcome, reservation amount, and nullable prompt/completion/total token counts. Bodies, prompts, bearer tokens, and upstream credentials are not copied into these records.

For `"stream": true`, RustGate sets `stream_options.include_usage = true`, overriding client `false` while preserving other option fields. Omitted options become an object; a supplied non-object receives `400` after admission. This requests the final cumulative usage chunk from compatible providers; it does not guarantee that they return usage.

Chat accounting requires both the response envelope and its `usage` report to be JSON objects, with named `prompt_tokens`, `completion_tokens`, and `total_tokens` fields. Embeddings also require object envelopes/reports and named `prompt_tokens` and `total_tokens`, but permit absent `completion_tokens` as zero; if supplied, it must be integer zero, not null or nonzero. Positional arrays at either level are malformed and remain unknown. All counts must be nonnegative signed 64-bit integers, and prompt plus completion must equal total without overflow. Model listing ignores upstream usage and records zero. Ordinary JSON (`application/json` or `application/*+json`) requires clean upstream EOF and a complete body no larger than 1 MiB. SSE (`text/event-stream`) is parsed incrementally across arbitrary transport boundaries, including multiline data and CR/LF/CRLF. Only blank-line-terminated events count. The last valid cumulative report is counted once, not summed per chunk; later incomplete or malformed reports never erase a known snapshot. **Known is not necessarily final:** an interrupted stream preserves its known snapshot but cannot refund the uncertain reservation remainder. At clean EOF, SSE usage is final only if the latest meaningful complete data event is a valid usage report, with no partial or discarded event afterward. Comments and `[DONE]` may follow a valid report; other later data, including null/malformed usage or additional choices, makes that snapshot nonfinal until a new valid report arrives.

For chat and embeddings, missing, malformed, partial, unsupported-media-type, non-identity-encoded, or oversized ordinary JSON usage is **unknown**, stored as SQL `NULL` in all three usage columns. SSE events/lines exceeding 64 KiB are discarded for accounting until the next event boundary; later valid reports can still be counted. These limits never change forwarded bytes. Known zero is stored as integer zero. Local rejections and cancellations before dispatch have known zero usage; after dispatch, transport failures, timeouts, and disconnects without a complete usage report remain unknown. Model listing remains known zero independently of its response.

Each parsed upstream chunk synchronously copies the latest complete SSE usage snapshot into the cancellation guard before the relay can await again. Runtime cancellation therefore records known usage even when the normal relay finalization path never runs. Later complete reports replace earlier cumulative snapshots rather than adding to them; an incomplete report never replaces a complete one. Ordinary JSON without clean EOF remains unknown. Reconciliation normally runs on a blocking worker. If runtime teardown discards its queued closure after the blocking pool closes, dropping the finalizer reconciles synchronously during teardown instead of losing the report; this can wait for SQLite cleanup but does not delay response delivery during normal operation. A process crash that skips destructors still has the unknown-usage recovery behavior described below.

Terminal outcomes are `completed`, `rejected`, `upstream_error`, `timeout`, `disconnected`, `cancelled`, and `interrupted`; `pending` denotes admitted unfinished work. Cancellation before response relay, including an overall deadline that drops the handler, records `cancelled`. Finalization atomically persists the first terminal outcome and releases that request's reservation exactly once. Known totals always add to `jobs.tokens_used`. A final known report refunds unused escrow; a nonfinal known snapshot instead adds `max(0, reserved_tokens - total_tokens)` to `jobs.tokens_unreported`. Entirely unknown usage adds the full reservation to `tokens_unreported` and leaves report columns `NULL`. Thus interrupted work retains at least its full reserved charge without discarding known counts or counting them twice. Finalization checks effective expiry before introducing exhaustion; known plus unreported spending reaching the cap otherwise exhausts an active job without changing earlier terminal states or cancelling admitted work. Repeated/concurrent finalization cannot double-charge or double-release. Response-stream termination is independent of persistence; usage, finality, and the in-flight permit remain owned by reconciliation while it waits for SQLite. If persistence fails, a sanitized diagnostic is emitted and the pending row remains available for startup recovery.

Startup, under exclusive ownership, reconciles every pending request as `interrupted` with unknown usage, moving its reservation into unreported spending in the migration transaction. Effective expiry takes precedence over newly introduced exhaustion, without discarding the retained charge. Even a crash before dispatch can consume its entire admission escrow: after a crash, dispatch cannot be ruled out durably. Repeated restart does not change finalized records or counters. Request charges remain spent. Usage observed only in memory before a crash cannot be reconstructed.

`tokens_used` remains the sum of known reported totals, including nonfinal snapshots, plus preserved legacy accounting, not fabricated estimates. `tokens_unreported` is conservative budget consumption for unknown reports and uncertain snapshot remainders; `tokens_reserved` covers unfinished work. Unknown usage is never treated as free or reported as known zero. Local rejection/cancellation known to precede dispatch refunds its token reservation, but never its request charge. Revoke, done, expiry, and request exhaustion retain in-flight reservations until ordinary reconciliation. Cancellation or timeout during either blocking admission or reservation narrowing retains capacity until the worker finishes; abandoned narrowing cannot dispatch upstream and reconciles its remaining escrow as known zero exactly once.

```sql
SELECT request_id, job_id, outcome, prompt_tokens, completion_tokens, total_tokens
FROM request_records ORDER BY request_id;
SELECT job_id, requests_used, tokens_used, tokens_unreported, tokens_reserved FROM jobs;
SELECT job_id, count(*) AS unknown_requests
FROM request_records WHERE outcome != 'pending' AND total_tokens IS NULL GROUP BY job_id;
```

### Operational endpoints and metric contract

`GET /metrics` and `GET /v1/usage` use the same control-plane bearer as issuance and lifecycle administration. Missing/invalid authorization and job bearers receive `401`; requests do not consume job allowance. Responses must not be cached. Both endpoints are available only on the normal HTTPS listener, not on a separate public monitoring port.

`/v1/usage` returns an object with `keys`, `jobs`, `outcomes`, and `blocked` arrays, read in one SQLite transaction:

| Array | Fields | Meaning |
| --- | --- | --- |
| `keys` | `upstream_key_id`, `requests_used`, `tokens_used`, `tokens_unreported`, `tokens_reserved` | Cumulative per-key admitted attempts, known usage, conservatively retained unknown consumption, and currently unfinished reservations, respectively |
| `jobs` | `job_id`, `tenant_id`, `requests_used` | Durable admitted attempts per job, including terminal jobs |
| `outcomes` | `outcome`, `count` | Request-record counts by durable outcome; a `pending` record is unfinished, not a terminal success |
| `blocked` | `reason`, `count` | Durable blocked-policy decisions by fixed reason |

Key IDs from persisted jobs remain represented even after removal from the configured credential pool. An unused configured key has no persisted usage to aggregate. These are lifetime database totals, not monthly windows. `tokens_used` is known reported usage (including preserved complete nonfinal snapshots), not an estimate; `tokens_unreported` and `tokens_reserved` must remain distinct. There are **no monetary fields, prices, currency totals, or spend-rate claims**. Provider billing must be obtained separately; currency reporting requires an explicit pricing model first.

| Prometheus family | Type / labels | Scope |
| --- | --- | --- |
| `rustgate_http_requests_total` | Counter: `route`, `status_class` | Process lifetime; status classes `1xx`–`5xx` or `cancelled` |
| `rustgate_request_duration_seconds` | Histogram | Process request lifetime, including durable reconciliation for admitted work |
| `rustgate_in_flight_requests` | Gauge | Used admission capacity, including queued/running blocking workers |
| `rustgate_upstream_requests_total` | Counter | Process upstream attempts, without automatic retries |
| `rustgate_upstream_errors_total` | Counter | Process non-success upstream HTTP responses or transport failures, conservatively including timeout/disconnect |
| `rustgate_job_requests_total` | Counter: `job_id`, `tenant_id` | Durable admitted attempts per job |
| `rustgate_tenant_requests_total` | Counter: `tenant_id` | Durable admitted attempts per tenant |
| `rustgate_key_requests_total` | Counter: `key_id` | Durable admitted attempts per upstream key ID |
| `rustgate_key_tokens_total` | Counter: `key_id` | Durable known reported tokens |
| `rustgate_key_tokens_unreported_total` | Counter: `key_id` | Durable conservative unknown consumption |
| `rustgate_key_tokens_reserved` | Gauge: `key_id` | Outstanding reservations, not spent tokens |
| `rustgate_requests_by_outcome` | Gauge: `outcome` | Durable request-record counts, including decreasing `pending` and terminal `interrupted` |
| `rustgate_blocked_requests_total` | Counter: `reason` | Durable block decisions, currently `possible_secret` |

Route labels are fixed categories `health`, `issue`, `revoke`, `done`, `metrics`, `usage`, `chat`, `embeddings`, `models`, or `other`; never raw paths or query strings. Histogram samples have standard `_bucket`, `_sum`, and `_count` suffixes. Process counters reset on restart; durable counters survive restart but can decrease after an older backup restore or offline retention. Account for counter resets and lost history when interpreting `rate()`/`increase()`. A scrape is a consistent durable snapshot, but process counters and SQLite are not one cross-system transaction.

Merge [the authenticated scrape example](deploy/prometheus.yml) into a private Prometheus deployment and install [the alert examples](deploy/rustgate-alerts.yml). Provision `/etc/prometheus/secrets/rustgate-control-token` with the actual control bearer only (no `Bearer ` prefix), readable only by the Prometheus service account and root. Install the issuing CA at `/etc/prometheus/tls/rustgate-ca.pem`, set the real certificate name in `server_name`, and select the correct private target. Never use `insecure_skip_verify` or an unauthenticated/public scrape. This bearer grants full control-plane privileges, not just read-only monitoring; the monitoring host, its configuration, and backups belong inside the control-plane trust boundary. Never use a job bearer for monitoring.

Examples in the alert file cover scrape loss for two minutes; upstream failure ratio above 5% for five minutes with meaningful traffic; more than 48 of the example's 64 admission slots busy for five minutes; growth in unreported tokens; reservations continuously nonzero for fifteen minutes; and possible-secret blocks. Tune thresholds to the deployment. Continuous reservations can be healthy under sustained traffic, and unreported consumption is uncertainty, not proof of provider billing. Also monitor disk capacity/inodes, backup freshness, certificate expiry, and host/network availability with host-level monitoring.

### Structured logs and retention

The service emits JSON process events for startup, listening, shutdown, stopped, and failure using static messages, and one JSON request event per received request when its final ownership is released. For admitted work that includes durable reconciliation; an HTTP response or stream can finish before its event appears. A killed process may not emit its final event; SQLite recovery is authoritative.

Request events use fixed route and method (`GET`, `POST`, `other`) categories, status (nullable when cancelled before headers), `duration_ms`, admitted `request_id`/`job_id`/`tenant_id`/`key_id` metadata, nullable usage counts, `usage_final`, `outcome`, `policy_verdict`, `block_reason`, `prompt_injection`, and `accounting_persisted`. `model` is always `null`: raw model text is client-controlled and could itself carry a secret. Null usage is unknown, not zero. An HTTP status alone does not establish final accounting, and `accounting_persisted = false` requires operator investigation.

**Redaction policy:** never log headers (including authorization), raw URLs/paths/query strings, request or response bodies, raw client model names, bearer/provider credentials, matched secret text, regex patterns, or database error details. This applies equally to service diagnostics, journald exports, ingress/firewall tracing, Prometheus annotations and incident tickets. Do not enable body capture, HTTP debug tracing, shell `set -x`, or curl `-v` while using real credentials. Operator-supplied tenant/job/key IDs are intentional metadata in restricted logs and metrics: they **must never contain secrets or sensitive customer content**. There is no opt-in production body-sampling store.

SQLite retains job rows, authoritative prompts, token hashes, request records, and accounting **indefinitely**; there is no online TTL cleanup, deletion API, or automatic retention job. Job expiry/revoke/done does not erase data. Backups need the same protection. Configure journald/central-log and Prometheus retention separately according to the organization's policy; that does not delete SQLite data.

Per-job and per-tenant labels intentionally grow with retained identities. Budget Prometheus memory/storage and scrape duration accordingly; every scrape also aggregates retained SQLite records. A longer scrape interval reduces frequency, not cardinality. For lower monitoring cardinality, use Prometheus `metric_relabel_configs` to drop the entire `rustgate_job_requests_total` family (and tenant family if necessary); do not merely remove identity labels, which creates colliding samples. Restricted `/v1/usage` remains available for operator inspection.

Retention, when authorized, is an **offline maintenance procedure**, not a running-service SQL job: stop and fully drain the service; take and validate a SQLite backup; define an approved terminal-job cutoff; work on a separate backup-derived database; transactionally delete that cohort's request records before its job rows with foreign keys enabled; keep every row/counter for retained jobs unchanged; run integrity and foreign-key checks; and install the result using the restore procedure below. Never retain request rows without their job or reset active/pending quotas. Deleted jobs lose UUID reservations and history, and aggregate counters decrease; record that accounting discontinuity and retain an access-controlled audit/archive if policy allows. Secure deletion of historical media/backups is a separate storage policy. Test any schema-specific SQL on the copy first; no automatic deletion script is shipped.

### Production deployment and operations

#### Single-host installation and trusted TLS

The baseline is one Linux/systemd host, one dedicated unprivileged `rustgate` account, one persistent SQLite database on a local filesystem with working OS locks, and a private HTTPS listener. No mock, shared/network filesystem, replicas sharing one database, public ingress, or container-distributed SQLite. The service unit's sandbox protects the host filesystem; **it is not an egress firewall**.

1. Build the release `rustgate` binary from the approved source revision on a trusted build host (`cargo build --release --bin rustgate`). Install only that binary, not `mock-upstream`, on the production host. Record the source revision and artifact checksum in your release inventory. Install the `sqlite3` CLI, CA trust prerequisites and systemd using the host's supported package manager.
2. Create the account and install the supplied unit and production configuration from the checkout:

   ```sh
   sudo useradd --system --user-group --home-dir /var/lib/rustgate \
     --no-create-home --shell /usr/sbin/nologin rustgate
   sudo install -o root -g root -m 0755 target/release/rustgate /usr/local/bin/rustgate
   sudo install -d -o root -g rustgate -m 0750 /etc/rustgate /etc/rustgate/tls
   sudo install -o root -g rustgate -m 0640 deploy/rustgate.toml /etc/rustgate/rustgate.toml
   sudo install -o root -g root -m 0644 deploy/rustgate.service /etc/systemd/system/rustgate.service
   ```

   Adapt the nologin path to the host; skip account creation if the dedicated account already exists. systemd creates `/var/lib/rustgate` owned by this account with mode `0700`. Do not put the binary/configuration in that writable directory.
3. Replace `proxy_url`'s example DNS name with the real private name and port. The production TOML initially binds **loopback only**. For remote builders, set `listen_addr` to the host's explicitly selected private/WireGuard IP and allow only trusted builder/control/monitoring sources through the host/network firewall. Do not switch to a wildcard public listener. The same listener serves data, control and monitoring routes, so path-based restrictions, if required, need a separately administered trusted gateway.
4. Obtain a valid certificate chain and key for that exact DNS name from an organizational CA trusted by builders and monitoring clients (or an appropriate public CA for an owned private deployment name). Install the chain as `/etc/rustgate/tls/fullchain.pem` and key as `/etc/rustgate/tls/privkey.pem`, root-owned, group `rustgate`, mode `0640`; keep their directory root-owned `0750`. Distribute the CA through trusted client provisioning. Never deploy the development self-signed example or disable certificate verification.
5. The configured upstream is `https://api.mistral.ai`; use an approved compatible HTTPS provider if changing it. Validate provider certificate trust and the token-budget compatibility assumptions above. There is no application configuration for custom upstream CA bundles or disabling TLS verification; do not assume setting a generic CA environment variable adds unsupported trust.

#### Secret provisioning and startup

`deploy/rustgate.toml` contains no credentials. The unit reads `/etc/rustgate/rustgate.env` using systemd's `EnvironmentFile`, before dropping privileges; the application consumes the supported `RUSTGATE_CONTROL_PLANE_TOKEN` and `RUSTGATE_UPSTREAM_KEYS` environment overrides. This is not shell syntax: omit `export` and shell substitutions. The commented `deploy/rustgate.env.example` is a checklist, not runnable credentials.

Generate the control bearer directly into the root-only file without putting it into command arguments/history:

```sh
sudo sh -c 'umask 077; printf "RUSTGATE_CONTROL_PLANE_TOKEN=%s\n" "$(openssl rand -hex 32)" > /etc/rustgate/rustgate.env'
sudo chown root:root /etc/rustgate/rustgate.env
sudo chmod 0600 /etc/rustgate/rustgate.env
sudoedit /etc/rustgate/rustgate.env
```

Run the generation command only for initial provisioning; rerunning it rotates the control credential. In the root-only editor or trusted secret provisioning tool, add `RUSTGATE_UPSTREAM_KEYS` as a single-quoted JSON object mapping non-secret key IDs to actual provider credentials. Preserve JSON escaping and systemd quoting; credentials must be nonempty visible ASCII without whitespace. Do not add active dummy values. Deliver the control bearer to the trusted builder/control client and monitoring credential file through a secure secret channel, never a job environment. No credential `_FILE` settings, dynamic Vault retrieval, or live reload are implemented. Environment secrets remain visible to root and the service process; exclude the account from interactive/shared workloads and protect process diagnostics/core dumps.

Before starting, complete the external boundary checklist below. Then:

```sh
sudo systemctl daemon-reload
sudo systemctl enable --now rustgate.service
sudo systemctl status rustgate.service
```

The unit uses `NoNewPrivileges`, an empty capability set, strict read-only system paths, protected home/kernel/device namespaces, restricted socket families, and only the managed state directory writable outside a private temporary directory. Port 8443 does not require root. Do not weaken file permissions to troubleshoot startup; inspect the static failure event and check paths/ownership/configuration privately.

On the trusted operator host, provision a mode-`0600` curl configuration at `/etc/rustgate/operator.curl` containing the authorization header with the real control bearer and the trusted CA path using curl's `header` and `cacert` options. Do not include this file in source control. This avoids secrets in process arguments. With the actual private hostname substituted:

```sh
curl --fail --silent --show-error --config /etc/rustgate/operator.curl https://rustgate.internal:8443/metrics
curl --fail --silent --show-error --config /etc/rustgate/operator.curl https://rustgate.internal:8443/v1/usage
curl --fail --silent --show-error --cacert /etc/rustgate/tls/ca.pem https://rustgate.internal:8443/healthz
```

Provision `/etc/rustgate/tls/ca.pem` on that operator host from the trusted CA distribution, not by downloading an unverified server chain. Keep usage output restricted because it contains operator IDs. Health alone is not a readiness/egress test. Missing credentials and job credentials must fail on both operational routes; a private unauthenticated health check is the only exception.

#### Required external network acceptance

Do not connect real build containers until a deployment operator has tested the actual host/container network namespaces and recorded the firewall result. Application tests cannot prove these controls.

* Ingress: only approved build sources can reach the private data listener; only trusted control/monitoring hosts hold the control bearer. Verify the public interface and any load balancer/NAT path cannot reach it. Check both IPv4 and IPv6; a missing IPv6 rule is not an IPv4 restriction.
* Proxy egress: permit only the selected provider's required HTTPS destinations and the designated DNS resolver (and explicitly approved host infrastructure such as time synchronization). Block arbitrary internet/private destinations from the proxy service boundary. Account for DNS resolution, all provider A/AAAA results, address changes and DNS-rebinding implications; maintain allowlists or use a controlled egress gateway. Do not claim a hostname in TOML is a firewall.
* Container boundary: from an actual builder container, prove direct provider access fails over IPv4 **and** IPv6, including direct resolved addresses, alternative DNS resolvers, proxy environment variables and other container/host networking paths. Verify the same legitimate call through RustGate succeeds without exposing the provider key.
* Separate necessary build/repository egress into independently approved paths; it must not provide a general CONNECT/SOCKS/HTTP relay or host-network escape to the upstream. Check container bridge/forward chains, published ports, host-gateway access and any shared upstream container namespace, not just host OUTPUT rules.
* TLS: verify real client and monitoring trust/hostname checks and certificate-expiry alerting. Verify DNS traffic is restricted to approved resolvers rather than allowing arbitrary outbound DNS as an exfiltration channel.

#### Backup and restore

Backups contain authoritative prompts and token hashes and are sensitive even though raw credentials are absent. Store them encrypted/access-controlled, off the service host as policy requires, and monitor backup success/age. Use SQLite's online backup API or the CLI `.backup` command; **never `cp`, tar, rsync, or snapshot only the live main database file**. Journals/WAL can contain committed state not present there.

For a CLI backup to a fresh operator-selected filename:

```sh
sudo install -d -o root -g root -m 0700 /var/backups/rustgate
sudo sqlite3 -readonly /var/lib/rustgate/rustgate.sqlite3 \
  '.timeout 5000' ".backup '/var/backups/rustgate/rustgate-before-upgrade.sqlite3'"
sudo chmod 0600 /var/backups/rustgate/rustgate-before-upgrade.sqlite3
sudo sqlite3 -readonly /var/backups/rustgate/rustgate-before-upgrade.sqlite3 \
  'PRAGMA integrity_check; PRAGMA foreign_key_check;'
```

Use a unique filename for each backup; do not overwrite the last good copy. Expect `ok` and no foreign-key violations, check command failures, and retry contention during a quiet window or after a graceful stop. A live backup is transactionally consistent but can contain pending requests; restoring/startup conservatively marks those interrupted and retains their uncertain token consumption. The service ownership sidecar does not block SQLite-aware readers.

Restore drill and actual restore:

1. Block issuance/data ingress and run `sudo systemctl stop rustgate.service`; wait until it is fully stopped with no process/worker retaining ownership. The service has no forced drain deadline. Investigate SQLite blockers rather than deleting locks or immediately killing it.
2. Keep a validated pre-restore backup and securely quarantine the old main database **and any** `-wal`, `-shm`, or `-journal` companions, only after all owners/readers/writers have stopped. Never apply old SQLite companions to the restored file. Do not remove, rename or replace the persistent `rustgate.sqlite3.lock` sidecar; it is not database contents, does not need backup/restore, and the OS lock—not its existence—indicates ownership.
3. Validate the chosen backup with the checks above, then install it as `/var/lib/rustgate/rustgate.sqlite3`, owned `rustgate:rustgate`, mode `0600`, inside the original `0700` state directory. Keep the database path and lock sidecar stable; never restore through a hard-link alias. The stopped service can reuse its existing sidecar.
4. Start the service; verify private HTTPS health, authenticated usage, preserved terminal jobs/counters and the intended active-job behavior before reopening ingress. Record the rollback point: restoring an older database loses later revocations, issuance and charges, can re-enable previously terminal job credentials, and decreases exported cumulative totals. Revoke/deny potentially restored jobs and rotate relevant provider/control credentials if the incident requires it. An ordinary restart with the same current database does not have this rollback risk.
5. Exercise restoration periodically on an isolated host with blocked provider egress and separately provisioned secrets. Do not start a restored production database next to the live instance against real upstream credentials.

#### Graceful shutdown, upgrade and rotation

`systemctl stop/restart rustgate` sends SIGTERM. The application stops accepting connections and drains admitted work plus accounting workers. `TimeoutStopSec=infinity` and `SendSIGKILL=no` deliberately prevent systemd from forcing an accounting-uncertain exit; normal request deadlines still apply, but a SQLite lock can extend draining beyond them. Inspect and release the external writer/lock holder. If emergency policy requires a forced kill, record that pending reservations will become unreported consumption on restart, not refunded.

For an upgrade: restrict ingress/issuance, take a fresh SQLite-aware backup, stop and drain, install the approved root-owned binary, then start and inspect health, usage, metrics and static process events before reopening. Startup migrations are transactional; a newer unsupported schema fails startup. Never assume a binary downgrade is safe: use a tested compatible binary or the isolated, validated database-restore procedure, accepting and mitigating rollback loss. Keep the previous binary and configuration in a protected release inventory.

All configuration, credentials and TLS material load at startup. There is no SIGHUP/zero-downtime reload; rotate files atomically with their restrictive owners/modes, then drain/restart:

* **Provider keys:** add a fresh key under a new non-secret ID, restart, and issue new jobs against that ID. Existing jobs remain pinned to their persisted ID; there is no live job-migration API. Keep the old ID/credential until its jobs finish/expire or revoke them intentionally. Removing an ID makes subsequent admitted attempts for its jobs fail before upstream dispatch but still consumes a request charge. Already admitted requests drain under the old process configuration. Reusing an existing ID with a new credential switches those jobs on restart and merges historical usage under that ID; prefer new IDs for an auditable rotation.
* **Control bearer:** update the root-only environment file, trusted control client and Prometheus secret through the secret channel, then restart. Only one bearer is accepted; coordinate the short monitoring/control interruption. Existing job bearers are not changed. A leaked control bearer has full administrative scope.
* **TLS:** stage a matching valid chain/key, preserve root ownership and group-read-only access, and restart before expiry. Distribute changed CA trust before switching certificates; test real hostname validation afterward. Do not delete the old trust anchor before clients migrate.
* **Job bearer:** no in-place rotation or token recovery exists. Revoke the old job and issue a new UUID/token through the control API; a lifecycle transition closes future admission but does not cancel already admitted work.

#### Staging acceptance before production

Use an isolated deployment and fake secrets with the loopback fixture, never production credentials in its capture API. Exercise issuance, incremental SSE, known/unknown usage, concurrent quota exhaustion, possible-secret blocking, revocation with admitted work draining, graceful restart and crash recovery. Check authenticated usage and metrics before/after restart; attempt both endpoints with absent/job credentials; inspect log/metric output for sentinel credentials, body/query/header/model markers and regex text. Confirm durable totals survive and process counters reset. Then separately complete the real-network firewall/TLS/backup-restore checks above. These are required operator acceptance steps, **not a claim that this document or the supplied deployment files have already been exercised on your host**.



### Local mock upstream

`mock-upstream` binds `127.0.0.1:9090` by default; override with `RUSTGATE_MOCK_LISTEN_ADDR` (loopback IP socket address only). It also exposes `GET /healthz`. Port `0` selects an ephemeral port; both executables print their actual bound address.

Send JSON to `POST /v1/chat/completions`. The fixed assistant reply is `Hello from mock.`, with usage of 5 prompt tokens, 3 completion tokens, and 8 total tokens.

`POST /v1/embeddings` accepts text or token-array inputs and returns a list with one deterministic three-element embedding per input, with 5 prompt tokens and 5 total tokens, omitting `completion_tokens`. `GET /v1/models` returns `mock-mistral` and `mock-embed` and captures a null body without reading client data. Both endpoints support immediate/default, `mode=normal`, `mode=delayed`, and `mode=failure` controls below; `mode=sse` receives `400` before capture. Their output is fixture data, not real embeddings or model discovery.

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

`GET /__mock/requests` returns an array of captured requests with `method`, `path` (without query), `authorization` (or `null`), and parsed JSON `body` (null for model listing). `DELETE /__mock/requests` clears it and returns 204. Captures are in memory, isolated per fixture, and retain only the newest 100 requests. JSON request bodies are limited to 1 MiB; malformed JSON and invalid mode/delay controls are rejected without capture. Valid requests are captured before waiting or returning a controlled failure.

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


### CI and GitHub releases

`.github/workflows/ci.yml` builds both executables and runs all-target tests in the release profile on every push, pull request, and published GitHub Release, including prereleases. Cargo uses `--locked`; commit `Cargo.lock` updates alongside dependency or package-version changes. The target is Linux x86-64 (`x86_64-unknown-linux-gnu`) on Ubuntu 24.04 with stable Rust. These are glibc-based executables, not static musl builds; use a compatible deployment host.

Every successful run preserves a CI artifact containing only the production `rustgate` executable in `rustgate-v<version>-x86_64-unknown-linux-gnu.tar.gz`, plus `SHA256SUMS`. `mock-upstream` is built and tested but is not distributed. Publishing a GitHub Release additionally uploads those same files to that release after the build, tests, and executable-version smoke check pass. A tag push alone does not upload release assets. The upload job alone receives `contents: write` through GitHub's built-in token; no separate deployment secret is required. Re-running the workflow replaces same-named release assets.

`Cargo.toml`'s `package.version` is the single source of truth. `rustgate --version` (or `-V`) prints that compile-time version and exits successfully without loading configuration, certificates, or the database. Release tags must be exactly `v<package.version>`; CI rejects a mismatch before building or uploading. SemVer prereleases work the same way, for example package version `1.2.0-rc.1` and tag `v1.2.0-rc.1`. The GitHub Release title is descriptive and does not set the version.

Release procedure:

1. Update `package.version` in `Cargo.toml`, run `cargo check --all-targets` to refresh the root package entry in `Cargo.lock`, and commit both files. Wait for CI to pass for that commit.
2. Tag that commit with the matching `v<version>` tag and push the tag. Publish a GitHub Release selecting that tag, or publish a prerelease for a prerelease version. Ensure this workflow has reached the repository's default branch before publishing.
3. Wait for the release workflow to finish, then download the archive and `SHA256SUMS` from that release. Verify before installing:

   ```sh
   sha256sum --check SHA256SUMS
   VERSION=0.1.0 # Set to the downloaded release's version, without the v prefix.
   tar -xzf "rustgate-v${VERSION}-x86_64-unknown-linux-gnu.tar.gz"
   ./rustgate --version
   ```

Keep the source tag/revision and checksum in the release inventory described in the deployment runbook. Release automation attaches binaries to GitHub Releases; it does not restart or modify production hosts.

### Checks

```sh
cargo test --all-targets
cargo clippy --all-targets -- -D warnings
```
