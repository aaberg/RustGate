# Model Proxy Service — Specification
## Purpose

A stateless HTTP reverse proxy that sits between build containers (running AI agents) and the upstream LLM API (Mistral). It ensures the real upstream API key never enters a build container, enforces per-job quotas, provides observability, and injects authoritative system prompts.
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
* Request body: { "job_id": "...", "tenant_id": "...", "upstream_key_id": "...", "ttl_seconds": 1800, "max_requests": 100, "max_tokens_total": 2_000_000 }
* The proxy generates the job's bearer token itself (random 32 bytes, crypto-secure RNG) and returns it in the issuance response; the proxy stores only its hash. The builder receives the token over the mutually authenticated channel and injects it into the build container.
  * Decision: the proxy is the token generator, not the builder. Rationale: the proxy owns the data-plane auth domain, token format and entropy requirements are enforced in one place, and the builder never needs logic for token generation. The trade-off is that the proxy momentarily sees the raw token at issuance time (before hashing) — accepted, since the proxy is trusted infrastructure. To keep the builder from having to trust the proxy's channel alone, issuance responses are sent only over mTLS or the shared internal token.
  * To mitigate token leakage on the proxy side, the raw token exists only in memory of the issuance handler for the duration of the response; it is never logged, never persisted, and never appears in metrics.
* Response: { "job_id": "...", "token": "...", "expires_at": "...", "proxy_url": "https://proxy.internal:8443/v1" }

2. POST /v1/jobs/{job_id}/revoke — immediate kill switch (builder service, or an admin)

* Marks the job revoked. Any in-flight proxied request is completed; any new request gets 401.

3. POST /v1/jobs/{job_id}/done — job finished normally

* Marks done, releases the quota reservation, closes the log entry.

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
* System prompt injection: if the request's messages don't already start with the authoritative job system prompt (identified by a sentinel string), prepend it. The system prompt is fetched per-job: the builder includes the job's "product system prompt" (framework rules, integration library, forbidden actions) when creating the job, and the proxy stores and injects it. This makes the user-facing prompt clearly separated from the operator's rules, and the operator's prompt can't be deleted by the agent. Details for the agent implementing this: inject as the first system message; if the container sends its own system message, keep it after the injected one; mark the injected one with a header field your harness recognizes.

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

* Listen addr + TLS cert paths; upstream base URL; path allowlist; default/max TTL, request cap, token cap; secret-scan regex list; system-prompt sentinel string; secrets source for the key pool; SQLite path.

## Testing requirements (give these to the agent as acceptance criteria)

 * Token issuance → proxied call → expiry → rejection flow end-to-end.
 * Revoke kills in-flight-then-new requests immediately.
 * Quota exhaustion: request cap and token cap both enforced under concurrency (spawn parallel requests; assert the cap holds).
 * Secret-scan: a request body containing a fake sk- key is blocked; a benign body is not.
 * System-prompt injection: request without the operator system message gets it prepended; with it, no duplicate.
 * Streaming: SSE response streams incrementally end-to-end.
 * Path allowlist rejects /v1/admin style paths.

## Decisions (implementation addendum)

Decisions made during implementation review, binding for v1:

### 1. Token accounting for streams
Three-tier, in priority order:
1. Upstream-reported usage (the adapter injects `stream_options.include_usage` on streamed chats for OpenAI-compatible upstreams, and sniffs the final SSE `data:` chunk for a `usage` object).
2. If a stream ends without usage (disconnect, mid-stream error, idle timeout): estimate `request_bytes/4 + response_bytes/4 + 16` and set `tokens_estimated = 1` on the log row. A cost guard must not under-count.
3. Never count nothing.

The token cap is enforced by an **atomic reservation** before forwarding (`tokens_reserved += est WHERE tokens_used + tokens_reserved + est <= max_tokens_total`), reconciled to actual usage on completion. Estimates use the *forwarded* body size (injected operator prompt included) and prefer the request's declared completion limit (`max_completion_tokens`, then legacy `max_tokens`; saturating arithmetic), applying a conservative default completion allowance when neither is declared. Concurrent calls can no longer spend the same remaining budget; the reservation is provisional, so reconciliation reactivates a job that was marked exhausted by an over-large estimate when actual usage leaves budget.

### 2. Reservation semantics
- Requests are reserved atomically before forwarding via `pending_requests` (`UPDATE ... WHERE requests_used + pending_requests < max_requests`); the reservation converts to `requests_used` when upstream response headers arrive (the call burned). Pending and consumed are tracked separately so a connect-failure refund cannot strand the job terminally exhausted with zero burned calls; refunds reactivate jobs exhausted only by pending pressure.
- Tokens use an atomic reservation (see 1), not just a snapshot gate.
- Reservations are released only for failures proven to precede forwarding (connect/builder-phase errors). Ambiguous transport failures (body possibly accepted upstream) consume the slot — a burned upstream call is a burned call.

### 3. Control-plane auth
`POST /v1/jobs`, `/revoke`, `/done` all require the same control-plane credential: a shared static bearer token (constant-time compared) or mTLS-terminated connection. Under the shared-token model, `/revoke` additionally verifies `tenant_id` in the body matches the job's tenant, so one tenant's builder cannot revoke another's job. A distinct admin credential (revoking any tenant's job) is a config-time role decision deferred to v2; v1 treats the control token as the admin credential.

### 4. Secrets file format
One entry per line: `key_id = { "provider": "...", "key": "..." }` (inline JSON) or compact `key_id = provider:sk-...`. `#` comments. Startup warns if the file is group/world readable; expected mode 0600. Env fallback: `RUSTGATE_KEY_<ID>=provider:key` for dev. Issuance referencing an unknown `upstream_key_id` fails loudly (400).

### 5. Injection scope
System-prompt injection applies only to `/v1/chat/completions` bodies with a `messages` array. `/v1/embeddings` and `/v1/models` are forwarded untouched (after auth/quota/secret-scan). The sentinel check is a substring match on any system message; the injected prompt is inserted as the first system message, before any container-supplied system message.

### 6. Status transitions
`revoked`, `done`, `expired`, `exhausted` are all terminal; none reversible. Re-opening requires a new job_id (same tenant/key bindings). Expiry is checked lazily at auth time plus a 30s periodic sweep. Quota exhaustion returns **429** consistently (including subsequent requests against an exhausted job); revoked/expired/done return 401.

Exhaustion by reservation is *provisional*: any refund or reconciliation re-checks both quotas and reactivates jobs that still have headroom (so a fresh job sent an oversized `max_tokens` is rejected, not exhausted, and connect-failure refunds can't strand a job with zero consumed calls). `revoke`/`done` are accepted from the `exhausted` state so cancellation always wins over provisional exhaustion. On startup, orphaned reservations from a previous process are settled (released) and affected jobs re-evaluated. Estimates are per-choice (`n` multiplies the declared completion limit) and input-only for `/v1/embeddings`; the pre-flight gate and all refunds are cancellation-safe (drop guards + spawned finalizers).

### 7. Per-tenant risk score
Deferred. The prompt-injection marker scan logs an advisory flag (`policy_verdict = allowed_injection_flag`) per request with tenant_id — that is the risk-score input. A score can be computed later from `request_log` with SQL, no schema change.

### 8. Provider-agnostic adapter seam
The client-facing protocol is fixed at the OpenAI-compatible shape; providers plug in via a `ProviderAdapter` trait (`id`, `forwards`, `auth_header`, `transform_request`, `extract_usage_*`, `is_chat_body`). v1 ships the `OpenAICompatible` adapter (Mistral, Groq, vLLM, OpenRouter, OpenAI, ...). Non-compatible providers (Anthropic) get a translating adapter later — contained, no proxy rewrite. Key pool entries carry `provider`; the jobs table stores `provider_id` explicitly; base URL, allowlist, and secret-scan patterns are per-provider config. No multi-provider routing in v1: one job → one key → one provider.

### 9. Job self-inspection
`GET /v1/job/me` (job-token authenticated) returns status and quota usage, so harnesses can surface remaining budget. Exposed on the data plane because it leaks nothing the container doesn't already hold.

### 10. Stack
axum 0.8 + tokio, reqwest (rustls) upstream client, rusqlite (bundled SQLite, WAL, synchronous = NORMAL, busy_timeout 5s, single connection behind a mutex via spawn_blocking-compatible access), tracing JSON logs, prometheus metrics. TLS via rustls when cert paths are configured; plain-HTTP dev mode requires explicit `allow_insecure`.

## Implementation map

- `src/config.rs` — env/config-file loading, per-provider settings, quota defaults/caps
- `src/secrets.rs` — key pool loading (secrets file / env), file-format above
- `src/store.rs` — SQLite schema (jobs, request_log), atomic quota reservation, status transitions, sweeps
- `src/adapters.rs` — `ProviderAdapter` trait, `OpenAICompatible` adapter, registry
- `src/policy.rs` — size cap, secret scan, advisory injection-marker scan, system-prompt injection with sentinel
- `src/proxy.rs` — data-plane handler: auth → state → allowlist → policy → quotas → forward; streaming/buffered paths, token reconciliation
- `src/stream_util.rs` — pass-through SSE stream: immediate flush, usage sniffing, byte counting, idle timeout, finalize-once
- `src/control.rs` — job issue/revoke/done, control-plane auth
- `src/router.rs` — routes: /healthz, /metrics, /v1/jobs*, /v1/job/me, data-plane fallback
- `src/usage.rs` — token estimation constants and pre-flight estimate
- `src/metrics.rs` — Prometheus registry: requests by verdict, blocked by reason, tokens by key/provider, in-flight, upstream errors
- `src/error.rs` — API error → status mapping; generic data-plane messages
- `tests/integration.rs` — the acceptance criteria, end-to-end against a mock upstream
