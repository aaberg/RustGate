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
