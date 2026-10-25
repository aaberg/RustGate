# Development roadmap

Build one working vertical slice first, then add one enforceable guarantee at a time. `DESIGN.md` remains the specification; each increment below should be an independently reviewable change with observable acceptance criteria.

Use a local mock upstream from the beginning. It should capture forwarded requests and produce controlled responses, delayed SSE chunks, usage reports, and failures. This makes acceptance checks deterministic without spending real API credits.

## Contracts to resolve before implementation

- **Authoritative prompt — issuance contract resolved in step 2:** `system_prompt` is required in the issuance API and persisted job data. Step 5 must never trust a client-supplied sentinel as proof that this authoritative prompt is present; a container could spoof it.
- **Token quota:** checking previously consumed tokens cannot enforce a hard cap under concurrency. Define reservation rules, output limits, and handling for missing usage, disconnects, and restarts.
- **Lifecycle — admission contract resolved in step 4:** revoke, done, and expiry close admission without cancelling admitted work. First terminal state wins; same-target retries are idempotent. Future accounting must retain in-flight reservations until normal reconciliation, including after `/done`; no early refund or usage reset.
- **Routing:** use a method-and-path allowlist. `/v1/models` ordinarily needs `GET`, unlike chat and embeddings.
- **Accounting:** decide whether blocked requests and upstream failures consume request quota. Monthly monetary spend also needs pricing data; token totals alone are insufficient.

## Implementation increments

Each increment delivers demonstrable behavior, not just scaffolding.

| Step | Deliverable | Acceptance check |
| --- | --- | --- |
| **1. Runnable service and mock upstream — complete** | Configuration loading, `/healthz`, graceful shutdown, and a local upstream fixture. | Start the service and call health; drive the fixture through ordinary and delayed responses. |
| **2. Job issuance and persistence — complete** | SQLite migrations, authenticated control plane over TLS, key-pool configuration, and `POST /v1/jobs`. Generate secure tokens; persist only hashes and the job's authoritative prompt. | Issue a job, inspect persisted state, restart the service, and verify the job survives. Reject unauthenticated issuance and unknown key IDs. |
| **3. First complete proxy request — complete** | Support non-streaming chat only: job-token authentication, active-state checks, exact route allowlisting, bounded request bodies, and upstream authorization replacement. Never retry automatically. | Issue a token and send a chat request. The mock sees the configured upstream credential, not the job token. Invalid tokens and forbidden routes never reach it. |
| **4. Complete job lifecycle — complete** | Expiry, revoke, and done transitions, including their interaction with requests already admitted. | Hold an upstream request open, revoke the job, verify that request finishes while subsequent requests fail. Exercise expiry and done across restart. |
| **5. Authoritative prompt injection** | Inject the stored operator prompt using a deterministic rule; preserve permitted client messages without trusting their sentinel claims. Apply only to supported chat requests. | Inspect the upstream payload for absent, spoofed, and duplicated operator-message cases. Verify exactly the intended authoritative content is injected. |
| **6. Concurrent request quotas** | Atomic request admission and the agreed charging/reconciliation rules. | Race more requests than the remaining allowance; upstream receives no more requests than permitted. Exercise rejection and upstream-failure paths. |
| **7. Streaming and resource bounds** | Incremental SSE forwarding, client-disconnect handling, connect/first-byte/idle/overall timeouts, and an in-flight limit with `503` retry guidance. | Observe the first SSE chunk before the response completes. Exercise stalls, disconnects, saturation, and upstream failures without hidden retries. |
| **8. Token usage accounting** | Extract usage from ordinary and streaming responses; persist request records and reconcile reservations on every terminal path. Keep unknown usage distinguishable from zero. | Reconcile known mock usage correctly; exercise missing usage, broken streams, and restart with outstanding requests. |
| **9. Hard token-budget enforcement** | Reserve a conservative request budget atomically before forwarding, constrain generation appropriately, and reconcile actual usage. Implement the agreed missing-usage policy. | Concurrent requests cannot admit more reserved work than the job budget allows. Unused reservations are released without granting budget twice. |
| **10. Body-policy checks** | Configurable secret-pattern blocking, advisory prompt-injection flags, and redacted audit reasons. | Fake secrets are blocked before forwarding; benign bodies pass; advisory matches are recorded without blocking. Bodies and credentials remain absent from logs. |
| **11. Remaining endpoint support** | Add embeddings and model listing individually, with endpoint-specific validation, accounting, and allowed methods. | Exercise each endpoint end-to-end. Chat prompt transformation never touches embeddings or model-list requests. |
| **12. Operational readiness** | Complete structured logs, Prometheus metrics, per-key usage aggregates, deployment configuration, and operator documentation. Implement spend reporting only with an explicit pricing model. | Run a staging scenario covering issuance, streaming, exhaustion, revocation, and restart; verify the resulting logs and metrics contain no secrets or request bodies. |

For every increment, update the relevant specification details and keep regression tests for consumer-visible boundaries and failure cases.

Steps 1 through 4 are implemented as the HTTPS-only `rustgate` service and separate loopback-only HTTP `mock-upstream` executable. See [the implemented service contract](DESIGN.md#implemented-service-steps-1-through-4) for required TLS/security configuration, issuance, lifecycle and proxy commands, persistence, fixture controls, and capture-API safety limits.

Verification: 46 regression tests pass; Clippy passes with warnings denied; both binaries build. Step 4 live HTTPS checks hold upstream calls open during revoke and done: the held calls complete with `200`, later calls receive `401`, and each scenario has exactly one upstream capture. Same-target retries return `204`; conflicting transitions return `409`. After restart, revoked/done tokens remain rejected, overdue active jobs persist as expired and receive `401`, and an unexpired active token still completes. Running-service TTL expiry persists on admission. Both binaries exit cleanly on SIGTERM, and process logs contain neither control nor upstream credentials. New regressions additionally cover exact expiry boundaries, authentication before path/body extraction, terminal-state durability and metadata preservation, competing transitions across independent stores, generic storage failures and rollback, and lifecycle changes while admitted request bodies are still pending. Prompt injection, quotas, and streaming remain later increments.

## Release checkpoints

- **After step 5:** useful local integration slice; not ready for build containers because quotas and streaming guarantees are incomplete.
- **After step 9:** core authenticated, streaming, quota-controlled proxy.
- **After step 12:** candidate for deployment against the complete specification.

Before connecting real build containers, verify the external network boundary: containers cannot reach the upstream directly, and the proxy's configured egress restrictions actually hold. Application tests cannot prove those firewall guarantees.

## Deferred extensions

Keep Vault integration, optional debugging-body storage, risk scoring, and escalation of advisory scans out of the initial roadmap. They are separable extensions, not prerequisites for the core proxy.
