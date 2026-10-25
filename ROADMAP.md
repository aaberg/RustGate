# Development roadmap

Build one working vertical slice first, then add one enforceable guarantee at a time. `DESIGN.md` remains the specification; each increment below should be an independently reviewable change with observable acceptance criteria.

Use a local mock upstream from the beginning. It should capture forwarded requests and produce controlled responses, delayed SSE chunks, usage reports, and failures. This makes acceptance checks deterministic without spending real API credits.

## Contracts to resolve before implementation

- **Authoritative prompt — resolved in steps 2 and 5:** `system_prompt` is required at issuance and persisted verbatim. Chat injection prepends a fresh system message and removes only client system messages with exactly matching string content. Sentinels, names, and headers never suppress it; all other messages retain their order and fields.
- **Token quota:** checking previously consumed tokens cannot enforce a hard cap under concurrency. Define reservation rules, output limits, and handling for missing usage, disconnects, and restarts.
- **Lifecycle — admission contract resolved in step 4:** revoke, done, and expiry close admission without cancelling admitted work. First terminal state wins; same-target retries are idempotent. Future accounting must retain in-flight reservations until normal reconciliation, including after `/done`; no early refund or usage reset.
- **Routing:** use a method-and-path allowlist. `/v1/models` ordinarily needs `GET`, unlike chat and embeddings.
- **Request accounting — resolved in step 6:** every admitted attempt permanently consumes one request before route/body validation, including blocked requests, missing keys, upstream failures, cancellation, and restart. Invalid credentials, inactive jobs, quota denials, and failed admission transactions consume nothing. No refunds; final admission persists exhaustion and later attempts receive `429`. Monthly monetary spend still needs pricing data; token totals alone are insufficient.

## Implementation increments

Each increment delivers demonstrable behavior, not just scaffolding.

| Step | Deliverable | Acceptance check |
| --- | --- | --- |
| **1. Runnable service and mock upstream — complete** | Configuration loading, `/healthz`, graceful shutdown, and a local upstream fixture. | Start the service and call health; drive the fixture through ordinary and delayed responses. |
| **2. Job issuance and persistence — complete** | SQLite migrations, authenticated control plane over TLS, key-pool configuration, and `POST /v1/jobs`. Generate secure tokens; persist only hashes and the job's authoritative prompt. | Issue a job, inspect persisted state, restart the service, and verify the job survives. Reject unauthenticated issuance and unknown key IDs. |
| **3. First complete proxy request — complete** | Support non-streaming chat only: job-token authentication, active-state checks, exact route allowlisting, bounded request bodies, and upstream authorization replacement. Never retry automatically. | Issue a token and send a chat request. The mock sees the configured upstream credential, not the job token. Invalid tokens and forbidden routes never reach it. |
| **4. Complete job lifecycle — complete** | Expiry, revoke, and done transitions, including their interaction with requests already admitted. | Hold an upstream request open, revoke the job, verify that request finishes while subsequent requests fail. Exercise expiry and done across restart. |
| **5. Authoritative prompt injection — complete** | Inject the stored operator prompt using a deterministic rule; preserve permitted client messages without trusting their sentinel claims. Apply only to supported chat requests. | Inspect the upstream payload for absent, spoofed, and duplicated operator-message cases. Verify exactly the intended authoritative content is injected. |
| **6. Concurrent request quotas — complete** | Atomic request admission with permanent attempt charging, durable exhaustion, and no completion-time refunds. | Race more requests than the remaining allowance; upstream receives no more requests than permitted. Exercise rejection and upstream-failure paths. |
| **7. Streaming and resource bounds — complete** | Incremental SSE forwarding, client-disconnect handling, connect/first-byte/idle/overall timeouts, and an in-flight limit with `503` retry guidance. | Observe the first SSE chunk before the response completes. Exercise stalls, disconnects, saturation, and upstream failures without hidden retries. |
| **8. Token usage accounting** | Extract usage from ordinary and streaming responses; persist request records and reconcile reservations on every terminal path. Keep unknown usage distinguishable from zero. | Reconcile known mock usage correctly; exercise missing usage, broken streams, and restart with outstanding requests. |
| **9. Hard token-budget enforcement** | Reserve a conservative request budget atomically before forwarding, constrain generation appropriately, and reconcile actual usage. Implement the agreed missing-usage policy. | Concurrent requests cannot admit more reserved work than the job budget allows. Unused reservations are released without granting budget twice. |
| **10. Body-policy checks** | Configurable secret-pattern blocking, advisory prompt-injection flags, and redacted audit reasons. | Fake secrets are blocked before forwarding; benign bodies pass; advisory matches are recorded without blocking. Bodies and credentials remain absent from logs. |
| **11. Remaining endpoint support** | Add embeddings and model listing individually, with endpoint-specific validation, accounting, and allowed methods. | Exercise each endpoint end-to-end. Chat prompt transformation never touches embeddings or model-list requests. |
| **12. Operational readiness** | Complete structured logs, Prometheus metrics, per-key usage aggregates, deployment configuration, and operator documentation. Implement spend reporting only with an explicit pricing model. | Run a staging scenario covering issuance, streaming, exhaustion, revocation, and restart; verify the resulting logs and metrics contain no secrets or request bodies. |

For every increment, update the relevant specification details and keep regression tests for consumer-visible boundaries and failure cases.

Steps 1 through 7 are implemented as the HTTPS-only `rustgate` service and separate loopback-only HTTP `mock-upstream` executable. See [the implemented service contract](DESIGN.md#implemented-service-steps-1-through-7) for required TLS/security configuration, issuance, lifecycle and proxy commands, prompt transformation, request charging, streaming deadlines and resource limits, persistence, fixture controls, and capture-API safety limits.

Verification: 73 regression tests pass; Clippy passes with warnings denied; both binaries build. Step 7 live HTTPS checks observe the first SSE chunk at 0.048 seconds and completion at 0.915 seconds. A held stream saturates the configured single slot: another attempt receives `503` with `Retry-After: 1` without reaching upstream; disconnect releases capacity. Delayed first bytes return `504`, idle and overall deadlines truncate committed SSE without a fabricated `[DONE]`, and a stalled incoming JSON upload returns `504`. A stalled upstream TLS handshake returns `504` at 1.012 seconds with exactly one TCP connection. Upstream `503` status/body/retry guidance pass through; the main mock scenario records exactly six expected captures with no hidden retries, using only the configured upstream credential and injected authoritative prompt. Regressions additionally cover actual HTTP disconnects before and after first-byte commitment, unpolled downstream backpressure, active streams that cannot reset the overall deadline, sanitized broken-stream errors, capacity recovery, configuration bounds/precedence, and permanent quota charging. Earlier quota/lifecycle/prompt regression coverage remains. Token accounting and enforcement remain steps 8–9.

PR #9 review fix: blocking admission retains its in-flight permit until the worker finishes, including when the caller disconnects or times out. Both queued-admission regressions fail before the fix and pass afterward. Live HTTPS checks hold a SQLite write lock, cancel admission by timeout and disconnect, and observe immediate `503` retry guidance for new attempts until the lock is released. Each abandoned admission charges once after release, capacity recovers, and no abandoned request reaches upstream.

OpenCode compatibility fix: the mock now selects SSE from literal `"stream": true` when no explicit fixture mode is supplied, rather than returning JSON to a streaming client. A failing-before/passing-after regression covers body-selected responses, explicit mode precedence, content, finish reason, and the SSE terminator. Real OpenCode 1.19.4 completes through HTTPS RustGate without query options, returning `Hello from mock.` with finish reason `stop`. A live body-selected stream also delivers its first event before delayed completion.

## Release checkpoints

- **After step 5:** useful local integration slice; not ready for build containers because quotas and streaming guarantees are incomplete.
- **After step 9:** core authenticated, streaming, quota-controlled proxy.
- **After step 12:** candidate for deployment against the complete specification.

Before connecting real build containers, verify the external network boundary: containers cannot reach the upstream directly, and the proxy's configured egress restrictions actually hold. Application tests cannot prove those firewall guarantees.

## Deferred extensions

Keep Vault integration, optional debugging-body storage, risk scoring, and escalation of advisory scans out of the initial roadmap. They are separable extensions, not prerequisites for the core proxy.
