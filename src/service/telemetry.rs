use std::{
    collections::BTreeMap,
    fmt::Write as _,
    io::Write as _,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

use axum::{
    extract::{Request, State},
    http::Method,
    middleware::Next,
    response::Response,
};
use serde_json::{Value, json};

use super::ServiceState;
use crate::store::{AdmittedJob, OperationalSnapshot, Usage};

const ROUTES: [&str; 10] = [
    "health",
    "issue",
    "revoke",
    "done",
    "metrics",
    "usage",
    "chat",
    "embeddings",
    "models",
    "other",
];
const CLASSES: [&str; 6] = ["cancelled", "1xx", "2xx", "3xx", "4xx", "5xx"];
const BOUNDS: [u64; 6] = [10, 100, 1000, 10_000, 60_000, 600_000];

#[derive(Default)]
pub(super) struct Metrics {
    histogram_lock: Mutex<()>,
    requests: [[AtomicU64; 6]; 10],
    duration_buckets: [AtomicU64; 6],
    duration_micros: AtomicU64,
    count: AtomicU64,
    pub upstream_requests: AtomicU64,
    pub upstream_errors: AtomicU64,
}

/// Last owner emits exactly one event, including cancelled blocking admissions.
#[derive(Clone)]
pub(super) struct Observation(Arc<RequestLog>);

struct RequestLog {
    start: Instant,
    route: usize,
    method: &'static str,
    metrics: Arc<Metrics>,
    fields: Mutex<Value>,
}

pub(super) fn event(value: Value) {
    // Serialize before taking the output lock; each event is one complete JSON line.
    let mut line = value.to_string();
    line.push('\n');
    let _ = std::io::stderr().lock().write_all(line.as_bytes());
}

impl Observation {
    fn new(request: &Request, metrics: Arc<Metrics>) -> Self {
        let route = match request.uri().path() {
            "/healthz" => 0,
            "/v1/jobs" => 1,
            "/metrics" => 4,
            "/v1/usage" => 5,
            "/v1/chat/completions" => 6,
            "/v1/embeddings" => 7,
            "/v1/models" => 8,
            _ => {
                // Never log raw paths or path parameters, including malformed ones.
                let mut segments = request.uri().path().split('/');
                match (
                    segments.next(),
                    segments.next(),
                    segments.next(),
                    segments.next(),
                    segments.next(),
                    segments.next(),
                ) {
                    (Some(""), Some("v1"), Some("jobs"), Some(_), Some("revoke"), None) => 2,
                    (Some(""), Some("v1"), Some("jobs"), Some(_), Some("done"), None) => 3,
                    _ => 9,
                }
            }
        };
        let method = match *request.method() {
            Method::GET => "GET",
            Method::POST => "POST",
            _ => "other",
        };
        Self(Arc::new(RequestLog {
            start: Instant::now(),
            route,
            method,
            metrics,
            fields: Mutex::new(
                json!({"event":"request", "status":null, "model":null, "outcome":"cancelled"}),
            ),
        }))
    }

    pub(super) fn update(&self, fields: Value) {
        if let Value::Object(fields) = fields
            && let Ok(mut current) = self.0.fields.lock()
        {
            current.as_object_mut().unwrap().extend(fields);
        }
    }

    fn responded(&self, status: u16) {
        let mut fields = self.0.fields.lock().unwrap_or_else(|e| e.into_inner());
        fields["status"] = json!(status);
        if fields.get("request_id").is_none() {
            fields["outcome"] = json!("responded");
        }
    }

    pub(super) fn admitted(&self, job: &AdmittedJob) {
        self.update(json!({"request_id":job.request_id, "job_id":job.job_id,
            "tenant_id":job.tenant_id, "key_id":job.upstream_key_id}));
    }

    pub(super) fn usage(&self, outcome: &'static str, usage: Option<Usage>, final_usage: bool) {
        self.update(json!({"outcome":outcome, "prompt_tokens":usage.map(|u|u.prompt_tokens),
            "completion_tokens":usage.map(|u|u.completion_tokens), "total_tokens":usage.map(|u|u.total_tokens),
            "usage_final":final_usage}));
    }

    pub(super) fn dispatched(&self) {
        self.0
            .metrics
            .upstream_requests
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn upstream_failed(&self) {
        self.0
            .metrics
            .upstream_errors
            .fetch_add(1, Ordering::Relaxed);
    }
}

impl Drop for RequestLog {
    fn drop(&mut self) {
        let elapsed = self.start.elapsed();
        let histogram = self
            .metrics
            .histogram_lock
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let mut fields = self.fields.lock().unwrap_or_else(|e| e.into_inner());
        let class = fields["status"]
            .as_u64()
            .map_or(0, |status| (status / 100).min(5) as usize);
        self.metrics.requests[self.route][class].fetch_add(1, Ordering::Relaxed);
        self.metrics.count.fetch_add(1, Ordering::Relaxed);
        self.metrics.duration_micros.fetch_add(
            elapsed.as_micros().min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
        for (index, bound) in BOUNDS.iter().enumerate() {
            if elapsed.as_millis() <= u128::from(*bound) {
                self.metrics.duration_buckets[index].fetch_add(1, Ordering::Relaxed);
            }
        }
        drop(histogram);
        fields["route"] = json!(ROUTES[self.route]);
        fields["method"] = json!(self.method);
        fields["duration_ms"] = json!(elapsed.as_secs_f64() * 1000.0);
        fields["timestamp"] = json!(time::OffsetDateTime::now_utc().unix_timestamp());
        event(fields.take());
    }
}

pub(super) async fn observe(
    State(state): State<Arc<ServiceState>>,
    mut request: Request,
    next: Next,
) -> Response {
    let observation = Observation::new(&request, state.metrics.clone());
    request.extensions_mut().insert(observation.clone());
    let response = next.run(request).await;
    observation.responded(response.status().as_u16());
    response
}

fn label(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace('"', "\\\"")
}

fn family(out: &mut String, name: &str, kind: &str, help: &str) {
    let _ = writeln!(out, "# HELP {name} {help}\n# TYPE {name} {kind}");
}

pub(super) fn render(state: &ServiceState, snapshot: OperationalSnapshot) -> String {
    let mut out = String::new();
    family(
        &mut out,
        "rustgate_http_requests_total",
        "counter",
        "Finished HTTP requests since process start, including accounting latency.",
    );
    for (route, counts) in ROUTES.iter().zip(&state.metrics.requests) {
        for (class, count) in CLASSES.iter().zip(counts) {
            let _ = writeln!(
                out,
                "rustgate_http_requests_total{{route=\"{route}\",status_class=\"{class}\"}} {}",
                count.load(Ordering::Relaxed)
            );
        }
    }
    family(
        &mut out,
        "rustgate_request_duration_seconds",
        "histogram",
        "Request lifetime through final ownership release.",
    );
    let histogram = state
        .metrics
        .histogram_lock
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    for (bound, count) in BOUNDS.iter().zip(&state.metrics.duration_buckets) {
        let _ = writeln!(
            out,
            "rustgate_request_duration_seconds_bucket{{le=\"{}\"}} {}",
            *bound as f64 / 1000.0,
            count.load(Ordering::Relaxed)
        );
    }
    let count = state.metrics.count.load(Ordering::Relaxed);
    let _ = writeln!(
        out,
        "rustgate_request_duration_seconds_bucket{{le=\"+Inf\"}} {count}\nrustgate_request_duration_seconds_count {count}\nrustgate_request_duration_seconds_sum {}",
        state.metrics.duration_micros.load(Ordering::Relaxed) as f64 / 1_000_000.0
    );
    drop(histogram);
    family(
        &mut out,
        "rustgate_in_flight_requests",
        "gauge",
        "Occupied proxy slots including durable accounting workers.",
    );
    let _ = writeln!(
        out,
        "rustgate_in_flight_requests {}",
        state.config.max_in_flight_requests as usize - state.in_flight.available_permits()
    );
    for (name, value) in [
        (
            "rustgate_upstream_requests_total",
            &state.metrics.upstream_requests,
        ),
        (
            "rustgate_upstream_errors_total",
            &state.metrics.upstream_errors,
        ),
    ] {
        family(
            &mut out,
            name,
            "counter",
            "Upstream dispatches or unsuccessful terminal attempts since process start.",
        );
        let _ = writeln!(out, "{name} {}", value.load(Ordering::Relaxed));
    }
    for (index, (name, kind, help)) in [
        (
            "rustgate_key_requests_total",
            "counter",
            "Durable admitted attempts per logical upstream key.",
        ),
        (
            "rustgate_key_tokens_total",
            "counter",
            "Durable known reported tokens per logical upstream key.",
        ),
        (
            "rustgate_key_tokens_unreported_total",
            "counter",
            "Durable uncertain spending charged per logical upstream key.",
        ),
        (
            "rustgate_key_tokens_reserved",
            "gauge",
            "Pending token escrow per logical upstream key.",
        ),
    ]
    .into_iter()
    .enumerate()
    {
        family(&mut out, name, kind, help);
        // Prometheus requires each family's metadata and samples to be contiguous.
        for key in &snapshot.keys {
            let value = match index {
                0 => key.requests_used,
                1 => key.tokens_used,
                2 => key.tokens_unreported,
                _ => key.tokens_reserved,
            };
            let _ = writeln!(
                out,
                "{name}{{key_id=\"{}\"}} {value}",
                label(&key.upstream_key_id)
            );
        }
    }
    family(
        &mut out,
        "rustgate_job_requests_total",
        "counter",
        "Durable charged requests per job and tenant.",
    );
    let mut tenants = BTreeMap::<String, i128>::new();
    for job in snapshot.jobs {
        let _ = writeln!(
            out,
            "rustgate_job_requests_total{{job_id=\"{}\",tenant_id=\"{}\"}} {}",
            label(&job.job_id),
            label(&job.tenant_id),
            job.requests_used
        );
        *tenants.entry(job.tenant_id).or_default() += i128::from(job.requests_used);
    }
    family(
        &mut out,
        "rustgate_tenant_requests_total",
        "counter",
        "Durable charged requests per tenant.",
    );
    for (tenant, count) in tenants {
        let _ = writeln!(
            out,
            "rustgate_tenant_requests_total{{tenant_id=\"{}\"}} {count}",
            label(&tenant)
        );
    }
    family(
        &mut out,
        "rustgate_requests_by_outcome",
        "gauge",
        "Current durable request counts by outcome; pending decreases on settlement.",
    );
    for row in snapshot.outcomes {
        let _ = writeln!(
            out,
            "rustgate_requests_by_outcome{{outcome=\"{}\"}} {}",
            label(&row.outcome),
            row.count
        );
    }
    family(
        &mut out,
        "rustgate_blocked_requests_total",
        "counter",
        "Durable body policy blocks by redacted reason.",
    );
    for row in snapshot.blocked {
        let _ = writeln!(
            out,
            "rustgate_blocked_requests_total{{reason=\"{}\"}} {}",
            label(&row.reason),
            row.count
        );
    }
    out
}
