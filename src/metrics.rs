use prometheus::{Encoder, IntGauge, Opts, Registry, TextEncoder};

#[derive(Clone)]
pub struct Metrics {
    pub registry: Registry,
    pub requests_total: prometheus::CounterVec,
    pub blocked_total: prometheus::CounterVec,
    pub tokens_total: prometheus::CounterVec,
    pub in_flight: IntGauge,
    pub upstream_errors_total: prometheus::CounterVec,
}

impl Metrics {
    pub fn new() -> Self {
        let registry = Registry::new();
        let requests_total = prometheus::CounterVec::new(
            Opts::new("rustgate_requests_total", "Proxied requests by job/tenant/verdict"),
            &["job_id", "tenant_id", "verdict"],
        )
        .unwrap();
        let blocked_total = prometheus::CounterVec::new(
            Opts::new("rustgate_blocked_total", "Blocked requests by reason"),
            &["reason"],
        )
        .unwrap();
        let tokens_total = prometheus::CounterVec::new(
            Opts::new("rustgate_tokens_total", "Tokens burned by key/provider"),
            &["key_id", "provider_id"],
        )
        .unwrap();
        let in_flight = IntGauge::new("rustgate_in_flight", "In-flight proxied requests").unwrap();
        let upstream_errors_total = prometheus::CounterVec::new(
            Opts::new("rustgate_upstream_errors_total", "Upstream error responses"),
            &["status_class"],
        )
        .unwrap();
        registry.register(Box::new(requests_total.clone())).unwrap();
        registry.register(Box::new(blocked_total.clone())).unwrap();
        registry.register(Box::new(tokens_total.clone())).unwrap();
        registry.register(Box::new(in_flight.clone())).unwrap();
        registry.register(Box::new(upstream_errors_total.clone())).unwrap();
        Metrics {
            registry,
            requests_total,
            blocked_total,
            tokens_total,
            in_flight,
            upstream_errors_total,
        }
    }

    pub fn encode(&self) -> String {
        let mut buf = Vec::new();
        let enc = TextEncoder::new();
        enc.encode(&self.registry.gather(), &mut buf).unwrap();
        String::from_utf8(buf).unwrap()
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}
