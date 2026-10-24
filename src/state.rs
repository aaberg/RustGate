use crate::adapters::Adapters;
use crate::config::Config;
use crate::metrics::Metrics;
use crate::policy::PolicyEngine;
use crate::secrets::KeyPool;
use crate::store::Store;
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    pub cfg: Config,
    pub store: Store,
    pub pool: KeyPool,
    pub adapters: Arc<Adapters>,
    pub policy: Arc<PolicyEngine>,
    pub metrics: Metrics,
    pub upstream: reqwest::Client,
    /// Bounds total in-flight proxied requests when max_in_flight > 0.
    pub gate: Option<Arc<tokio::sync::Semaphore>>,
}

pub type SharedState = std::sync::Arc<AppState>;

impl AppState {
    pub fn build(cfg: Config, store: Store, pool: KeyPool) -> Self {
        let adapters = Adapters::build(&cfg.providers);
        let extra_patterns: Vec<String> = cfg
            .providers
            .values()
            .flat_map(|p| p.secret_patterns.clone())
            .collect();
        let policy = PolicyEngine::new(
            cfg.request_size_limit_bytes,
            &extra_patterns,
            crate::policy::DEFAULT_SENTINEL.to_string(),
        );
        let metrics = Metrics::new();
        let upstream = reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_secs(5))
            .timeout(std::time::Duration::from_secs(600))
            // Never follow redirects automatically: a 307/308 would carry
            // the real Authorization header (and body) to a target the path
            // allowlist never validated. Redirects pass through to the client.
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .expect("upstream client");
        let gate = if cfg.max_in_flight > 0 {
            Some(std::sync::Arc::new(tokio::sync::Semaphore::new(cfg.max_in_flight)))
        } else {
            None
        };
        AppState {
            cfg,
            store,
            pool,
            adapters: Arc::new(adapters),
            policy: Arc::new(policy),
            metrics,
            upstream,
            gate,
        }
    }
}
