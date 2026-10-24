use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use std::path::PathBuf;

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct Config {
    pub listen_addr: String,
    pub listen_port: u16,
    pub tls_cert_path: Option<PathBuf>,
    pub tls_key_path: Option<PathBuf>,
    pub sqlite_path: PathBuf,
    pub secrets_file: Option<PathBuf>,
    /// Static control-plane bearer token. If None, control-plane requires mTLS.
    pub control_token: Option<String>,
    /// Defaults applied at job issuance when the request omits values.
    pub default_ttl_seconds: u64,
    pub max_ttl_seconds: u64,
    pub default_max_requests: u64,
    pub max_max_requests: u64,
    pub default_max_tokens_total: u64,
    pub max_max_tokens_total: u64,
    pub request_size_limit_bytes: usize,
    /// In-flight proxied request bound; 0 = unbounded.
    pub max_in_flight: usize,
    /// Per-provider settings.
    pub providers: HashMap<String, ProviderConfig>,
    /// If true, data plane may serve plain HTTP (dev only).
    pub allow_insecure: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProviderConfig {
    pub base_url: String,
    /// Forwardable upstream paths; defaults to the OpenAI-compatible set.
    #[serde(default)]
    pub allow_paths: Vec<String>,
    /// Extra secret-scan regexes specific to this provider's key shapes.
    #[serde(default)]
    pub secret_patterns: Vec<String>,
    /// Whether this adapter requests usage reporting on streamed chats.
    #[serde(default = "default_true")]
    pub request_stream_usage: bool,
}

fn default_true() -> bool {
    true
}

impl Default for ProviderConfig {
    fn default() -> Self {
        ProviderConfig {
            base_url: String::new(),
            allow_paths: default_allow_paths(),
            secret_patterns: Vec::new(),
            request_stream_usage: true,
        }
    }
}

pub fn default_allow_paths() -> Vec<String> {
    vec![
        "/v1/chat/completions".to_string(),
        "/v1/embeddings".to_string(),
        "/v1/models".to_string(),
    ]
}

impl Default for Config {
    fn default() -> Self {
        let mut providers = HashMap::new();
        providers.insert(
            "mistral".to_string(),
            ProviderConfig {
                base_url: "https://api.mistral.ai".to_string(),
                ..ProviderConfig::default()
            },
        );
        Config {
            listen_addr: "127.0.0.1".to_string(),
            listen_port: 8443,
            tls_cert_path: None,
            tls_key_path: None,
            sqlite_path: PathBuf::from("rustgate.db"),
            secrets_file: None,
            control_token: None,
            default_ttl_seconds: 1800,
            max_ttl_seconds: 7200,
            default_max_requests: 100,
            max_max_requests: 10000,
            default_max_tokens_total: 2_000_000,
            max_max_tokens_total: 100_000_000,
            request_size_limit_bytes: 1024 * 1024,
            max_in_flight: 0,
            providers,
            allow_insecure: false,
        }
    }
}

impl Config {
    /// Load from a TOML-ish env overlay: RUSTGATE_<SECTION>_<KEY> is kept simple —
    /// the primary path is a JSON config file plus env overrides for secrets.
    pub fn from_file(path: &PathBuf) -> Result<Self, ConfigError> {
        let raw = std::fs::read_to_string(path)
            .map_err(|e| ConfigError::Io(format!("read {path:?}: {e}")))?;
        let cfg: Config = serde_json::from_str(&raw).map_err(ConfigError::Parse)?;
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.providers.is_empty() {
            return Err(ConfigError::Validation("no providers configured".into()));
        }
        for (id, p) in &self.providers {
            if p.base_url.is_empty() {
                return Err(ConfigError::Validation(format!(
                    "provider {id}: base_url is empty"
                )));
            }
        }
        if !self.allow_insecure && self.tls_cert_path.is_none() {
            return Err(ConfigError::Validation(
                "TLS cert path required (or set allow_insecure for dev)".into(),
            ));
        }
        Ok(())
    }

    /// Load from environment variables (RUSTGATE_*), falling back to defaults.
    pub fn from_env() -> Result<Self, ConfigError> {
        let mut cfg = Config::default();
        if let Ok(v) = std::env::var("RUSTGATE_LISTEN_ADDR") {
            cfg.listen_addr = v;
        }
        if let Ok(v) = std::env::var("RUSTGATE_LISTEN_PORT") {
            cfg.listen_port = v
                .parse()
                .map_err(|_| ConfigError::Validation("bad RUSTGATE_LISTEN_PORT".into()))?;
        }
        if let Ok(v) = std::env::var("RUSTGATE_SQLITE_PATH") {
            cfg.sqlite_path = PathBuf::from(v);
        }
        if let Ok(v) = std::env::var("RUSTGATE_SECRETS_FILE") {
            cfg.secrets_file = Some(PathBuf::from(v));
        }
        if let Ok(v) = std::env::var("RUSTGATE_CONTROL_TOKEN") {
            cfg.control_token = Some(v);
        }
        if let Ok(v) = std::env::var("RUSTGATE_TLS_CERT") {
            cfg.tls_cert_path = Some(PathBuf::from(v));
        }
        if let Ok(v) = std::env::var("RUSTGATE_TLS_KEY") {
            cfg.tls_key_path = Some(PathBuf::from(v));
        }
        if let Ok(_) = std::env::var("RUSTGATE_ALLOW_INSECURE") {
            cfg.allow_insecure = true;
        }
        if let Ok(v) = std::env::var("RUSTGATE_UPSTREAM_BASE_URL") {
            if let Some(p) = cfg.providers.get_mut("mistral") {
                p.base_url = v;
            }
        }
        if let Ok(v) = std::env::var("RUSTGATE_CONTROL_TOKEN_FILE") {
            cfg.control_token = Some(
                std::fs::read_to_string(v)
                    .map_err(|e| ConfigError::Io(e.to_string()))?
                    .trim()
                    .to_string(),
            );
        }
        cfg.validate()?;
        Ok(cfg)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("io: {0}")]
    Io(String),
    #[error("parse: {0}")]
    Parse(#[from] serde_json::Error),
    #[error("validation: {0}")]
    Validation(String),
}

#[derive(Debug, Clone, Serialize)]
pub struct JobPolicySnapshot {
    pub job_id: String,
    pub tenant_id: String,
    pub provider_id: String,
    pub upstream_key_id: String,
    pub status: String,
    pub expires_at: chrono::DateTime<chrono::Utc>,
    pub max_requests: u64,
    pub requests_used: u64,
    pub max_tokens_total: u64,
    pub tokens_used: u64,
}
