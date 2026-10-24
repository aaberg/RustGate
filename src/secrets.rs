use std::collections::HashMap;
use std::path::Path;

/// One entry of the upstream key pool.
#[derive(Debug, Clone)]
pub struct UpstreamKey {
    pub key_id: String,
    pub provider_id: String,
    pub key: String,
}

/// The key pool, loaded at startup from a secrets file (preferred) or env vars.
/// The proxy never exposes key material beyond this module and the forwarding path.
#[derive(Debug, Clone, Default)]
pub struct KeyPool {
    keys: HashMap<String, UpstreamKey>,
}

impl KeyPool {
    /// File format: one entry per line, `key_id = { provider = "...", key = "..." }`
    /// (inline JSON) or the compact form `key_id = provider:sk-...`.
    /// Lines starting with `#` are comments. File should be mode 0600.
    pub fn load_file(path: &Path) -> Result<Self, SecretsError> {
        let meta = std::fs::metadata(path)
            .map_err(|e| SecretsError::Io(format!("stat {path:?}: {e}")))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = meta.permissions().mode() & 0o777;
            if mode & 0o077 != 0 {
                tracing::warn!(
                    path = ?path,
                    mode = format!("{mode:o}"),
                    "secrets file is group/world accessible; expected 0600"
                );
            }
        }
        let raw = std::fs::read_to_string(path)
            .map_err(|e| SecretsError::Io(format!("read {path:?}: {e}")))?;
        let mut keys = HashMap::new();
        for (i, line) in raw.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let (key_id, rest) = line
                .split_once('=')
                .ok_or_else(|| SecretsError::Parse(format!("line {}: expected `key_id = ...`", i + 1)))?;
            let key_id = key_id.trim();
            let rest = rest.trim();
            let entry = if rest.starts_with('{') {
                let v: serde_json::Value = serde_json::from_str(rest)
                    .map_err(|e| SecretsError::Parse(format!("line {}: {e}", i + 1)))?;
                UpstreamKey {
                    key_id: key_id.to_string(),
                    provider_id: v
                        .get("provider")
                        .and_then(|p| p.as_str())
                        .ok_or_else(|| SecretsError::Parse(format!("line {}: missing provider", i + 1)))?
                        .to_string(),
                    key: v
                        .get("key")
                        .and_then(|k| k.as_str())
                        .ok_or_else(|| SecretsError::Parse(format!("line {}: missing key", i + 1)))?
                        .to_string(),
                }
            } else {
                // compact: provider:sk-...
                let (provider, key) = rest
                    .split_once(':')
                    .ok_or_else(|| SecretsError::Parse(format!("line {}: expected provider:key", i + 1)))?;
                UpstreamKey {
                    key_id: key_id.to_string(),
                    provider_id: provider.trim().to_string(),
                    key: key.trim().to_string(),
                }
            };
            if entry.key.is_empty() {
                return Err(SecretsError::Parse(format!("line {}: empty key", i + 1)));
            }
            keys.insert(entry.key_id.clone(), entry);
        }
        Ok(KeyPool { keys })
    }

    /// Fallback: env vars of the form RUSTGATE_KEY_<ID>=provider:sk-...
    pub fn load_env() -> Self {
        let mut keys = HashMap::new();
        for (k, v) in std::env::vars() {
            if let Some(id) = k.strip_prefix("RUSTGATE_KEY_") {
                if let Some((provider, key)) = v.split_once(':') {
                    keys.insert(
                        id.to_lowercase(),
                        UpstreamKey {
                            key_id: id.to_lowercase(),
                            provider_id: provider.trim().to_string(),
                            key: key.trim().to_string(),
                        },
                    );
                }
            }
        }
        KeyPool { keys }
    }

    #[doc(hidden)]
    pub fn load_env_for_test(provider_id: &str, key: &str) -> Self {
        let mut keys = HashMap::new();
        keys.insert(
            "mistral-main".to_string(),
            UpstreamKey {
                key_id: "mistral-main".to_string(),
                provider_id: provider_id.to_string(),
                key: key.to_string(),
            },
        );
        KeyPool { keys }
    }

    pub fn load(cfg: &crate::config::Config) -> Result<Self, SecretsError> {
        if let Some(p) = &cfg.secrets_file {
            if p.exists() {
                return Self::load_file(p);
            }
            tracing::warn!(path = ?p, "secrets file configured but missing; falling back to env");
        }
        Ok(Self::load_env())
    }

    pub fn get(&self, key_id: &str) -> Option<&UpstreamKey> {
        self.keys.get(key_id)
    }

    pub fn ids(&self) -> Vec<&str> {
        self.keys.keys().map(|s| s.as_str()).collect()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SecretsError {
    #[error("io: {0}")]
    Io(String),
    #[error("parse: {0}")]
    Parse(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_inline_json() {
        let dir = tempfile::tempdir().unwrap();
        let f = dir.path().join("keys");
        std::fs::write(
            &f,
            "# comment\nmistral-main = { \"provider\": \"mistral\", \"key\": \"sk-test\" }\n",
        )
        .unwrap();
        let pool = KeyPool::load_file(&f).unwrap();
        let k = pool.get("mistral-main").unwrap();
        assert_eq!(k.provider_id, "mistral");
        assert_eq!(k.key, "sk-test");
    }

    #[test]
    fn parses_compact() {
        let dir = tempfile::tempdir().unwrap();
        let f = dir.path().join("keys");
        std::fs::write(&f, "mistral-main = mistral:sk-abc\n").unwrap();
        let pool = KeyPool::load_file(&f).unwrap();
        assert_eq!(pool.get("mistral-main").unwrap().key, "sk-abc");
    }
}
