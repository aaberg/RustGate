use std::{
    collections::HashMap,
    ffi::OsString,
    fmt,
    net::SocketAddr,
    path::{Path, PathBuf},
};

use anyhow::{Result, anyhow, bail};
use serde::Deserialize;
use url::{Host, Url};

const DEFAULT_LISTEN_ADDR: &str = "127.0.0.1:8443";
const DEFAULT_UPSTREAM_BASE_URL: &str = "http://127.0.0.1:9090";

pub struct Config {
    pub listen_addr: SocketAddr,
    pub upstream_base_url: Url,
    pub tls_cert_path: PathBuf,
    pub tls_key_path: PathBuf,
    pub database_path: PathBuf,
    pub control_plane_token: String,
    pub upstream_keys: HashMap<String, String>,
    pub proxy_url: Url,
    pub default_ttl_seconds: u64,
    pub max_ttl_seconds: u64,
    pub default_max_requests: u64,
    pub max_requests: u64,
    pub default_max_tokens_total: u64,
    pub max_tokens_total: u64,
    pub connect_timeout_seconds: u64,
    pub first_byte_timeout_seconds: u64,
    pub idle_timeout_seconds: u64,
    pub overall_timeout_seconds: u64,
    pub max_in_flight_requests: u64,
    pub secret_scan_patterns: Vec<String>,
    pub prompt_injection_patterns: Vec<String>,
}

impl fmt::Debug for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Config").finish_non_exhaustive()
    }
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileConfig {
    listen_addr: Option<String>,
    upstream_base_url: Option<String>,
    tls_cert_path: Option<String>,
    tls_key_path: Option<String>,
    database_path: Option<String>,
    control_plane_token: Option<String>,
    upstream_keys: Option<HashMap<String, String>>,
    proxy_url: Option<String>,
    default_ttl_seconds: Option<u64>,
    max_ttl_seconds: Option<u64>,
    default_max_requests: Option<u64>,
    max_requests: Option<u64>,
    default_max_tokens_total: Option<u64>,
    max_tokens_total: Option<u64>,
    connect_timeout_seconds: Option<u64>,
    first_byte_timeout_seconds: Option<u64>,
    idle_timeout_seconds: Option<u64>,
    overall_timeout_seconds: Option<u64>,
    max_in_flight_requests: Option<u64>,
    secret_scan_patterns: Option<Vec<String>>,
    prompt_injection_patterns: Option<Vec<String>>,
}

impl Config {
    pub fn load() -> Result<Self> {
        load_with(
            |name| std::env::var_os(name),
            |path| std::fs::read_to_string(path),
        )
    }

    // Lists replace defaults wholesale; an empty list disables that scanner.
    // The long-run heuristic requires at least 97 base64-alphabet bytes and a
    // digit, '+' or '/' with at least 48 such bytes on either side. It does not
    // measure entropy and can match benign encoded data.
    pub(crate) fn default_secret_scan_patterns() -> Vec<String> {
        vec![
            r"sk-[A-Za-z0-9_-]{20,}".to_owned(),
            r"[A-Za-z0-9+/]{48,}[0-9+/][A-Za-z0-9+/]{48,}={0,2}".to_owned(),
        ]
    }

    pub(crate) fn default_prompt_injection_patterns() -> Vec<String> {
        vec![
            r"(?i)reveal\s+your\s+system\s+prompt".to_owned(),
            r"(?i)print\s+your\s+environment".to_owned(),
            r"(?i)include\s+the\s+API\s+key".to_owned(),
        ]
    }
}

fn env_value(env: &mut impl FnMut(&str) -> Option<OsString>, name: &str) -> Result<Option<String>> {
    env(name)
        .map(|value| {
            value
                .into_string()
                .map_err(|_| anyhow!("{name} must contain valid Unicode"))
        })
        .transpose()
}

fn required(value: Option<String>, name: &str) -> Result<String> {
    value
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| anyhow!("missing or blank {name}"))
}

pub(crate) fn validate_policy_patterns(patterns: &[String], field: &'static str) -> Result<()> {
    if patterns.len() > 64
        || patterns.iter().any(|pattern| pattern.len() > 4096)
        || patterns.iter().map(String::len).sum::<usize>() > 65536
    {
        bail!(
            "invalid {field}: at most 64 patterns, 4096 bytes per pattern, and 65536 total bytes are allowed"
        );
    }
    Ok(())
}

fn load_with(
    mut env: impl FnMut(&str) -> Option<OsString>,
    read_file: impl FnOnce(&Path) -> std::io::Result<String>,
) -> Result<Config> {
    let mut settings = match env_value(&mut env, "RUSTGATE_CONFIG")? {
        Some(path) => {
            let contents = read_file(Path::new(&path)).map_err(|_| {
                anyhow!("could not read configuration file selected by RUSTGATE_CONFIG")
            })?;
            // Parser diagnostics can include source lines containing credentials.
            toml::from_str::<FileConfig>(&contents)
                .map_err(|_| anyhow!("invalid configuration file selected by RUSTGATE_CONFIG"))?
        }
        None => FileConfig::default(),
    };
    macro_rules! strings {
        ($($field:ident),+ $(,)?) => {$(
            if let Some(value) = env_value(&mut env, concat!("RUSTGATE_", stringify!($field)).to_ascii_uppercase().as_str())? {
                settings.$field = Some(value);
            }
        )+};
    }
    strings!(
        listen_addr,
        upstream_base_url,
        tls_cert_path,
        tls_key_path,
        database_path,
        control_plane_token,
        proxy_url
    );
    macro_rules! numbers {
        ($($field:ident),+ $(,)?) => {$(
            if let Some(value) = env_value(&mut env, concat!("RUSTGATE_", stringify!($field)).to_ascii_uppercase().as_str())? {
                settings.$field = Some(value.parse::<u64>().map_err(|_| anyhow!(concat!("invalid ", stringify!($field), ": expected an unsigned integer")))?);
            }
        )+};
    }
    numbers!(
        default_ttl_seconds,
        max_ttl_seconds,
        default_max_requests,
        max_requests,
        default_max_tokens_total,
        max_tokens_total,
        connect_timeout_seconds,
        first_byte_timeout_seconds,
        idle_timeout_seconds,
        overall_timeout_seconds,
        max_in_flight_requests
    );
    if let Some(value) = env_value(&mut env, "RUSTGATE_UPSTREAM_KEYS")? {
        settings.upstream_keys = Some(serde_json::from_str(&value).map_err(|_| {
            anyhow!("invalid upstream_keys: expected a JSON object of string credentials")
        })?);
    }
    for (field, destination) in [
        ("secret_scan_patterns", &mut settings.secret_scan_patterns),
        (
            "prompt_injection_patterns",
            &mut settings.prompt_injection_patterns,
        ),
    ] {
        let name = format!("RUSTGATE_{}", field.to_ascii_uppercase());
        if let Some(value) = env_value(&mut env, &name)? {
            *destination =
                Some(serde_json::from_str::<Vec<String>>(&value).map_err(|_| {
                    anyhow!("invalid {field}: expected a JSON array of regex strings")
                })?);
        }
    }
    let listen_addr = settings
        .listen_addr
        .as_deref()
        .unwrap_or(DEFAULT_LISTEN_ADDR)
        .parse::<SocketAddr>()
        .map_err(|_| anyhow!("invalid listen_addr: expected an IP socket address"))?;
    let upstream_base_url = Url::parse(
        settings
            .upstream_base_url
            .as_deref()
            .unwrap_or(DEFAULT_UPSTREAM_BASE_URL),
    )
    .map_err(|_| anyhow!("invalid upstream_base_url: expected an absolute HTTP(S) URL"))?;
    validate_upstream(&upstream_base_url)?;
    let tls_cert_path = PathBuf::from(required(settings.tls_cert_path, "tls_cert_path")?);
    let tls_key_path = PathBuf::from(required(settings.tls_key_path, "tls_key_path")?);
    let database = settings
        .database_path
        .unwrap_or_else(|| "rustgate.sqlite3".to_owned());
    if database.trim().is_empty() || database == ":memory:" || database.starts_with("file:") {
        bail!("invalid database_path: a persistent file path is required");
    }
    let control_plane_token = required(settings.control_plane_token, "control_plane_token")?;
    if control_plane_token.len() < 32 || !control_plane_token.bytes().all(|b| b.is_ascii_graphic())
    {
        bail!(
            "invalid control_plane_token: expected at least 32 visible ASCII bytes without whitespace"
        );
    }
    let upstream_keys = settings
        .upstream_keys
        .ok_or_else(|| anyhow!("missing upstream_keys"))?;
    if upstream_keys.is_empty()
        || upstream_keys.iter().any(|(id, key)| {
            id.trim().is_empty()
                || id.len() > 128
                || key.is_empty()
                || !key.bytes().all(|b| b.is_ascii_graphic())
        })
    {
        bail!(
            "invalid upstream_keys: expected nonblank IDs of at most 128 bytes and nonempty visible ASCII credentials"
        );
    }
    let proxy_url = Url::parse(&required(settings.proxy_url, "proxy_url")?)
        .map_err(|_| anyhow!("invalid proxy_url: expected an absolute HTTPS URL ending in /v1"))?;
    if proxy_url.scheme() != "https"
        || proxy_url.host().is_none()
        || proxy_url.path() != "/v1"
        || !proxy_url.username().is_empty()
        || proxy_url.password().is_some()
        || proxy_url.query().is_some()
        || proxy_url.fragment().is_some()
    {
        bail!(
            "invalid proxy_url: expected HTTPS with a host, /v1 path, and no credentials, query or fragment"
        );
    }
    let default_ttl_seconds = settings.default_ttl_seconds.unwrap_or(1800);
    let max_ttl_seconds = settings.max_ttl_seconds.unwrap_or(86400);
    let default_max_requests = settings.default_max_requests.unwrap_or(100);
    let max_requests = settings.max_requests.unwrap_or(10000);
    let default_max_tokens_total = settings.default_max_tokens_total.unwrap_or(2000000);
    let max_tokens_total = settings.max_tokens_total.unwrap_or(100000000);
    for (name, default, maximum) in [
        ("ttl_seconds", default_ttl_seconds, max_ttl_seconds),
        ("max_requests", default_max_requests, max_requests),
        (
            "max_tokens_total",
            default_max_tokens_total,
            max_tokens_total,
        ),
    ] {
        if default == 0 || default > maximum || maximum > i64::MAX as u64 {
            bail!(
                "invalid {name} bounds: default must be positive and no greater than maximum; maximum must fit SQLite INTEGER"
            );
        }
    }
    if max_ttl_seconds > 31536000 {
        bail!("invalid max_ttl_seconds: maximum is 31536000");
    }
    let connect_timeout_seconds = settings.connect_timeout_seconds.unwrap_or(5);
    let first_byte_timeout_seconds = settings.first_byte_timeout_seconds.unwrap_or(60);
    let idle_timeout_seconds = settings.idle_timeout_seconds.unwrap_or(120);
    let overall_timeout_seconds = settings.overall_timeout_seconds.unwrap_or(600);
    let max_in_flight_requests = settings.max_in_flight_requests.unwrap_or(64);
    for (name, value) in [
        ("connect_timeout_seconds", connect_timeout_seconds),
        ("first_byte_timeout_seconds", first_byte_timeout_seconds),
        ("idle_timeout_seconds", idle_timeout_seconds),
        ("overall_timeout_seconds", overall_timeout_seconds),
    ] {
        if !(1..=86400).contains(&value) {
            bail!("invalid {name}: expected 1..=86400 seconds");
        }
    }
    if !(1..=65536).contains(&max_in_flight_requests) {
        bail!("invalid max_in_flight_requests: expected 1..=65536");
    }
    let secret_scan_patterns = settings
        .secret_scan_patterns
        .unwrap_or_else(Config::default_secret_scan_patterns);
    let prompt_injection_patterns = settings
        .prompt_injection_patterns
        .unwrap_or_else(Config::default_prompt_injection_patterns);
    validate_policy_patterns(&secret_scan_patterns, "secret_scan_patterns")?;
    validate_policy_patterns(&prompt_injection_patterns, "prompt_injection_patterns")?;
    Ok(Config {
        listen_addr,
        upstream_base_url,
        tls_cert_path,
        tls_key_path,
        database_path: database.into(),
        control_plane_token,
        upstream_keys,
        proxy_url,
        default_ttl_seconds,
        max_ttl_seconds,
        default_max_requests,
        max_requests,
        default_max_tokens_total,
        max_tokens_total,
        connect_timeout_seconds,
        first_byte_timeout_seconds,
        idle_timeout_seconds,
        overall_timeout_seconds,
        max_in_flight_requests,
        secret_scan_patterns,
        prompt_injection_patterns,
    })
}

fn validate_upstream(url: &Url) -> Result<()> {
    if !matches!(url.scheme(), "http" | "https") || url.host().is_none() {
        bail!("invalid upstream_base_url: expected an absolute HTTP(S) URL with a host");
    }
    if !url.username().is_empty() || url.password().is_some() {
        bail!("invalid upstream_base_url: credentials are not allowed");
    }
    if url.query().is_some() || url.fragment().is_some() {
        bail!("invalid upstream_base_url: query strings and fragments are not allowed");
    }
    let is_loopback = match url.host() {
        Some(Host::Ipv4(ip)) => ip.is_loopback(),
        Some(Host::Ipv6(ip)) => ip.is_loopback(),
        Some(Host::Domain(domain)) => domain.eq_ignore_ascii_case("localhost"),
        None => false,
    };
    if url.scheme() == "http" && !is_loopback {
        bail!(
            "invalid upstream_base_url: HTTP is allowed only for loopback hosts; use HTTPS otherwise"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECURITY: &[(&str, &str)] = &[
        ("RUSTGATE_TLS_CERT_PATH", "cert.pem"),
        ("RUSTGATE_TLS_KEY_PATH", "key.pem"),
        (
            "RUSTGATE_CONTROL_PLANE_TOKEN",
            "secret-token-012345678901234567890123456789",
        ),
        ("RUSTGATE_UPSTREAM_KEYS", r#"{"primary":"secret-upstream"}"#),
        ("RUSTGATE_PROXY_URL", "https://proxy.example.com/v1"),
    ];

    fn load(env: &[(&str, &str)], file: Option<&str>) -> Result<Config> {
        load_with(
            |name| {
                env.iter()
                    .find(|(key, _)| *key == name)
                    .map(|(_, value)| OsString::from(*value))
            },
            |_| {
                file.map(str::to_owned)
                    .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::NotFound))
            },
        )
    }

    fn secured(env: &[(&str, &str)], file: Option<&str>) -> Result<Config> {
        let mut values = env.to_vec();
        values.extend_from_slice(SECURITY);
        load(&values, file)
    }

    #[test]
    fn required_security_settings_fail_closed() {
        assert!(load(&[], None).is_err());
        for (missing, _) in SECURITY {
            let values: Vec<_> = SECURITY
                .iter()
                .copied()
                .filter(|(name, _)| name != missing)
                .collect();
            assert!(load(&values, None).is_err(), "{missing}");
            assert!(secured(&[(missing, " ")], None).is_err(), "{missing}");
        }
    }

    #[test]
    fn upstream_key_ids_fit_the_issuance_contract() {
        let valid =
            serde_json::to_string(&HashMap::from([("x".repeat(128), "credential")])).unwrap();
        assert!(secured(&[("RUSTGATE_UPSTREAM_KEYS", &valid)], None).is_ok());
        let oversized =
            serde_json::to_string(&HashMap::from([("x".repeat(129), "credential")])).unwrap();
        assert!(secured(&[("RUSTGATE_UPSTREAM_KEYS", &oversized)], None).is_err());
    }

    #[test]
    fn security_settings_load_from_toml() {
        let config = load(
            &[("RUSTGATE_CONFIG", "settings.toml")],
            Some(
                r#"
tls_cert_path = "certificate.pem"
tls_key_path = "private.pem"
control_plane_token = "01234567890123456789012345678901"
proxy_url = "https://internal.example.com/v1"
default_ttl_seconds = 10
max_ttl_seconds = 20
default_max_requests = 3
max_requests = 4
default_max_tokens_total = 5
max_tokens_total = 6
[upstream_keys]
primary = "provider-credential"
"#,
            ),
        )
        .unwrap();
        assert_eq!(config.tls_cert_path, PathBuf::from("certificate.pem"));
        assert_eq!(config.tls_key_path, PathBuf::from("private.pem"));
        assert_eq!(config.control_plane_token.len(), 32);
        assert_eq!(config.proxy_url.as_str(), "https://internal.example.com/v1");
        assert_eq!(
            (config.default_ttl_seconds, config.max_ttl_seconds),
            (10, 20)
        );
        assert_eq!((config.default_max_requests, config.max_requests), (3, 4));
        assert_eq!(
            (config.default_max_tokens_total, config.max_tokens_total),
            (5, 6)
        );
        assert_eq!(config.upstream_keys["primary"], "provider-credential");
    }

    #[test]
    fn file_settings_and_environment_overrides_reach_consumers() {
        let config = secured(&[
            ("RUSTGATE_CONFIG", "settings.toml"),
            ("RUSTGATE_LISTEN_ADDR", "0.0.0.0:9443"),
            ("RUSTGATE_DEFAULT_MAX_REQUESTS", "12"),
            ("RUSTGATE_UPSTREAM_KEYS", r#"{"override":"credential"}"#),
        ], Some("listen_addr = '[::1]:8181'\ndefault_max_requests = 11\nmax_requests = 20\ndatabase_path = 'jobs.db'\n[upstream_keys]\nfile = 'file-credential'" )).unwrap();
        assert_eq!(config.listen_addr.to_string(), "0.0.0.0:9443");
        assert_eq!(config.default_max_requests, 12);
        assert_eq!(config.max_requests, 20);
        assert_eq!(config.database_path, PathBuf::from("jobs.db"));
        assert_eq!(config.upstream_keys.len(), 1);
        assert_eq!(config.upstream_keys["override"], "credential");
        assert!(!format!("{config:?}").contains("credential"));
        assert!(!format!("{config:?}").contains("secret"));
    }

    fn resource_values(config: &Config) -> [u64; 5] {
        [
            config.connect_timeout_seconds,
            config.first_byte_timeout_seconds,
            config.idle_timeout_seconds,
            config.overall_timeout_seconds,
            config.max_in_flight_requests,
        ]
    }

    #[test]
    fn resource_settings_load_from_file_and_environment_takes_precedence() {
        let file = "connect_timeout_seconds = 11\nfirst_byte_timeout_seconds = 12\nidle_timeout_seconds = 13\noverall_timeout_seconds = 14\nmax_in_flight_requests = 15";
        let config = secured(&[("RUSTGATE_CONFIG", "settings.toml")], Some(file)).unwrap();
        assert_eq!(resource_values(&config), [11, 12, 13, 14, 15]);

        let config = secured(
            &[
                ("RUSTGATE_CONFIG", "settings.toml"),
                ("RUSTGATE_CONNECT_TIMEOUT_SECONDS", "21"),
                ("RUSTGATE_FIRST_BYTE_TIMEOUT_SECONDS", "22"),
                ("RUSTGATE_IDLE_TIMEOUT_SECONDS", "23"),
                ("RUSTGATE_OVERALL_TIMEOUT_SECONDS", "24"),
                ("RUSTGATE_MAX_IN_FLIGHT_REQUESTS", "25"),
            ],
            Some(file),
        )
        .unwrap();
        assert_eq!(resource_values(&config), [21, 22, 23, 24, 25]);
    }

    #[test]
    fn resource_bounds_apply_to_file_and_environment() {
        for (field, maximum) in [
            ("connect_timeout_seconds", 86400_u64),
            ("first_byte_timeout_seconds", 86400),
            ("idle_timeout_seconds", 86400),
            ("overall_timeout_seconds", 86400),
            ("max_in_flight_requests", 65536),
        ] {
            let name = format!("RUSTGATE_{}", field.to_ascii_uppercase());
            for value in [1, maximum] {
                let value = value.to_string();
                let file = format!("{field} = {value}");
                assert!(secured(&[(&name, &value)], None).is_ok(), "{field}={value}");
                assert!(
                    secured(&[("RUSTGATE_CONFIG", "settings.toml")], Some(&file)).is_ok(),
                    "{file}"
                );
            }
            for value in [0, maximum + 1, u64::MAX] {
                let value = value.to_string();
                let file = format!("{field} = {value}");
                let error = secured(&[(&name, &value)], None).unwrap_err();
                assert!(error.to_string().contains(field), "{field}={value}");
                assert!(
                    secured(&[("RUSTGATE_CONFIG", "settings.toml")], Some(&file)).is_err(),
                    "{file}"
                );
                // Environment overrides are applied before validating file bounds.
                assert!(
                    secured(
                        &[("RUSTGATE_CONFIG", "settings.toml"), (&name, "2")],
                        Some(&file),
                    )
                    .is_ok(),
                    "{file}"
                );
            }
            for value in ["-1", "not-an-integer", "18446744073709551616"] {
                assert!(secured(&[(&name, value)], None).is_err(), "{field}={value}");
            }
        }
    }

    #[test]
    fn unsafe_urls_and_bounds_are_rejected_without_echo() {
        for (name, values) in [
            (
                "RUSTGATE_UPSTREAM_BASE_URL",
                vec![
                    "secret-invalid",
                    "/relative/secret",
                    "ftp://secret.example.com",
                    "https://",
                    "http://example.com/secret",
                    "http://192.0.2.1/secret",
                    "http://[2001:db8::1]/secret",
                    "http://localhost.example.com/secret",
                    "https://secret-user:secret-password@example.com",
                    "https://:secret-password@example.com",
                    "https://secret-user@example.com",
                    "https://example.com/?",
                    "https://example.com/#",
                ],
            ),
            (
                "RUSTGATE_PROXY_URL",
                vec![
                    "http://localhost/v1",
                    "https://example.com/",
                    "https://example.com/v1/",
                    "https://user:secret@example.com/v1",
                    "https://example.com/v1?",
                    "https://example.com/v1#",
                    "secret-invalid",
                ],
            ),
            (
                "RUSTGATE_CONTROL_PLANE_TOKEN",
                vec![
                    "secret-short",
                    "secret-012345678901234567890123456789 ",
                    "secret-012345678901234567890123456789é",
                ],
            ),
            (
                "RUSTGATE_UPSTREAM_KEYS",
                vec![
                    "secret-invalid",
                    "{}",
                    r#"{"":"secret"}"#,
                    r#"{"id":"secret key"}"#,
                    r#"{"id":1}"#,
                ],
            ),
            (
                "RUSTGATE_DATABASE_PATH",
                vec!["", ":memory:", "file::memory:", "file:jobs?mode=memory"],
            ),
            (
                "RUSTGATE_DEFAULT_TTL_SECONDS",
                vec!["0", "86401", "-1", "secret"],
            ),
            ("RUSTGATE_MAX_TTL_SECONDS", vec!["31536001", "0"]),
            ("RUSTGATE_DEFAULT_MAX_REQUESTS", vec!["0", "10001"]),
            ("RUSTGATE_MAX_REQUESTS", vec!["0", "9223372036854775808"]),
            ("RUSTGATE_DEFAULT_MAX_TOKENS_TOTAL", vec!["0", "100000001"]),
            (
                "RUSTGATE_MAX_TOKENS_TOTAL",
                vec!["0", "9223372036854775808"],
            ),
        ] {
            for value in values {
                let error = secured(&[(name, value)], None).unwrap_err();
                assert!(!format!("{error:#}").contains("secret"), "{name}");
            }
        }
    }

    #[test]
    fn upstream_accepts_loopback_http_and_remote_https() {
        for value in [
            "http://127.0.0.1:9090",
            "http://127.10.20.30:9090/v1",
            "http://[::1]:9090",
            "http://localhost:9090",
            "https://api.example.com/v1",
            "https://192.0.2.1:8443",
        ] {
            assert!(secured(&[("RUSTGATE_UPSTREAM_BASE_URL", value)], None).is_ok());
        }
    }

    #[test]
    fn file_errors_are_redacted_even_with_overrides() {
        for contents in [
            None,
            Some("upstream_base_url = 'secret-unclosed"),
            Some("secret_unknown_key = 'secret-value'"),
            Some("listen_addr = ['secret-value']"),
            Some("max_requests = 'secret-value'"),
            Some("default_max_requests = -1 # secret"),
        ] {
            let error = secured(&[("RUSTGATE_CONFIG", "secret-path.toml")], contents).unwrap_err();
            let message = format!("{error:#}");
            assert!(message.contains("configuration file"));
            assert!(!message.contains("secret"));
        }
    }

    #[test]
    fn policy_defaults_are_enabled_and_debug_stays_redacted() {
        let config = secured(&[], None).unwrap();
        assert_eq!(
            config.secret_scan_patterns,
            Config::default_secret_scan_patterns()
        );
        assert_eq!(
            config.prompt_injection_patterns,
            Config::default_prompt_injection_patterns()
        );
        let config = secured(
            &[
                (
                    "RUSTGATE_SECRET_SCAN_PATTERNS",
                    r#"["private-regex-value"]"#,
                ),
                (
                    "RUSTGATE_PROMPT_INJECTION_PATTERNS",
                    r#"["private-advisory-value"]"#,
                ),
            ],
            None,
        )
        .unwrap();
        let debug = format!("{config:?}");
        assert!(!debug.contains("private-regex-value"));
        assert!(!debug.contains("private-advisory-value"));
    }

    #[test]
    fn policy_arrays_replace_defaults_and_environment_replaces_file() {
        let file =
            "secret_scan_patterns = ['file-secret']\nprompt_injection_patterns = ['file-advisory']";
        let config = secured(&[("RUSTGATE_CONFIG", "settings.toml")], Some(file)).unwrap();
        assert_eq!(config.secret_scan_patterns, ["file-secret"]);
        assert_eq!(config.prompt_injection_patterns, ["file-advisory"]);

        let config = secured(
            &[
                ("RUSTGATE_CONFIG", "settings.toml"),
                ("RUSTGATE_SECRET_SCAN_PATTERNS", "[]"),
                (
                    "RUSTGATE_PROMPT_INJECTION_PATTERNS",
                    r#"["env-advisory", "second-advisory"]"#,
                ),
            ],
            Some(file),
        )
        .unwrap();
        assert!(config.secret_scan_patterns.is_empty());
        assert_eq!(
            config.prompt_injection_patterns,
            ["env-advisory", "second-advisory"]
        );
        let config = secured(
            &[("RUSTGATE_CONFIG", "settings.toml")],
            Some("secret_scan_patterns = []\nprompt_injection_patterns = []"),
        )
        .unwrap();
        assert!(config.secret_scan_patterns.is_empty());
        assert!(config.prompt_injection_patterns.is_empty());
    }

    #[test]
    fn malformed_policy_arrays_fail_without_echoing_supplied_values() {
        for field in ["secret_scan_patterns", "prompt_injection_patterns"] {
            let name = format!("RUSTGATE_{}", field.to_ascii_uppercase());
            for value in [
                "private-config-value",
                r#"{"private-config-value":"regex"}"#,
                r#"["private-config-value", 7]"#,
                r#""private-config-value""#,
            ] {
                let error = secured(&[(&name, value)], None).unwrap_err();
                let message = format!("{error:#}");
                assert!(message.contains(field));
                assert!(!message.contains("private-config-value"));
            }
            let file = format!("{field} = ['private-config-value', 7]");
            let error = secured(&[("RUSTGATE_CONFIG", "settings.toml")], Some(&file)).unwrap_err();
            assert!(!format!("{error:#}").contains("private-config-value"));
        }
    }

    #[test]
    fn policy_array_size_limits_apply_without_echoing_patterns() {
        for field in ["secret_scan_patterns", "prompt_injection_patterns"] {
            let name = format!("RUSTGATE_{}", field.to_ascii_uppercase());
            for patterns in [
                vec!["private-config-value".to_owned(); 65],
                vec!["private-config-value".repeat(300)],
                vec!["private-config-value".repeat(200); 17],
            ] {
                let value = serde_json::to_string(&patterns).unwrap();
                let error = secured(&[(&name, &value)], None).unwrap_err();
                let message = format!("{error:#}");
                assert!(message.contains(field));
                assert!(!message.contains("private-config-value"));
                let file = format!("{field} = {value}");
                let error =
                    secured(&[("RUSTGATE_CONFIG", "settings.toml")], Some(&file)).unwrap_err();
                assert!(!format!("{error:#}").contains("private-config-value"));
            }
        }
    }

    #[cfg(unix)]
    #[test]
    fn non_unicode_environment_is_rejected_without_echoing_bytes() {
        use std::os::unix::ffi::OsStringExt;
        for name in [
            "RUSTGATE_CONFIG",
            "RUSTGATE_LISTEN_ADDR",
            "RUSTGATE_UPSTREAM_BASE_URL",
            "RUSTGATE_CONTROL_PLANE_TOKEN",
            "RUSTGATE_UPSTREAM_KEYS",
            "RUSTGATE_MAX_REQUESTS",
        ] {
            let error = load_with(
                |key| (key == name).then(|| OsString::from_vec(b"secret\xff".to_vec())),
                |_| panic!("non-Unicode configuration path must not be read"),
            )
            .unwrap_err();
            let message = format!("{error:#}");
            assert!(message.contains(name));
            assert!(!message.contains("secret"));
        }
    }
}
