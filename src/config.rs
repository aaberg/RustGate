use std::{ffi::OsString, net::SocketAddr, path::Path};

use anyhow::{Result, anyhow, bail};
use serde::Deserialize;
use url::{Host, Url};

const DEFAULT_LISTEN_ADDR: &str = "127.0.0.1:8080";
const DEFAULT_UPSTREAM_BASE_URL: &str = "http://127.0.0.1:9090";

#[derive(Debug)]
pub struct Config {
    pub listen_addr: SocketAddr,
    pub upstream_base_url: Url,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileConfig {
    listen_addr: Option<String>,
    upstream_base_url: Option<String>,
}

impl Config {
    pub fn load() -> Result<Self> {
        load_with(
            |name| std::env::var_os(name),
            |path| std::fs::read_to_string(path),
        )
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
            toml::from_str::<FileConfig>(&contents).map_err(|_| {
                anyhow!(
                    "invalid configuration file selected by RUSTGATE_CONFIG: expected TOML with only string settings listen_addr and upstream_base_url"
                )
            })?
        }
        None => FileConfig::default(),
    };

    if let Some(value) = env_value(&mut env, "RUSTGATE_LISTEN_ADDR")? {
        settings.listen_addr = Some(value);
    }
    if let Some(value) = env_value(&mut env, "RUSTGATE_UPSTREAM_BASE_URL")? {
        settings.upstream_base_url = Some(value);
    }

    let listen_addr: SocketAddr = settings
        .listen_addr
        .as_deref()
        .unwrap_or(DEFAULT_LISTEN_ADDR)
        .parse()
        .map_err(|_| anyhow!("invalid listen_addr: expected an IP socket address"))?;
    if !listen_addr.ip().is_loopback() {
        bail!("invalid listen_addr: only loopback addresses are allowed without TLS");
    }

    let upstream_base_url = Url::parse(
        settings
            .upstream_base_url
            .as_deref()
            .unwrap_or(DEFAULT_UPSTREAM_BASE_URL),
    )
    .map_err(|_| anyhow!("invalid upstream_base_url: expected an absolute HTTP(S) URL"))?;
    validate_upstream(&upstream_base_url)?;

    Ok(Config {
        listen_addr,
        upstream_base_url,
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

    #[test]
    fn defaults_and_partial_file_environment_precedence() {
        let defaults = load(&[], None).unwrap();
        assert_eq!(defaults.listen_addr.to_string(), DEFAULT_LISTEN_ADDR);
        assert_eq!(
            defaults.upstream_base_url.as_str(),
            "http://127.0.0.1:9090/"
        );

        let partial = load(
            &[("RUSTGATE_CONFIG", "settings.toml")],
            Some("listen_addr = '[::1]:8181'"),
        )
        .unwrap();
        assert_eq!(partial.listen_addr.to_string(), "[::1]:8181");
        assert_eq!(partial.upstream_base_url, defaults.upstream_base_url);

        let overridden = load(
            &[
                ("RUSTGATE_CONFIG", "settings.toml"),
                ("RUSTGATE_UPSTREAM_BASE_URL", "https://api.example.com/v1"),
            ],
            Some("listen_addr = '127.0.0.2:8181'"),
        )
        .unwrap();
        assert_eq!(overridden.listen_addr.to_string(), "127.0.0.2:8181");
        assert_eq!(
            overridden.upstream_base_url.as_str(),
            "https://api.example.com/v1"
        );

        let overridden = load(
            &[
                ("RUSTGATE_CONFIG", "settings.toml"),
                ("RUSTGATE_LISTEN_ADDR", "127.0.0.1:8282"),
                ("RUSTGATE_UPSTREAM_BASE_URL", "http://[::1]:9091"),
            ],
            Some("listen_addr = '127.0.0.1:8181'\nupstream_base_url = 'https://api.example.com'"),
        )
        .unwrap();
        assert_eq!(overridden.listen_addr.to_string(), "127.0.0.1:8282");
        assert_eq!(overridden.upstream_base_url.as_str(), "http://[::1]:9091/");
    }

    #[test]
    fn explicit_missing_file_fails_even_with_environment_overrides() {
        let error = load(
            &[
                ("RUSTGATE_CONFIG", "missing-secret-path.toml"),
                ("RUSTGATE_LISTEN_ADDR", "127.0.0.1:8081"),
                ("RUSTGATE_UPSTREAM_BASE_URL", "https://api.example.com"),
            ],
            None,
        )
        .unwrap_err();
        let message = format!("{error:#}");
        assert!(message.contains("configuration file"));
        assert!(message.contains("RUSTGATE_CONFIG"));
        assert!(!message.contains("missing-secret-path"));
    }

    #[test]
    fn malformed_unknown_and_wrong_type_file_settings_are_redacted() {
        for contents in [
            "upstream_base_url = 'secret-unclosed",
            "secret_unknown_key = 'secret-value'",
            "listen_addr = ['secret-value']",
            "upstream_base_url = 12345 # secret-value",
        ] {
            let error = load(
                &[
                    ("RUSTGATE_CONFIG", "settings.toml"),
                    ("RUSTGATE_UPSTREAM_BASE_URL", "https://api.example.com"),
                ],
                Some(contents),
            )
            .unwrap_err();
            let message = format!("{error:#}");
            assert!(message.contains("configuration file"));
            assert!(!message.contains("secret"));
        }
    }

    #[test]
    fn listen_address_requires_a_loopback_ip_socket() {
        for value in [
            "secret-invalid",
            "localhost:8080",
            "0.0.0.0:8080",
            "[::]:8080",
            "192.0.2.1:8080",
        ] {
            let error = load(&[("RUSTGATE_LISTEN_ADDR", value)], None).unwrap_err();
            let message = format!("{error:#}");
            assert!(message.contains("listen_addr"));
            assert!(!message.contains(value));
        }
        for value in ["127.0.0.1:0", "127.10.20.30:8080", "[::1]:8080"] {
            assert!(load(&[("RUSTGATE_LISTEN_ADDR", value)], None).is_ok());
        }
    }

    #[test]
    fn upstream_rejects_unsafe_or_invalid_urls_without_echoing_them() {
        for value in [
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
            "https://example.com/?secret",
            "https://example.com/#secret",
            "https://example.com/?",
            "https://example.com/#",
        ] {
            let error = load(&[("RUSTGATE_UPSTREAM_BASE_URL", value)], None).unwrap_err();
            let message = format!("{error:#}");
            assert!(message.contains("upstream_base_url"));
            assert!(!message.contains(value));
            assert!(!message.contains("secret"));
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
            assert!(load(&[("RUSTGATE_UPSTREAM_BASE_URL", value)], None).is_ok());
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
