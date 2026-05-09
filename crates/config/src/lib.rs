//! YAML configuration for the gateway.
//!
//! Two equivalent forms are accepted for backend discovery, to keep
//! Phase 1 deployments unchanged while Phase 2 introduces dynamic
//! sources:
//!
//! ```yaml
//! # Phase 1 shorthand (still valid):
//! backends:
//!   - "host1:15002"
//!   - "host2:15002"
//! ```
//!
//! ```yaml
//! # Tagged form (Phase 2):
//! backend_discovery:
//!   type: static
//!   addresses: ["host1:15002", "host2:15002"]
//! ```
//!
//! ```yaml
//! # Tagged form (Phase 2, K8s):
//! backend_discovery:
//!   type: k8s
//!   namespace: spark-connect
//!   service_name: spark-connect
//!   port: 15002
//! ```

use serde::Deserialize;
use std::path::Path;

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("read config {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("parse config {path}: {source}")]
    Parse {
        path: String,
        #[source]
        source: serde_yaml::Error,
    },
    #[error("config: must specify either `backends` or `backend_discovery`")]
    NoDiscoverySource,
    #[error("config: cannot specify both `backends` and `backend_discovery`")]
    ConflictingDiscovery,
    #[error("config: static backend list must contain at least one address")]
    EmptyStatic,
}

/// One of the supported backend discovery sources.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BackendDiscovery {
    /// Fixed list of `host:port` addresses, configured at startup.
    Static { addresses: Vec<String> },
    /// Watch a Kubernetes Service's Endpoints object.
    K8s {
        namespace: String,
        service_name: String,
        port: u16,
    },
}

/// Authentication configuration. Defaults to `none` so Phase 1 configs
/// keep working unchanged.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AuthConfig {
    /// No auth — anyone reaching the gateway is `user_id: anonymous`.
    /// Acceptable on a trusted network; **not** for production.
    #[default]
    None,
    /// Bearer-token allowlist. See [`scg-auth::token`].
    Static { tokens: Vec<TokenEntry> },
    /// Local-key JWT verification. See [`scg-auth::jwt`].
    Jwt(JwtSettings),
    /// Remote JWKS / OIDC verification. See [`scg-auth::oidc`].
    Oidc(OidcSettings),
}

/// One entry in the static-token table — kept here (rather than only in
/// `scg-auth`) so config files can describe auth without depending on
/// the auth crate's serde shape.
#[derive(Debug, Clone, Deserialize)]
pub struct TokenEntry {
    pub token: String,
    pub user_id: String,
    #[serde(default)]
    pub tenant: Option<String>,
    #[serde(default)]
    pub groups: Vec<String>,
}

/// JWT verification settings; mirrors `scg_auth::jwt::JwtConfig`.
#[derive(Debug, Clone, Deserialize)]
pub struct JwtSettings {
    pub key: KeySource,
    pub algorithms: Vec<String>,
    #[serde(default)]
    pub issuer: Option<String>,
    #[serde(default)]
    pub audience: Option<String>,
    #[serde(default = "default_user_id_claim")]
    pub user_id_claim: String,
    #[serde(default)]
    pub tenant_claim: Option<String>,
    #[serde(default)]
    pub groups_claim: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum KeySource {
    PemFile { path: String },
    PemInline { pem: String },
    HmacSecret { secret: String },
}

/// OIDC verification settings; mirrors `scg_auth::oidc::OidcConfig`.
#[derive(Debug, Clone, Deserialize)]
pub struct OidcSettings {
    #[serde(default)]
    pub jwks_url: Option<String>,
    #[serde(default)]
    pub discovery_url: Option<String>,
    pub algorithms: Vec<String>,
    #[serde(default)]
    pub issuer: Option<String>,
    #[serde(default)]
    pub audience: Option<String>,
    #[serde(default = "default_user_id_claim")]
    pub user_id_claim: String,
    #[serde(default)]
    pub tenant_claim: Option<String>,
    #[serde(default)]
    pub groups_claim: Option<String>,
    #[serde(default = "default_refresh_floor_secs")]
    pub refresh_floor_secs: u64,
}

fn default_user_id_claim() -> String {
    "sub".into()
}
fn default_refresh_floor_secs() -> u64 {
    60
}

/// Distributed-tracing configuration. Off by default — Phase-1 configs
/// without a `tracing:` section keep working.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct TracingSettings {
    /// OTLP/gRPC collector endpoint (e.g. `http://otel-collector:4317`).
    /// `None` disables span export — only the JSON log formatter runs.
    #[serde(default)]
    pub endpoint: Option<String>,
    /// `service.name` resource attribute reported on every span.
    #[serde(default = "default_service_name")]
    pub service_name: String,
    /// `service.version` resource attribute. Defaults to the
    /// gateway's compile-time CARGO_PKG_VERSION when omitted.
    #[serde(default)]
    pub service_version: Option<String>,
    /// `TraceIdRatioBased` sampling ratio in `[0.0, 1.0]`. Wrapped in
    /// `ParentBased` at runtime so a sampled remote parent always wins.
    #[serde(default = "default_sample_ratio")]
    pub sample_ratio: f64,
    /// Per-batch OTLP export deadline, in seconds.
    #[serde(default = "default_export_timeout_secs")]
    pub export_timeout_secs: u64,
}

fn default_service_name() -> String {
    "spark-connect-gateway".into()
}
fn default_sample_ratio() -> f64 {
    1.0
}
fn default_export_timeout_secs() -> u64 {
    10
}

/// Raw YAML shape — accepts either the legacy `backends` shorthand or the
/// tagged `backend_discovery` form, never both.
#[derive(Debug, Deserialize)]
struct RawConfig {
    #[serde(default = "default_bind_addr")]
    bind_addr: String,
    #[serde(default)]
    backends: Option<Vec<String>>,
    #[serde(default)]
    backend_discovery: Option<BackendDiscovery>,
    #[serde(default)]
    auth: Option<AuthConfig>,
    /// Address for the admin / metrics HTTP server. `null` disables it.
    /// Default `0.0.0.0:9090`.
    #[serde(default = "default_admin_addr_opt")]
    admin_addr: Option<String>,
    #[serde(default)]
    tracing: Option<TracingSettings>,
}

fn default_admin_addr_opt() -> Option<String> {
    Some(":9090".into())
}

#[derive(Debug, Clone)]
pub struct Config {
    pub bind_addr: String,
    pub discovery: BackendDiscovery,
    pub auth: AuthConfig,
    /// `Some(addr)` to enable the admin HTTP server, `None` to skip it.
    pub admin_addr: Option<String>,
    /// Distributed-tracing settings. `None` keeps tracing off (the
    /// gateway only emits structured JSON logs in that case).
    pub tracing: Option<TracingSettings>,
}

fn default_bind_addr() -> String {
    ":15003".into()
}

impl Config {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path_str = path.as_ref().display().to_string();
        let data = std::fs::read_to_string(path.as_ref()).map_err(|e| ConfigError::Io {
            path: path_str.clone(),
            source: e,
        })?;
        let raw: RawConfig = serde_yaml::from_str(&data).map_err(|e| ConfigError::Parse {
            path: path_str,
            source: e,
        })?;
        Self::from_raw(raw)
    }

    fn from_raw(raw: RawConfig) -> Result<Self, ConfigError> {
        let discovery = match (raw.backends, raw.backend_discovery) {
            (Some(_), Some(_)) => return Err(ConfigError::ConflictingDiscovery),
            (None, None) => return Err(ConfigError::NoDiscoverySource),
            (Some(addrs), None) => {
                if addrs.is_empty() {
                    return Err(ConfigError::EmptyStatic);
                }
                BackendDiscovery::Static { addresses: addrs }
            }
            (None, Some(d)) => {
                if let BackendDiscovery::Static { addresses } = &d {
                    if addresses.is_empty() {
                        return Err(ConfigError::EmptyStatic);
                    }
                }
                d
            }
        };
        let bind_addr = if raw.bind_addr.is_empty() {
            default_bind_addr()
        } else {
            raw.bind_addr
        };
        Ok(Self {
            bind_addr,
            discovery,
            auth: raw.auth.unwrap_or_default(),
            admin_addr: raw.admin_addr.filter(|s| !s.is_empty()),
            tracing: raw.tracing,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write(text: &str) -> tempfile::NamedTempFile {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        write!(f, "{}", text).unwrap();
        f
    }

    #[test]
    fn loads_legacy_backends_shorthand() {
        let f = write(
            r#"
bind_addr: ":15003"
backends:
  - "127.0.0.1:15002"
"#,
        );
        let c = Config::load(f.path()).unwrap();
        assert_eq!(c.bind_addr, ":15003");
        match c.discovery {
            BackendDiscovery::Static { addresses } => {
                assert_eq!(addresses, vec!["127.0.0.1:15002"]);
            }
            other => panic!("expected Static, got {:?}", other),
        }
    }

    #[test]
    fn loads_tagged_static() {
        let f = write(
            r#"
backend_discovery:
  type: static
  addresses: ["a:1", "b:2"]
"#,
        );
        let c = Config::load(f.path()).unwrap();
        match c.discovery {
            BackendDiscovery::Static { addresses } => {
                assert_eq!(addresses, vec!["a:1", "b:2"]);
            }
            other => panic!("expected Static, got {:?}", other),
        }
    }

    #[test]
    fn loads_tagged_k8s() {
        let f = write(
            r#"
backend_discovery:
  type: k8s
  namespace: spark-connect
  service_name: spark-connect
  port: 15002
"#,
        );
        let c = Config::load(f.path()).unwrap();
        match c.discovery {
            BackendDiscovery::K8s {
                namespace,
                service_name,
                port,
            } => {
                assert_eq!(namespace, "spark-connect");
                assert_eq!(service_name, "spark-connect");
                assert_eq!(port, 15002);
            }
            other => panic!("expected K8s, got {:?}", other),
        }
    }

    #[test]
    fn empty_backends_shorthand_rejected() {
        let f = write("backends: []\n");
        assert!(matches!(
            Config::load(f.path()).unwrap_err(),
            ConfigError::EmptyStatic
        ));
    }

    #[test]
    fn empty_static_in_tagged_form_rejected() {
        let f = write(
            r#"
backend_discovery:
  type: static
  addresses: []
"#,
        );
        assert!(matches!(
            Config::load(f.path()).unwrap_err(),
            ConfigError::EmptyStatic
        ));
    }

    #[test]
    fn missing_discovery_rejected() {
        let f = write("bind_addr: ':15003'\n");
        assert!(matches!(
            Config::load(f.path()).unwrap_err(),
            ConfigError::NoDiscoverySource
        ));
    }

    #[test]
    fn conflicting_discovery_rejected() {
        let f = write(
            r#"
backends: ["a:1"]
backend_discovery:
  type: static
  addresses: ["b:2"]
"#,
        );
        assert!(matches!(
            Config::load(f.path()).unwrap_err(),
            ConfigError::ConflictingDiscovery
        ));
    }

    #[test]
    fn defaults_bind_addr() {
        let f = write("backends: [\"a:1\"]\n");
        let c = Config::load(f.path()).unwrap();
        assert_eq!(c.bind_addr, ":15003");
    }

    #[test]
    fn auth_defaults_to_none() {
        let f = write("backends: [\"a:1\"]\n");
        let c = Config::load(f.path()).unwrap();
        assert!(matches!(c.auth, AuthConfig::None));
    }

    #[test]
    fn loads_static_auth() {
        let f = write(
            r#"
backends: ["a:1"]
auth:
  type: static
  tokens:
    - token: "alice-secret"
      user_id: "alice"
      tenant: "team-a"
      groups: ["devs"]
    - token: "bob-secret"
      user_id: "bob"
"#,
        );
        let c = Config::load(f.path()).unwrap();
        match c.auth {
            AuthConfig::Static { tokens } => {
                assert_eq!(tokens.len(), 2);
                assert_eq!(tokens[0].user_id, "alice");
                assert_eq!(tokens[0].tenant.as_deref(), Some("team-a"));
                assert_eq!(tokens[1].user_id, "bob");
                assert!(tokens[1].tenant.is_none());
            }
            other => panic!("expected Static, got {:?}", other),
        }
    }

    #[test]
    fn loads_jwt_auth() {
        let f = write(
            r#"
backends: ["a:1"]
auth:
  type: jwt
  algorithms: ["RS256"]
  issuer: "https://idp.example.com"
  audience: "spark-connect-gateway"
  key:
    kind: pem_file
    path: "/etc/gateway/idp-pub.pem"
"#,
        );
        let c = Config::load(f.path()).unwrap();
        match c.auth {
            AuthConfig::Jwt(s) => {
                assert_eq!(s.algorithms, vec!["RS256"]);
                assert_eq!(s.issuer.as_deref(), Some("https://idp.example.com"));
                match s.key {
                    KeySource::PemFile { path } => assert_eq!(path, "/etc/gateway/idp-pub.pem"),
                    other => panic!("expected PemFile, got {:?}", other),
                }
            }
            other => panic!("expected Jwt, got {:?}", other),
        }
    }

    #[test]
    fn tracing_defaults_to_off() {
        let f = write("backends: [\"a:1\"]\n");
        let c = Config::load(f.path()).unwrap();
        assert!(c.tracing.is_none());
    }

    #[test]
    fn loads_tracing_block() {
        let f = write(
            r#"
backends: ["a:1"]
tracing:
  endpoint: "http://otel-collector:4317"
  service_name: "scg-staging"
  sample_ratio: 0.25
  export_timeout_secs: 5
"#,
        );
        let c = Config::load(f.path()).unwrap();
        let t = c.tracing.expect("tracing settings parsed");
        assert_eq!(t.endpoint.as_deref(), Some("http://otel-collector:4317"));
        assert_eq!(t.service_name, "scg-staging");
        assert!((t.sample_ratio - 0.25).abs() < 1e-9);
        assert_eq!(t.export_timeout_secs, 5);
    }

    #[test]
    fn tracing_block_endpoint_can_be_omitted_for_log_only() {
        // A `tracing:` block without an endpoint is legal — the gateway
        // skips OTLP export but still respects the other knobs (e.g.
        // service_name) for when the user later sets an endpoint.
        let f = write(
            r#"
backends: ["a:1"]
tracing:
  service_name: "scg-test"
"#,
        );
        let c = Config::load(f.path()).unwrap();
        let t = c.tracing.expect("tracing settings parsed");
        assert!(t.endpoint.is_none());
        assert_eq!(t.service_name, "scg-test");
        // Defaults round-trip:
        assert!((t.sample_ratio - 1.0).abs() < 1e-9);
        assert_eq!(t.export_timeout_secs, 10);
    }

    #[test]
    fn loads_oidc_auth() {
        let f = write(
            r#"
backends: ["a:1"]
auth:
  type: oidc
  algorithms: ["RS256"]
  discovery_url: "https://idp.example.com/.well-known/openid-configuration"
  audience: "spark-connect-gateway"
"#,
        );
        let c = Config::load(f.path()).unwrap();
        match c.auth {
            AuthConfig::Oidc(s) => {
                assert_eq!(
                    s.discovery_url.as_deref(),
                    Some("https://idp.example.com/.well-known/openid-configuration")
                );
                assert!(s.jwks_url.is_none());
            }
            other => panic!("expected Oidc, got {:?}", other),
        }
    }
}
