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

/// Where the gateway keeps its `(user_id, session_id) -> backend`
/// affinity table. Default `memory` keeps the Phase-1 in-process
/// behaviour. `redis` is required for HA across multiple gateway
/// replicas — without it, two replicas will pin the same session to
/// different backends and Spark Connect's per-driver session state
/// stops being consistent.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AffinityStoreConfig {
    #[default]
    Memory,
    Redis(RedisStoreSettings),
}

#[derive(Debug, Clone, Deserialize)]
pub struct RedisStoreSettings {
    /// `redis://` URL. Supports password (`redis://:pw@host:6379`)
    /// and database index (`redis://host:6379/2`).
    pub url: String,
    /// Key prefix; lets multiple gateway deployments share a Redis
    /// without colliding. Default `scg`.
    #[serde(default = "default_redis_prefix")]
    pub key_prefix: String,
    /// TTL for `(user_id, session_id) -> backend` bindings (seconds).
    /// Refreshed on every read.
    #[serde(default = "default_session_ttl_secs")]
    pub session_ttl_secs: u64,
    /// TTL for `op_id -> backend` bindings (seconds).
    #[serde(default = "default_op_ttl_secs")]
    pub op_ttl_secs: u64,
}

fn default_redis_prefix() -> String {
    "scg".into()
}
fn default_session_ttl_secs() -> u64 {
    60 * 60
}
fn default_op_ttl_secs() -> u64 {
    15 * 60
}

/// Per-tenant backend pool overrides (Phase 3). A multi-tenant
/// deployment lists one entry per tenant that needs its own pool;
/// any tenant *not* listed here routes through the deployment's
/// default pool (the existing `backends:` / `backend_discovery:`
/// settings).
///
/// The fallback `policy` decides what happens when an inbound RPC
/// carries a tenant that has neither an explicit override nor (in
/// the `Reject` case) any pool at all. Default `UseDefault` matches
/// Phase 1/2 single-tenant deployments — everything routes to the
/// default pool. `Reject` is the right choice for SaaS-style
/// deployments where unconfigured tenants must not get any access.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct TenantPoolsSettings {
    /// Tenant name → its own pool's discovery configuration. The
    /// `default` tenant is **not** special here; if you want the
    /// default pool to be different from what `backends:` /
    /// `backend_discovery:` provides, list it as an override too.
    #[serde(default)]
    pub overrides: std::collections::HashMap<String, BackendDiscovery>,
    /// What to do when an inbound RPC has a tenant that's not in
    /// `overrides`. `use_default` (default) routes through the
    /// deployment's default pool; `reject` returns
    /// `PermissionDenied` to the client.
    #[serde(default = "default_unknown_tenant_policy")]
    pub on_unknown_tenant: UnknownTenantPolicySetting,
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnknownTenantPolicySetting {
    #[default]
    UseDefault,
    Reject,
}

fn default_unknown_tenant_policy() -> UnknownTenantPolicySetting {
    UnknownTenantPolicySetting::UseDefault
}

/// How the gateway figures out which tenant an inbound RPC belongs
/// to. The resolved tenant becomes the first segment of the routing
/// key, so two tenants with the same `session_id` get isolated
/// affinity buckets.
///
/// Phase 1/2 deployments without a `tenant_resolver:` block fall
/// back to `from_claim + use_default + "default"`, which is the
/// pre-Phase-3 behaviour (every RPC ends up in `tenant="default"`).
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "source", rename_all = "snake_case")]
pub enum TenantResolverSource {
    /// Read from `Identity.tenant` produced by the auth interceptor
    /// (JWT/OIDC `tenant` claim, static-token `tenant` field).
    FromClaim,
    /// Read from a gRPC metadata header. For deployments where auth
    /// is disabled but clients still cooperate by declaring a
    /// tenant.
    FromMetadata {
        #[serde(default = "default_tenant_header")]
        header: String,
    },
    /// Always use `default_name`. Single-tenant deployments running
    /// Phase 3 code without bothering with auth claims or headers.
    AlwaysDefault,
}

fn default_tenant_header() -> String {
    "x-tenant".into()
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TenantOnMissing {
    /// Fall back to `default_name` when the source yields nothing.
    /// Back-compat default for Phase 1/2 upgrades.
    UseDefault,
    /// Fail the RPC with `Unauthenticated`. Used by SaaS-style
    /// deployments where a missing tenant claim almost always means
    /// the IdP is misconfigured.
    Reject,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TenantResolverSettings {
    #[serde(flatten)]
    pub source: TenantResolverSource,
    #[serde(default = "default_tenant_on_missing")]
    pub on_missing: TenantOnMissing,
    #[serde(default = "default_tenant_name")]
    pub default_name: String,
}

fn default_tenant_on_missing() -> TenantOnMissing {
    TenantOnMissing::UseDefault
}
fn default_tenant_name() -> String {
    "default".into()
}

impl Default for TenantResolverSettings {
    fn default() -> Self {
        Self {
            source: TenantResolverSource::FromClaim,
            on_missing: TenantOnMissing::UseDefault,
            default_name: default_tenant_name(),
        }
    }
}

/// Active gRPC health-check probing for backend pool members. Wraps
/// the configured pool with a probe loop that calls
/// `grpc.health.v1.Health/Check` on each backend and removes
/// repeatedly-failing ones from `pick()`. Off by default to avoid
/// breaking deployments where backends don't ship the standard
/// Health service.
#[derive(Debug, Clone, Deserialize)]
pub struct HealthCheckSettings {
    /// Master switch. `false` (default) keeps the Phase-1 passive
    /// behaviour: routing fails through to the next session on a
    /// forward error.
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_hc_interval_secs")]
    pub interval_secs: u64,
    #[serde(default = "default_hc_timeout_secs")]
    pub timeout_secs: u64,
    #[serde(default = "default_hc_unhealthy_threshold")]
    pub unhealthy_threshold: u32,
    #[serde(default = "default_hc_healthy_threshold")]
    pub healthy_threshold: u32,
}

impl Default for HealthCheckSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            interval_secs: default_hc_interval_secs(),
            timeout_secs: default_hc_timeout_secs(),
            unhealthy_threshold: default_hc_unhealthy_threshold(),
            healthy_threshold: default_hc_healthy_threshold(),
        }
    }
}

fn default_hc_interval_secs() -> u64 {
    5
}
fn default_hc_timeout_secs() -> u64 {
    2
}
fn default_hc_unhealthy_threshold() -> u32 {
    3
}
fn default_hc_healthy_threshold() -> u32 {
    2
}

/// Graceful shutdown behaviour. On SIGINT/SIGTERM, the gateway
/// flips `/readyz` to 503 (so K8s drains it from the Service), then
/// waits for in-flight streaming RPCs (`ExecutePlan`,
/// `ReattachExecute`, `AddArtifacts`) to complete, up to
/// `deadline_secs`.
#[derive(Debug, Clone, Deserialize)]
pub struct ShutdownSettings {
    /// Hard ceiling on the drain period. After this many seconds the
    /// gateway forcibly shuts down regardless of in-flight streams.
    /// Pick something compatible with your K8s
    /// `terminationGracePeriodSeconds` (the chart defaults to 30).
    #[serde(default = "default_shutdown_deadline_secs")]
    pub deadline_secs: u64,
}

impl Default for ShutdownSettings {
    fn default() -> Self {
        Self {
            deadline_secs: default_shutdown_deadline_secs(),
        }
    }
}

fn default_shutdown_deadline_secs() -> u64 {
    30
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
    #[serde(default)]
    affinity_store: Option<AffinityStoreConfig>,
    #[serde(default)]
    health_check: Option<HealthCheckSettings>,
    #[serde(default)]
    shutdown: Option<ShutdownSettings>,
    #[serde(default)]
    tenant_resolver: Option<TenantResolverSettings>,
    #[serde(default)]
    tenant_pools: Option<TenantPoolsSettings>,
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
    /// Where to keep affinity state. Defaults to in-memory; use
    /// `redis` for multi-replica HA.
    pub affinity_store: AffinityStoreConfig,
    /// Active gRPC health-check probing for backends. Off by default.
    pub health_check: HealthCheckSettings,
    /// Graceful shutdown / drain settings.
    pub shutdown: ShutdownSettings,
    /// How to figure out the tenant for each inbound RPC. Defaults
    /// to the back-compat behaviour (every RPC -> tenant="default").
    pub tenant_resolver: TenantResolverSettings,
    /// Per-tenant backend pool overrides + unknown-tenant policy.
    /// Empty `overrides` + `use_default` reproduces Phase 1/2
    /// single-pool behaviour.
    pub tenant_pools: TenantPoolsSettings,
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
            affinity_store: raw.affinity_store.unwrap_or_default(),
            health_check: raw.health_check.unwrap_or_default(),
            shutdown: raw.shutdown.unwrap_or_default(),
            tenant_resolver: raw.tenant_resolver.unwrap_or_default(),
            tenant_pools: raw.tenant_pools.unwrap_or_default(),
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
    fn affinity_store_defaults_to_memory() {
        let f = write("backends: [\"a:1\"]\n");
        let c = Config::load(f.path()).unwrap();
        assert!(matches!(c.affinity_store, AffinityStoreConfig::Memory));
    }

    #[test]
    fn loads_redis_affinity_store() {
        let f = write(
            r#"
backends: ["a:1"]
affinity_store:
  type: redis
  url: "redis://redis-cluster:6379"
  key_prefix: "scg-prod"
  session_ttl_secs: 7200
  op_ttl_secs: 600
"#,
        );
        let c = Config::load(f.path()).unwrap();
        match c.affinity_store {
            AffinityStoreConfig::Redis(s) => {
                assert_eq!(s.url, "redis://redis-cluster:6379");
                assert_eq!(s.key_prefix, "scg-prod");
                assert_eq!(s.session_ttl_secs, 7200);
                assert_eq!(s.op_ttl_secs, 600);
            }
            other => panic!("expected Redis, got {:?}", other),
        }
    }

    #[test]
    fn redis_affinity_store_has_sane_defaults() {
        let f = write(
            r#"
backends: ["a:1"]
affinity_store:
  type: redis
  url: "redis://localhost:6379"
"#,
        );
        let c = Config::load(f.path()).unwrap();
        match c.affinity_store {
            AffinityStoreConfig::Redis(s) => {
                assert_eq!(s.key_prefix, "scg");
                assert_eq!(s.session_ttl_secs, 3600);
                assert_eq!(s.op_ttl_secs, 900);
            }
            other => panic!("expected Redis, got {:?}", other),
        }
    }

    #[test]
    fn health_check_defaults_to_disabled() {
        let f = write("backends: [\"a:1\"]\n");
        let c = Config::load(f.path()).unwrap();
        assert!(!c.health_check.enabled);
        // Default values present even when block omitted:
        assert_eq!(c.health_check.interval_secs, 5);
        assert_eq!(c.health_check.unhealthy_threshold, 3);
    }

    #[test]
    fn loads_health_check_block() {
        let f = write(
            r#"
backends: ["a:1"]
health_check:
  enabled: true
  interval_secs: 10
  timeout_secs: 3
  unhealthy_threshold: 5
  healthy_threshold: 3
"#,
        );
        let c = Config::load(f.path()).unwrap();
        assert!(c.health_check.enabled);
        assert_eq!(c.health_check.interval_secs, 10);
        assert_eq!(c.health_check.timeout_secs, 3);
        assert_eq!(c.health_check.unhealthy_threshold, 5);
        assert_eq!(c.health_check.healthy_threshold, 3);
    }

    #[test]
    fn shutdown_defaults() {
        let f = write("backends: [\"a:1\"]\n");
        let c = Config::load(f.path()).unwrap();
        assert_eq!(c.shutdown.deadline_secs, 30);
    }

    #[test]
    fn loads_shutdown_block() {
        let f = write(
            r#"
backends: ["a:1"]
shutdown:
  deadline_secs: 90
"#,
        );
        let c = Config::load(f.path()).unwrap();
        assert_eq!(c.shutdown.deadline_secs, 90);
    }

    #[test]
    fn tenant_resolver_defaults_to_from_claim_use_default() {
        let f = write("backends: [\"a:1\"]\n");
        let c = Config::load(f.path()).unwrap();
        assert!(matches!(
            c.tenant_resolver.source,
            TenantResolverSource::FromClaim
        ));
        assert!(matches!(
            c.tenant_resolver.on_missing,
            TenantOnMissing::UseDefault
        ));
        assert_eq!(c.tenant_resolver.default_name, "default");
    }

    #[test]
    fn loads_from_claim_reject() {
        let f = write(
            r#"
backends: ["a:1"]
tenant_resolver:
  source: from_claim
  on_missing: reject
  default_name: "default"
"#,
        );
        let c = Config::load(f.path()).unwrap();
        assert!(matches!(
            c.tenant_resolver.source,
            TenantResolverSource::FromClaim
        ));
        assert!(matches!(
            c.tenant_resolver.on_missing,
            TenantOnMissing::Reject
        ));
    }

    #[test]
    fn loads_from_metadata_with_custom_header() {
        let f = write(
            r#"
backends: ["a:1"]
tenant_resolver:
  source: from_metadata
  header: "x-org"
  on_missing: use_default
  default_name: "shared"
"#,
        );
        let c = Config::load(f.path()).unwrap();
        match c.tenant_resolver.source {
            TenantResolverSource::FromMetadata { header } => assert_eq!(header, "x-org"),
            other => panic!("expected FromMetadata, got {:?}", other),
        }
        assert_eq!(c.tenant_resolver.default_name, "shared");
    }

    #[test]
    fn from_metadata_header_defaults_to_x_tenant() {
        let f = write(
            r#"
backends: ["a:1"]
tenant_resolver:
  source: from_metadata
"#,
        );
        let c = Config::load(f.path()).unwrap();
        match c.tenant_resolver.source {
            TenantResolverSource::FromMetadata { header } => assert_eq!(header, "x-tenant"),
            other => panic!("expected FromMetadata, got {:?}", other),
        }
    }

    #[test]
    fn loads_always_default() {
        let f = write(
            r#"
backends: ["a:1"]
tenant_resolver:
  source: always_default
  default_name: "single-tenant"
"#,
        );
        let c = Config::load(f.path()).unwrap();
        assert!(matches!(
            c.tenant_resolver.source,
            TenantResolverSource::AlwaysDefault
        ));
        assert_eq!(c.tenant_resolver.default_name, "single-tenant");
    }

    #[test]
    fn tenant_pools_default_empty_use_default() {
        let f = write("backends: [\"a:1\"]\n");
        let c = Config::load(f.path()).unwrap();
        assert!(c.tenant_pools.overrides.is_empty());
        assert!(matches!(
            c.tenant_pools.on_unknown_tenant,
            UnknownTenantPolicySetting::UseDefault
        ));
    }

    #[test]
    fn loads_tenant_pools_with_overrides() {
        let f = write(
            r#"
backends: ["default-a:1", "default-b:1"]
tenant_pools:
  on_unknown_tenant: reject
  overrides:
    team-a:
      type: static
      addresses: ["a-1:15002", "a-2:15002"]
    team-b:
      type: k8s
      namespace: spark-b
      service_name: spark-connect
      port: 15002
"#,
        );
        let c = Config::load(f.path()).unwrap();
        assert_eq!(c.tenant_pools.overrides.len(), 2);
        assert!(matches!(
            c.tenant_pools.on_unknown_tenant,
            UnknownTenantPolicySetting::Reject
        ));
        match c.tenant_pools.overrides.get("team-a").unwrap() {
            BackendDiscovery::Static { addresses } => {
                assert_eq!(
                    addresses,
                    &vec!["a-1:15002".to_string(), "a-2:15002".to_string()]
                )
            }
            other => panic!("expected Static, got {:?}", other),
        }
        match c.tenant_pools.overrides.get("team-b").unwrap() {
            BackendDiscovery::K8s {
                namespace,
                service_name,
                port,
            } => {
                assert_eq!(namespace, "spark-b");
                assert_eq!(service_name, "spark-connect");
                assert_eq!(*port, 15002);
            }
            other => panic!("expected K8s, got {:?}", other),
        }
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
