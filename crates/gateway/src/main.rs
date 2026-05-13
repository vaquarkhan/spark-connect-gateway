//! Spark Connect Gateway entry point.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use scg_audit::{AuditConfig, AuditLogger};
use scg_auth::{
    jwt::{JwtConfig, KeySource as AuthKeySource},
    oidc::OidcConfig,
    token::{StaticTokenAuthenticator, TokenEntry as AuthTokenEntry},
    AnonymousAuthenticator, AuthInterceptor, Authenticator, JwtAuthenticator, OidcAuthenticator,
};
use scg_config::{
    AffinityStoreConfig, AuditSettings, AuthConfig, BackendDiscovery, BucketSettings, Config,
    HealthCheckSettings, JwtSettings, KeySource as CfgKeySource, OidcSettings, RateLimitFailMode,
    RateLimitRedisSettings, RateLimitSettings, RateLimitStore, RedisStoreSettings, TenantOnMissing,
    TenantResolverSettings, TenantResolverSource, TokenEntry as CfgTokenEntry, TracingSettings,
    UnknownTenantPolicySetting,
};
use scg_genproto::pb::spark_connect_service_server::SparkConnectServiceServer;
use scg_healthcheck::{HealthAwarePool, HealthCheckConfig};
use scg_observability::{
    init_tracing, serve_admin, AdminConfig, Metrics, ReadinessProbe, TracingConfig, TracingHandle,
};
use scg_pool_k8s::{K8sPool, K8sPoolConfig};
use scg_pool_static::StaticPool;
use scg_proxy::{Dialer, SparkConnectProxy};
use scg_ratelimit::redis::{RedisLimiter, RedisLimiterConfig};
use scg_ratelimit::{
    BucketRate, FailMode, LimiterObserver, MemoryLimiter, RateLimiter, RedisErrorObserver,
    RejectScope, TenantLimits,
};
use scg_routing::{AffinityStore, Pool, Router, UnknownTenantPolicy};
use scg_store_memory::MemoryStore;
use scg_store_redis::{RedisStore, RedisStoreConfig};
use scg_tenant::{
    OnMissing as TenantOnMissingRt, TenantResolver, TenantResolverConfig, TenantSource,
};
use tokio::sync::watch;
use tonic::transport::Server;
use tracing::{info, warn};

#[derive(Parser, Debug)]
#[command(version, about = "Open-source Spark Connect Gateway")]
struct Args {
    /// Path to YAML config file.
    #[arg(long, default_value = "config.yaml")]
    config: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let cfg = Config::load(&args.config).with_context(|| format!("loading {}", args.config))?;

    // Tracing must be installed before any other tokio work so the
    // global subscriber is in place. We also keep the handle alive for
    // the lifetime of `main` and shut it down explicitly so the OTLP
    // batch exporter has a chance to flush in-flight spans.
    let mut tracing_handle = build_tracing(&cfg.tracing)?;

    let metrics = Metrics::new().context("building metrics registry")?;

    // Static pools are ready immediately; K8s watch pools become ready
    // when the watcher emits its first list event (we approximate by
    // marking ready once the watcher *starts* — the gateway will
    // simply return Unavailable until backends are populated).
    let readiness = ReadinessProbe::default();

    let (tenant_router, _watchers) = build_tenant_routing(&cfg, &metrics, &readiness).await?;
    let store = build_affinity_store(&cfg.affinity_store).await?;
    let router = Arc::new(Router::new(tenant_router, store));
    let dialer = Dialer::new();

    let auth = build_auth(&cfg.auth).await?;
    let tenant_resolver = build_tenant_resolver(&cfg.tenant_resolver);
    let rate_limiter = build_rate_limiter(&cfg.rate_limit, &metrics).await?;
    let audit = build_audit_logger(&cfg.audit);
    let svc = SparkConnectProxy::with_all(
        router,
        dialer,
        AuthInterceptor::new(auth),
        metrics.clone(),
        tenant_resolver,
        rate_limiter,
        audit,
    );

    let addr = parse_bind_addr(&cfg.bind_addr)?;
    log_startup(&cfg, &addr);

    // Two-step shutdown:
    //   1. SIGTERM arrives → flip /readyz to 503 (K8s drains us from
    //      the Service), wait for active streams to finish or for the
    //      drain deadline.
    //   2. Trigger the gRPC + admin server shutdown.
    //
    // The two steps are coordinated by `drain_tx` (step 1 → 2) and
    // `shutdown_tx` (step 2 → both servers).
    let (drain_tx, mut drain_rx) = watch::channel(false);
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    let readiness_for_drain = readiness.clone();
    let metrics_for_drain = metrics.clone();
    let shutdown_deadline = std::time::Duration::from_secs(cfg.shutdown.deadline_secs);
    tokio::spawn(async move {
        shutdown_signal().await;
        info!(
            deadline_secs = shutdown_deadline.as_secs(),
            "shutdown: SIGTERM/SIGINT received; entering drain"
        );
        // Step 1: tell K8s we're not ready so new traffic stops flowing.
        readiness_for_drain.mark_not_ready();
        let _ = drain_tx.send(true);
        // Wait for in-flight streams to drain or the deadline to expire.
        let drain_result = tokio::time::timeout(shutdown_deadline, async {
            loop {
                let active = metrics_for_drain.active_streams_value();
                if active <= 0 {
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
        })
        .await;
        let final_active = metrics_for_drain.active_streams_value();
        match drain_result {
            Ok(()) => info!(
                final_active_streams = final_active,
                "shutdown: drain complete"
            ),
            Err(_) => warn!(
                deadline_secs = shutdown_deadline.as_secs(),
                final_active_streams = final_active,
                "shutdown: drain deadline reached; forcing shutdown"
            ),
        }
        // Step 2: stop the servers.
        let _ = shutdown_tx.send(true);
    });
    // The drain_rx is parked here to keep the channel alive; the drain
    // task is the only producer.
    let _ = drain_rx.borrow_and_update();

    let admin_handle = if let Some(admin_addr_str) = &cfg.admin_addr {
        let admin_addr = parse_bind_addr(admin_addr_str)?;
        let admin_cfg = AdminConfig {
            bind_addr: admin_addr,
        };
        let metrics_for_admin = metrics.clone();
        let readiness_for_admin = readiness.clone();
        let mut shutdown_rx_admin = shutdown_rx.clone();
        let handle = tokio::spawn(async move {
            let shutdown = async move {
                let _ = shutdown_rx_admin.changed().await;
            };
            if let Err(e) =
                serve_admin(admin_cfg, metrics_for_admin, readiness_for_admin, shutdown).await
            {
                tracing::warn!(error = %e, "admin server stopped with error");
            }
        });
        Some(handle)
    } else {
        info!("admin_addr is null in config; skipping admin/metrics endpoint");
        None
    };

    let grpc_shutdown = async move {
        let _ = shutdown_rx.changed().await;
    };

    let grpc_result = Server::builder()
        .add_service(SparkConnectServiceServer::new(svc))
        .serve_with_shutdown(addr, grpc_shutdown)
        .await
        .context("gRPC server error");

    if let Some(h) = admin_handle {
        let _ = h.await;
    }

    // Flush in-flight spans before the process exits. After this call
    // any further `tracing` events tagged for OTLP export are dropped
    // (logs still work via the JSON formatter layer).
    tracing_handle.shutdown();

    grpc_result?;
    info!("shutdown complete");
    Ok(())
}

/// Build the configured `Pool` implementation. For dynamic sources
/// (K8s) the watcher task is spawned and its `JoinHandle` is returned
/// so the caller can keep it alive for the lifetime of the server.
///
/// Returned tuple: (pool, optional watcher join-handle, initial pool
/// size for metrics).
async fn build_pool_from_discovery(
    discovery: &BackendDiscovery,
) -> Result<(Arc<dyn Pool>, Option<tokio::task::JoinHandle<()>>, i64)> {
    match discovery {
        BackendDiscovery::Static { addresses } => {
            let size = addresses.len() as i64;
            let pool = StaticPool::new(addresses.clone()).context("building static pool")?;
            Ok((Arc::new(pool), None, size))
        }
        BackendDiscovery::K8s {
            namespace,
            service_name,
            port,
        } => {
            let pool = K8sPool::new();
            let cfg = K8sPoolConfig {
                namespace: namespace.clone(),
                service_name: service_name.clone(),
                port: *port,
            };
            let handle = pool
                .spawn_watcher(cfg)
                .await
                .context("spawning K8s Endpoints watcher")?;
            Ok((Arc::new(pool), Some(handle), 0))
        }
    }
}

/// Build the per-tenant routing topology from `cfg`. Produces a
/// [`TenantRouter`] plus a list of K8s-watcher join handles that
/// must stay alive for the lifetime of the server (Tokio drops
/// abort the watchers; we park them in a Vec the caller owns).
///
/// Pool selection per tenant:
///
/// * The deployment's *default* pool comes from `cfg.discovery`
///   (the `backends:` / `backend_discovery:` config — also the
///   single-pool baseline for non-multi-tenant deployments).
/// * Each entry in `cfg.tenant_pools.overrides` becomes a separate
///   tenant-scoped pool, optionally wrapped in `HealthAwarePool`
///   when active health probing is enabled.
/// * The unknown-tenant policy from `tenant_pools.on_unknown_tenant`
///   tells `TenantRouter` whether to fall back to the default pool
///   or reject with `PermissionDenied`.
///
/// `readiness.mark_ready()` is called once at the end — readiness
/// reflects "the gateway is ready to serve at all", not per-tenant
/// pool readiness. The proxy still returns `Unavailable` per-RPC
/// when a tenant's pool happens to be empty (K8s discovery during
/// startup).
///
/// `metrics.set_backend_pool_size` is set to the *default* pool's
/// size only — per-tenant gauges would need a `tenant` label and
/// we deliberately keep `scg_backend_pool_size` unlabelled to
/// bound cardinality. Per-tenant pool sizes show up in logs
/// instead.
async fn build_tenant_routing(
    cfg: &Config,
    metrics: &Metrics,
    readiness: &ReadinessProbe,
) -> Result<(scg_routing::TenantRouter, Vec<tokio::task::JoinHandle<()>>)> {
    let mut watchers = Vec::new();

    // Default pool from the existing discovery config.
    let (default_pool, default_watcher, default_size) =
        build_pool_from_discovery(&cfg.discovery).await?;
    if let Some(h) = default_watcher {
        watchers.push(h);
    }
    metrics.set_backend_pool_size(default_size);
    let default_pool = wrap_with_healthcheck(default_pool, &cfg.health_check);
    info!(size = default_size, "default tenant pool ready");

    // Per-tenant overrides.
    let mut tenants: HashMap<String, Arc<dyn Pool>> = HashMap::new();
    for (tenant, override_disc) in &cfg.tenant_pools.overrides {
        let (pool, watcher, size) = build_pool_from_discovery(override_disc).await?;
        if let Some(h) = watcher {
            watchers.push(h);
        }
        let pool = wrap_with_healthcheck(pool, &cfg.health_check);
        info!(
            tenant = %tenant,
            size,
            "tenant override pool ready"
        );
        tenants.insert(tenant.clone(), pool);
    }

    readiness.mark_ready();

    let policy = match cfg.tenant_pools.on_unknown_tenant {
        UnknownTenantPolicySetting::UseDefault => UnknownTenantPolicy::UseDefault,
        UnknownTenantPolicySetting::Reject => UnknownTenantPolicy::Reject,
    };
    Ok((
        scg_routing::TenantRouter::new(tenants, Some(default_pool), policy),
        watchers,
    ))
}

/// Wrap `inner` with active health-check probing if enabled.
/// Otherwise return the inner pool unchanged. The probe task is
/// spawned and detached; we hold no JoinHandle because the task
/// keeps a strong Arc to the wrapper, which is itself owned by the
/// router for the lifetime of the process.
fn wrap_with_healthcheck(inner: Arc<dyn Pool>, cfg: &HealthCheckSettings) -> Arc<dyn Pool> {
    if !cfg.enabled {
        return inner;
    }
    let hc_cfg = HealthCheckConfig {
        interval: std::time::Duration::from_secs(cfg.interval_secs),
        timeout: std::time::Duration::from_secs(cfg.timeout_secs),
        unhealthy_threshold: cfg.unhealthy_threshold,
        healthy_threshold: cfg.healthy_threshold,
    };
    let wrapper = HealthAwarePool::new(inner, hc_cfg);
    let _probe = wrapper.spawn_probe();
    wrapper
}

/// Build the configured `AffinityStore`. Memory is in-process and
/// always succeeds; Redis dials the URL eagerly so misconfiguration
/// surfaces at startup, not at the first inbound RPC.
async fn build_affinity_store(cfg: &AffinityStoreConfig) -> Result<Arc<dyn AffinityStore>> {
    match cfg {
        AffinityStoreConfig::Memory => Ok(Arc::new(MemoryStore::new())),
        AffinityStoreConfig::Redis(s) => {
            let store = RedisStore::connect(redis_store_config(s))
                .await
                .with_context(|| format!("connecting to redis at {}", s.url))?;
            Ok(Arc::new(store))
        }
    }
}

fn redis_store_config(s: &RedisStoreSettings) -> RedisStoreConfig {
    RedisStoreConfig {
        url: s.url.clone(),
        key_prefix: s.key_prefix.clone(),
        session_ttl: std::time::Duration::from_secs(s.session_ttl_secs),
        op_ttl: std::time::Duration::from_secs(s.op_ttl_secs),
    }
}

/// Adapter that turns a [`Metrics`] handle into a
/// [`LimiterObserver`]: every rate-limit rejection bumps
/// `scg_rate_limit_rejected_total{tenant, scope}`. Lives in the
/// gateway crate (not in `scg-ratelimit`) so the ratelimit crate
/// stays free of an observability dependency.
struct MetricsLimiterObserver {
    metrics: Metrics,
}

impl LimiterObserver for MetricsLimiterObserver {
    fn on_reject(&self, tenant: &str, scope: RejectScope) {
        self.metrics
            .record_rate_limit_reject(tenant, scope.as_str());
    }
}

/// Adapter for `scg_rate_limit_redis_errors_total` — bumped by the
/// Redis-backed limiter when an EVAL fails. Distinct from
/// `LimiterObserver` because Redis errors and rate-limit rejections
/// are different events (an error doesn't necessarily reject).
struct MetricsRedisErrorObserver {
    metrics: Metrics,
}

impl RedisErrorObserver for MetricsRedisErrorObserver {
    fn on_redis_error(&self, tenant: &str, reason: &'static str) {
        self.metrics.record_rate_limit_redis_error(tenant, reason);
    }
}

fn bucket_rate(rps: f64, burst: u64) -> BucketRate {
    BucketRate {
        rpcs_per_second: rps,
        burst,
    }
}

fn tenant_limits_from(cfg: &BucketSettings) -> TenantLimits {
    TenantLimits {
        tenant: bucket_rate(cfg.rpcs_per_second, cfg.burst),
        per_user: bucket_rate(cfg.per_user_rpcs_per_second, cfg.per_user_burst),
    }
}

/// Build the configured [`RateLimiter`]. Returns `None` when
/// `rate_limit.enabled: false` (the default) so the proxy hot
/// path can skip the check entirely.
///
/// `memory` is sync to build; `redis` dials the server eagerly so
/// a misconfigured URL / unreachable host surfaces at startup
/// instead of the first inbound RPC. Per-RPC Redis failures take
/// the configured [`FailMode`] path inside the limiter.
async fn build_rate_limiter(
    cfg: &RateLimitSettings,
    metrics: &Metrics,
) -> Result<Option<RateLimiter>> {
    if !cfg.enabled {
        return Ok(None);
    }
    let default = tenant_limits_from(&cfg.default);
    let overrides: std::collections::HashMap<String, TenantLimits> = cfg
        .overrides
        .iter()
        .map(|(k, v)| (k.clone(), tenant_limits_from(v)))
        .collect();
    let observer = Arc::new(MetricsLimiterObserver {
        metrics: metrics.clone(),
    });
    match cfg.store {
        RateLimitStore::Memory => Ok(Some(RateLimiter::Memory(MemoryLimiter::new(
            default, overrides, observer,
        )))),
        RateLimitStore::Redis => {
            let redis_obs = Arc::new(MetricsRedisErrorObserver {
                metrics: metrics.clone(),
            });
            let redis_cfg = rate_limit_redis_config(&cfg.redis);
            let limiter = RedisLimiter::connect(redis_cfg, default, overrides, observer, redis_obs)
                .await
                .with_context(|| format!("connecting rate-limit redis at {}", cfg.redis.url,))?;
            Ok(Some(RateLimiter::Redis(limiter)))
        }
    }
}

fn rate_limit_redis_config(s: &RateLimitRedisSettings) -> RedisLimiterConfig {
    RedisLimiterConfig {
        url: s.url.clone(),
        key_prefix: s.key_prefix.clone(),
        key_ttl: std::time::Duration::from_secs(s.key_ttl_secs),
        fail_mode: match s.on_failure {
            RateLimitFailMode::Open => FailMode::Open,
            RateLimitFailMode::Closed => FailMode::Closed,
        },
    }
}

/// Translate the YAML `audit:` block into the runtime [`AuditLogger`].
/// Defaults are conservative: enabled, but `rpc.ok` events stay off
/// — operators flip `log_successful_rpcs: true` only under strict
/// monitoring policies, since every successful RPC would otherwise
/// hit the log pipeline.
fn build_audit_logger(cfg: &AuditSettings) -> AuditLogger {
    AuditLogger::new(AuditConfig {
        enabled: cfg.enabled,
        log_successful_rpcs: cfg.log_successful_rpcs,
    })
}

/// Translate the YAML `tenant_resolver:` block into a runtime
/// [`TenantResolver`]. The tagged `source` enum maps to the
/// equivalent `scg-tenant` variant. With no `tenant_resolver:`
/// block in config the gateway gets the back-compat default — every
/// inbound RPC ends up in `tenant="default"`, preserving the
/// single-tenant baseline.
fn build_tenant_resolver(cfg: &TenantResolverSettings) -> TenantResolver {
    let source = match &cfg.source {
        TenantResolverSource::FromClaim => TenantSource::FromClaim,
        TenantResolverSource::FromMetadata { header } => TenantSource::FromMetadata {
            header: header.clone(),
        },
        TenantResolverSource::AlwaysDefault => TenantSource::AlwaysDefault,
    };
    let on_missing = match cfg.on_missing {
        TenantOnMissing::UseDefault => TenantOnMissingRt::UseDefault,
        TenantOnMissing::Reject => TenantOnMissingRt::Reject,
    };
    TenantResolver::new(TenantResolverConfig {
        source,
        on_missing,
        default_name: cfg.default_name.clone(),
    })
}

/// Construct the right Authenticator from config.
async fn build_auth(cfg: &AuthConfig) -> Result<Arc<dyn Authenticator>> {
    match cfg {
        AuthConfig::None => Ok(Arc::new(AnonymousAuthenticator)),
        AuthConfig::Static { tokens } => {
            let entries = tokens.iter().map(token_entry).collect();
            let auth = StaticTokenAuthenticator::new(entries)
                .context("building static-token authenticator")?;
            Ok(Arc::new(auth))
        }
        AuthConfig::Jwt(s) => {
            let auth =
                JwtAuthenticator::new(jwt_config(s)).context("building JWT authenticator")?;
            Ok(Arc::new(auth))
        }
        AuthConfig::Oidc(s) => {
            let auth = OidcAuthenticator::new(oidc_config(s))
                .await
                .context("building OIDC authenticator")?;
            Ok(Arc::new(auth))
        }
    }
}

fn token_entry(t: &CfgTokenEntry) -> AuthTokenEntry {
    AuthTokenEntry {
        token: t.token.clone(),
        user_id: t.user_id.clone(),
        tenant: t.tenant.clone(),
        groups: t.groups.clone(),
    }
}

fn jwt_config(s: &JwtSettings) -> JwtConfig {
    JwtConfig {
        key: match &s.key {
            CfgKeySource::PemFile { path } => AuthKeySource::PemFile { path: path.clone() },
            CfgKeySource::PemInline { pem } => AuthKeySource::PemInline { pem: pem.clone() },
            CfgKeySource::HmacSecret { secret } => AuthKeySource::HmacSecret {
                secret: secret.clone(),
            },
        },
        algorithms: s.algorithms.clone(),
        issuer: s.issuer.clone(),
        audience: s.audience.clone(),
        user_id_claim: s.user_id_claim.clone(),
        tenant_claim: s.tenant_claim.clone(),
        groups_claim: s.groups_claim.clone(),
    }
}

fn oidc_config(s: &OidcSettings) -> OidcConfig {
    OidcConfig {
        jwks_url: s.jwks_url.clone(),
        discovery_url: s.discovery_url.clone(),
        algorithms: s.algorithms.clone(),
        issuer: s.issuer.clone(),
        audience: s.audience.clone(),
        user_id_claim: s.user_id_claim.clone(),
        tenant_claim: s.tenant_claim.clone(),
        groups_claim: s.groups_claim.clone(),
        refresh_floor_secs: s.refresh_floor_secs,
    }
}

fn log_startup(cfg: &Config, addr: &std::net::SocketAddr) {
    let version = env!("CARGO_PKG_VERSION");
    let auth_kind = match &cfg.auth {
        AuthConfig::None => "none",
        AuthConfig::Static { .. } => "static",
        AuthConfig::Jwt(_) => "jwt",
        AuthConfig::Oidc(_) => "oidc",
    };
    let store_kind = match &cfg.affinity_store {
        AffinityStoreConfig::Memory => "memory",
        AffinityStoreConfig::Redis(_) => "redis",
    };
    let tenant_source = match &cfg.tenant_resolver.source {
        TenantResolverSource::FromClaim => "from_claim",
        TenantResolverSource::FromMetadata { .. } => "from_metadata",
        TenantResolverSource::AlwaysDefault => "always_default",
    };
    let tenant_on_missing = match &cfg.tenant_resolver.on_missing {
        TenantOnMissing::UseDefault => "use_default",
        TenantOnMissing::Reject => "reject",
    };
    let rate_limit_enabled = cfg.rate_limit.enabled;
    let rate_limit_store = match cfg.rate_limit.store {
        RateLimitStore::Memory => "memory",
        RateLimitStore::Redis => "redis",
    };
    let audit_enabled = cfg.audit.enabled;
    match &cfg.discovery {
        BackendDiscovery::Static { addresses } => {
            info!(
                version,
                %addr,
                discovery = "static",
                auth = auth_kind,
                affinity_store = store_kind,
                tenant_source,
                tenant_on_missing,
                rate_limit = rate_limit_enabled,
                rate_limit_store,
                audit = audit_enabled,
                backends = ?addresses,
                "spark-connect-gateway starting"
            );
        }
        BackendDiscovery::K8s {
            namespace,
            service_name,
            port,
        } => {
            info!(
                version,
                %addr,
                discovery = "k8s",
                auth = auth_kind,
                affinity_store = store_kind,
                tenant_source,
                tenant_on_missing,
                rate_limit = rate_limit_enabled,
                rate_limit_store,
                audit = audit_enabled,
                namespace = %namespace,
                service = %service_name,
                port,
                "spark-connect-gateway starting (will populate backends from K8s Endpoints)"
            );
        }
    }
}

fn parse_bind_addr(s: &str) -> Result<std::net::SocketAddr> {
    let normalized = if let Some(stripped) = s.strip_prefix(':') {
        format!("0.0.0.0:{}", stripped)
    } else {
        s.to_string()
    };
    normalized
        .parse()
        .with_context(|| format!("invalid bind_addr {}", s))
}

/// Translate the YAML `tracing:` block into the runtime
/// `TracingConfig`, then install the global subscriber. Returns the
/// [`TracingHandle`] that the caller keeps alive until shutdown so
/// in-flight spans get flushed.
fn build_tracing(settings: &Option<TracingSettings>) -> Result<TracingHandle> {
    let mut cfg = TracingConfig::default();
    if let Some(s) = settings {
        cfg.service_name = s.service_name.clone();
        if let Some(v) = &s.service_version {
            cfg.service_version = v.clone();
        }
        cfg.endpoint = s.endpoint.clone();
        cfg.sample_ratio = s.sample_ratio;
        cfg.export_timeout = std::time::Duration::from_secs(s.export_timeout_secs);
    }
    init_tracing(cfg).context("installing tracing subscriber")
}

async fn shutdown_signal() {
    use tokio::signal::unix::{signal, SignalKind};
    let mut int = signal(SignalKind::interrupt()).expect("install SIGINT handler");
    let mut term = signal(SignalKind::terminate()).expect("install SIGTERM handler");
    tokio::select! {
        _ = int.recv() => info!("SIGINT received"),
        _ = term.recv() => info!("SIGTERM received"),
    }
}
