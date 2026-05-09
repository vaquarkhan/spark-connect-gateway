//! Spark Connect Gateway entry point.

use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use scg_auth::{
    jwt::{JwtConfig, KeySource as AuthKeySource},
    oidc::OidcConfig,
    token::{StaticTokenAuthenticator, TokenEntry as AuthTokenEntry},
    AnonymousAuthenticator, AuthInterceptor, Authenticator, JwtAuthenticator, OidcAuthenticator,
};
use scg_config::{
    AffinityStoreConfig, AuthConfig, BackendDiscovery, Config, JwtSettings,
    KeySource as CfgKeySource, OidcSettings, RedisStoreSettings, TokenEntry as CfgTokenEntry,
    TracingSettings,
};
use scg_genproto::pb::spark_connect_service_server::SparkConnectServiceServer;
use scg_observability::{
    init_tracing, serve_admin, AdminConfig, Metrics, ReadinessProbe, TracingConfig, TracingHandle,
};
use scg_pool_k8s::{K8sPool, K8sPoolConfig};
use scg_pool_static::StaticPool;
use scg_proxy::{Dialer, SparkConnectProxy};
use scg_routing::{AffinityStore, Pool, Router};
use scg_store_memory::MemoryStore;
use scg_store_redis::{RedisStore, RedisStoreConfig};
use tokio::sync::watch;
use tonic::transport::Server;
use tracing::info;

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

    let (pool, _watcher) = build_pool(&cfg.discovery, &metrics, &readiness).await?;
    let store = build_affinity_store(&cfg.affinity_store).await?;
    let router = Arc::new(Router::new(pool, store));
    let dialer = Dialer::new();

    let auth = build_auth(&cfg.auth).await?;
    let svc = SparkConnectProxy::with_auth_and_metrics(
        router,
        dialer,
        AuthInterceptor::new(auth),
        metrics.clone(),
    );

    let addr = parse_bind_addr(&cfg.bind_addr)?;
    log_startup(&cfg, &addr);

    // Coordinate shutdown across the two servers (gRPC + admin) using a
    // watch channel.
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    tokio::spawn(async move {
        shutdown_signal().await;
        let _ = shutdown_tx.send(true);
    });

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
async fn build_pool(
    discovery: &BackendDiscovery,
    metrics: &Metrics,
    readiness: &ReadinessProbe,
) -> Result<(Arc<dyn Pool>, Option<tokio::task::JoinHandle<()>>)> {
    match discovery {
        BackendDiscovery::Static { addresses } => {
            let pool = StaticPool::new(addresses.clone()).context("building static pool")?;
            metrics.set_backend_pool_size(addresses.len() as i64);
            // Static pool is always ready: addresses are known at load time.
            readiness.mark_ready();
            Ok((Arc::new(pool), None))
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
            // Optimistic: once the watcher task is scheduled we mark
            // readiness, even if it hasn't yet emitted its first list
            // event. /readyz still returns 200 here; clients hitting
            // the gateway during the brief gap get Unavailable from
            // the proxy ("no healthy backend available").
            readiness.mark_ready();
            // Initial pool size is zero; K8sPool emits a tracing event
            // on each watcher update. Phase 2.5 will let the K8s pool
            // notify the metrics handle directly.
            metrics.set_backend_pool_size(0);
            Ok((Arc::new(pool), Some(handle)))
        }
    }
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
    match &cfg.discovery {
        BackendDiscovery::Static { addresses } => {
            info!(
                version,
                %addr,
                discovery = "static",
                auth = auth_kind,
                affinity_store = store_kind,
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
