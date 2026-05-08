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
    AuthConfig, BackendDiscovery, Config, JwtSettings, KeySource as CfgKeySource, OidcSettings,
    TokenEntry as CfgTokenEntry,
};
use scg_genproto::pb::spark_connect_service_server::SparkConnectServiceServer;
use scg_observability::{serve_admin, AdminConfig, Metrics, ReadinessProbe};
use scg_pool_k8s::{K8sPool, K8sPoolConfig};
use scg_pool_static::StaticPool;
use scg_proxy::{Dialer, SparkConnectProxy};
use scg_routing::{AffinityStore, Pool, Router};
use scg_store_memory::MemoryStore;
use tokio::sync::watch;
use tonic::transport::Server;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(version, about = "Open-source Spark Connect Gateway")]
struct Args {
    /// Path to YAML config file.
    #[arg(long, default_value = "config.yaml")]
    config: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    let args = Args::parse();
    let cfg = Config::load(&args.config).with_context(|| format!("loading {}", args.config))?;

    let metrics = Metrics::new().context("building metrics registry")?;

    // Static pools are ready immediately; K8s watch pools become ready
    // when the watcher emits its first list event (we approximate by
    // marking ready once the watcher *starts* — the gateway will
    // simply return Unavailable until backends are populated).
    let readiness = ReadinessProbe::default();

    let (pool, _watcher) = build_pool(&cfg.discovery, &metrics, &readiness).await?;
    let store: Arc<dyn AffinityStore> = Arc::new(MemoryStore::new());
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
    match &cfg.discovery {
        BackendDiscovery::Static { addresses } => {
            info!(
                version,
                %addr,
                discovery = "static",
                auth = auth_kind,
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

fn init_tracing() {
    let env = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(env)
        .json()
        .with_target(false)
        .init();
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
