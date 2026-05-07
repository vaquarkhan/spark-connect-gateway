//! Spark Connect Gateway entry point.

use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use scg_config::Config;
use scg_genproto::pb::spark_connect_service_server::SparkConnectServiceServer;
use scg_pool_static::StaticPool;
use scg_proxy::{Dialer, SparkConnectProxy};
use scg_routing::{AffinityStore, Pool, Router};
use scg_store_memory::MemoryStore;
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

    let pool: Arc<dyn Pool> =
        Arc::new(StaticPool::new(cfg.backends.clone()).context("building static pool")?);
    let store: Arc<dyn AffinityStore> = Arc::new(MemoryStore::new());
    let router = Arc::new(Router::new(pool, store));
    let dialer = Dialer::new();
    let svc = SparkConnectProxy::new(router, dialer);

    let addr = parse_bind_addr(&cfg.bind_addr)?;
    info!(version = env!("CARGO_PKG_VERSION"), %addr, backends = ?cfg.backends, "spark-connect-gateway starting");

    Server::builder()
        .add_service(SparkConnectServiceServer::new(svc))
        .serve_with_shutdown(addr, shutdown_signal())
        .await
        .context("gRPC server error")?;

    info!("shutdown complete");
    Ok(())
}

fn parse_bind_addr(s: &str) -> Result<std::net::SocketAddr> {
    // Allow forms like ":15003" → "0.0.0.0:15003", "host:port", or "[::]:port".
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
