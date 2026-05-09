//! Integration test for [`HealthAwarePool`] against real gRPC
//! servers. Verifies that:
//!
//! 1. A backend whose `Health` server is alive stays in
//!    `pick()`/`all_healthy()`.
//! 2. A backend that goes down (server task dropped) is evicted
//!    after `unhealthy_threshold` consecutive failed probes.
//! 3. `pick()` keeps returning the surviving backend.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use scg_healthcheck::{HealthAwarePool, HealthCheckConfig};
use scg_routing::Pool;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tonic_health::server::health_reporter;

/// Static pool stub: returns whatever vec we feed it. We rebuild it
/// for each test so we control which backends "exist" from the
/// inner pool's perspective; the wrapper's job is to filter by
/// health.
struct StubPool {
    backends: parking_lot::RwLock<Vec<String>>,
    cursor: AtomicU64,
}

impl StubPool {
    fn new(backends: Vec<String>) -> Arc<Self> {
        Arc::new(Self {
            backends: parking_lot::RwLock::new(backends),
            cursor: AtomicU64::new(0),
        })
    }
}

impl Pool for StubPool {
    fn pick(&self) -> Option<String> {
        let g = self.backends.read();
        if g.is_empty() {
            return None;
        }
        let idx = self.cursor.fetch_add(1, Ordering::Relaxed);
        Some(g[(idx as usize) % g.len()].clone())
    }
    fn all_healthy(&self) -> Vec<String> {
        self.backends.read().clone()
    }
}

/// Spawn a gRPC server with the standard `Health` service registered
/// and reporting `Serving`. Returns its host:port and a shutdown
/// sender.
async fn spawn_health_server() -> (String, oneshot::Sender<()>) {
    let (reporter, health_svc) = health_reporter();
    // Empty service name = "the entire backend"; our probe code uses
    // the same convention.
    reporter
        .set_service_status("", tonic_health::ServingStatus::Serving)
        .await;

    let lis = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = lis.local_addr().unwrap().to_string();
    let (tx, rx) = oneshot::channel();
    tokio::spawn(async move {
        Server::builder()
            .add_service(health_svc)
            .serve_with_incoming_shutdown(TcpListenerStream::new(lis), async {
                let _ = rx.await;
            })
            .await
            .ok();
    });
    (addr, tx)
}

#[tokio::test]
async fn evicts_dead_backend_then_keeps_alive_one() {
    let (alive, _alive_kill) = spawn_health_server().await;
    let (dying, dying_kill) = spawn_health_server().await;

    let stub = StubPool::new(vec![alive.clone(), dying.clone()]);
    // Aggressive: 200ms interval, 200ms timeout, 2-failure eviction
    // → eviction of `dying` should land within ~600ms after kill.
    let cfg = HealthCheckConfig {
        interval: Duration::from_millis(200),
        timeout: Duration::from_millis(300),
        unhealthy_threshold: 2,
        healthy_threshold: 2,
    };
    let wrapper = HealthAwarePool::new(stub.clone() as Arc<dyn Pool>, cfg);
    let _probe = wrapper.spawn_probe();

    // Let one probe round complete; both should still be healthy.
    tokio::time::sleep(Duration::from_millis(400)).await;
    let healthy = wrapper.all_healthy();
    assert!(healthy.contains(&alive), "alive backend should be present");
    assert!(
        healthy.contains(&dying),
        "dying backend should still be present pre-kill"
    );

    // Kill `dying`. Give the OS a moment to actually tear the
    // listener down before probes start failing.
    let _ = dying_kill.send(());
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Wait for the wrapper to evict it. With 200ms interval and
    // unhealthy_threshold=2, we expect eviction within ~600ms.
    let mut evicted = false;
    for _ in 0..30 {
        let healthy = wrapper.all_healthy();
        if !healthy.contains(&dying) && healthy.contains(&alive) {
            evicted = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        evicted,
        "dying backend should be evicted within the test window"
    );

    // pick() must keep returning the surviving backend.
    for _ in 0..5 {
        assert_eq!(wrapper.pick().as_deref(), Some(alive.as_str()));
    }
}
