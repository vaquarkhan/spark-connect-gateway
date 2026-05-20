//! Kubernetes service-watch backend pool.
//!
//! Watches a `Service`'s `Endpoints` object in a given namespace and exposes
//! the ready-pod IPs (paired with the service's target port) as the live
//! backend list. The pool is updated in-process whenever the K8s watcher
//! emits an event, so a `kubectl scale`, a pod evict, or a rolling update
//! flows through automatically — no gateway redeploy.
//!
//! Design notes:
//!
//! * We watch the legacy `core/v1/Endpoints` resource rather than the newer
//!   `discovery.k8s.io/v1/EndpointSlice`. Endpoints is universally
//!   supported (back to K8s 1.0) and gives us a single object per Service;
//!   EndpointSlice support can be added later as an alternative source if
//!   we need topology hints or 1000+-endpoint Services.
//! * We only consider entries in `subset.addresses` (i.e. ready pods) and
//!   skip `subset.not_ready_addresses`.
//! * Internal state is a `Vec<String>` of `host:port` strings plus a
//!   round-robin cursor. The watcher fully replaces the Vec on every
//!   event, so we don't need fine-grained add/remove handling.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use futures::StreamExt;
use k8s_openapi::api::core::v1::Endpoints;
use kube::api::Api;
use kube::runtime::watcher;
use parking_lot::RwLock;
use scg_routing::Pool;
use serde::Deserialize;
use thiserror::Error;
use tracing::{debug, info, warn};

/// Configuration for the K8s pool.
#[derive(Debug, Clone, Deserialize)]
pub struct K8sPoolConfig {
    /// Namespace the Service lives in.
    pub namespace: String,
    /// Name of the Service whose Endpoints we watch.
    pub service_name: String,
    /// Target port that pods listen on. K8s `Endpoints.subset.ports` may
    /// have multiple named ports; we filter by the port *number* here.
    pub port: u16,
}

#[derive(Debug, Error)]
pub enum K8sPoolError {
    #[error("kube client init: {0}")]
    Client(#[from] kube::Error),
}

/// In-memory snapshot of the live backend list, plus a round-robin cursor.
#[derive(Default)]
struct Inner {
    backends: RwLock<Vec<String>>,
    cursor: AtomicU64,
}

/// K8s service-watch backend pool. Cheap to clone: an `Arc` wraps the
/// shared state.
#[derive(Clone)]
pub struct K8sPool {
    inner: Arc<Inner>,
}

impl K8sPool {
    /// Build an empty pool. The watcher task must be spawned separately
    /// via [`K8sPool::spawn_watcher`] for the pool to become populated.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner::default()),
        }
    }

    /// Replace the backend list. Used by the spawned watcher task
    /// (in this crate) and by the in-crate integration tests.
    /// Production callers never invoke this directly — it's an
    /// internal seam exposed only because the integration-test
    /// binary lives in a separate compilation unit.
    #[doc(hidden)]
    pub fn set_backends(&self, addrs: Vec<String>) {
        let mut g = self.inner.backends.write();
        if *g != addrs {
            info!(count = addrs.len(), "k8s pool: backend list updated");
            debug!(?addrs, "k8s pool: new backends");
            *g = addrs;
        }
    }

    /// Spawn a Tokio task that drives the K8s `Endpoints` watcher and
    /// updates the pool whenever the Endpoints object changes. Returns
    /// the spawned `JoinHandle` so callers can `abort()` on shutdown.
    pub async fn spawn_watcher(
        &self,
        cfg: K8sPoolConfig,
    ) -> Result<tokio::task::JoinHandle<()>, K8sPoolError> {
        let client = kube::Client::try_default().await?;
        let api: Api<Endpoints> = Api::namespaced(client, &cfg.namespace);

        // Filter to a single Endpoints object by name. The runtime watcher
        // takes a `ListParams`-shaped config and we use field_selector to
        // narrow to one resource.
        let watcher_cfg =
            watcher::Config::default().fields(&format!("metadata.name={}", cfg.service_name));

        let pool = self.clone();
        let target_port = cfg.port;
        let service_name = cfg.service_name.clone();

        let handle = tokio::spawn(async move {
            let mut stream = watcher(api, watcher_cfg).boxed();
            while let Some(event) = stream.next().await {
                match event {
                    Ok(watcher::Event::Apply(ep)) | Ok(watcher::Event::InitApply(ep)) => {
                        let addrs = extract_addresses(&ep, target_port);
                        pool.set_backends(addrs);
                    }
                    Ok(watcher::Event::Delete(_)) => {
                        warn!(service = %service_name, "k8s pool: Endpoints object deleted");
                        pool.set_backends(Vec::new());
                    }
                    Ok(watcher::Event::Init) | Ok(watcher::Event::InitDone) => {}
                    Err(e) => {
                        warn!(error = %e, "k8s pool: watcher error; will reconnect");
                    }
                }
            }
            warn!("k8s pool: watcher stream ended");
        });
        Ok(handle)
    }
}

impl Default for K8sPool {
    fn default() -> Self {
        Self::new()
    }
}

impl Pool for K8sPool {
    fn pick(&self) -> Option<String> {
        let g = self.inner.backends.read();
        if g.is_empty() {
            return None;
        }
        let idx = self.inner.cursor.fetch_add(1, Ordering::Relaxed);
        Some(g[(idx as usize) % g.len()].clone())
    }

    fn all_healthy(&self) -> Vec<String> {
        self.inner.backends.read().clone()
    }

    fn mark_unhealthy(&self, addr: &str) {
        // The K8s watcher is the source of truth for membership. A
        // forward error against `addr` is usually transient (pod
        // restarted) and the watcher will reconcile within seconds. We
        // log the hint but do not optimistically evict — eviction would
        // race with the watcher and risks oscillation.
        debug!(%addr, "k8s pool: mark_unhealthy hint (no-op)");
    }
}

/// Extract `host:port` strings from a single `Endpoints` object, keeping
/// only the addresses that match `target_port`.
///
/// `Endpoints.subsets[i].addresses` are the ready pods;
/// `subsets[i].not_ready_addresses` are excluded. Each subset has its own
/// list of `ports`; we walk the cartesian product within each subset.
fn extract_addresses(ep: &Endpoints, target_port: u16) -> Vec<String> {
    let target_port_i32 = i32::from(target_port);
    let Some(subsets) = ep.subsets.as_ref() else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for subset in subsets {
        let port_matches = subset
            .ports
            .as_ref()
            .is_some_and(|ports| ports.iter().any(|p| p.port == target_port_i32));
        if !port_matches {
            continue;
        }
        let Some(addrs) = subset.addresses.as_ref() else {
            continue;
        };
        for a in addrs {
            out.push(format!("{}:{}", a.ip, target_port));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::api::core::v1::{EndpointAddress, EndpointPort, EndpointSubset, Endpoints};

    fn ep(subsets: Vec<EndpointSubset>) -> Endpoints {
        Endpoints {
            metadata: Default::default(),
            subsets: Some(subsets),
        }
    }

    fn addr(ip: &str) -> EndpointAddress {
        EndpointAddress {
            ip: ip.into(),
            ..Default::default()
        }
    }

    fn port(p: i32) -> EndpointPort {
        EndpointPort {
            port: p,
            ..Default::default()
        }
    }

    #[test]
    fn extracts_ready_addresses_only_for_matching_port() {
        let e = ep(vec![EndpointSubset {
            addresses: Some(vec![addr("10.0.0.1"), addr("10.0.0.2")]),
            not_ready_addresses: Some(vec![addr("10.0.0.99")]),
            ports: Some(vec![port(15002)]),
        }]);
        let got = extract_addresses(&e, 15002);
        assert_eq!(got, vec!["10.0.0.1:15002", "10.0.0.2:15002"]);
    }

    #[test]
    fn skips_subsets_with_wrong_port() {
        let e = ep(vec![
            EndpointSubset {
                addresses: Some(vec![addr("10.0.0.1")]),
                not_ready_addresses: None,
                ports: Some(vec![port(80)]),
            },
            EndpointSubset {
                addresses: Some(vec![addr("10.0.0.2")]),
                not_ready_addresses: None,
                ports: Some(vec![port(15002)]),
            },
        ]);
        let got = extract_addresses(&e, 15002);
        assert_eq!(got, vec!["10.0.0.2:15002"]);
    }

    #[test]
    fn empty_subsets_returns_empty_list() {
        let e = ep(Vec::new());
        assert!(extract_addresses(&e, 15002).is_empty());

        let e = Endpoints {
            metadata: Default::default(),
            subsets: None,
        };
        assert!(extract_addresses(&e, 15002).is_empty());
    }

    #[test]
    fn pool_round_robins_over_current_backends() {
        let p = K8sPool::new();
        assert!(p.pick().is_none(), "empty pool returns None");
        p.set_backends(vec!["a".into(), "b".into(), "c".into()]);

        let got: Vec<_> = (0..4).filter_map(|_| p.pick()).collect();
        assert_eq!(got, vec!["a", "b", "c", "a"]);

        let mut healthy = p.all_healthy();
        healthy.sort();
        assert_eq!(healthy, vec!["a", "b", "c"]);
    }

    #[test]
    fn pool_handles_membership_changes() {
        let p = K8sPool::new();
        p.set_backends(vec!["a".into(), "b".into()]);
        assert_eq!(p.pick().as_deref(), Some("a"));

        // Replace; cursor is shared so we keep advancing into the new
        // list — not a correctness issue since pick() is `Option<String>`
        // anyway, but worth pinning down via test.
        p.set_backends(vec!["x".into(), "y".into(), "z".into()]);
        let got: Vec<_> = (0..3).filter_map(|_| p.pick()).collect();
        // We don't assert exact ordering here because it depends on the
        // cursor state; instead just check that all returned addrs are
        // from the new set.
        for g in &got {
            assert!(
                ["x", "y", "z"].contains(&g.as_str()),
                "unexpected pick: {}",
                g
            );
        }
        assert_eq!(got.len(), 3);
    }

    #[test]
    fn pool_returns_none_after_drain() {
        let p = K8sPool::new();
        p.set_backends(vec!["a".into()]);
        assert_eq!(p.pick().as_deref(), Some("a"));
        p.set_backends(Vec::new());
        assert!(p.pick().is_none());
    }
}
