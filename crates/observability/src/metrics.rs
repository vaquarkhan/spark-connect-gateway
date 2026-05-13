//! Prometheus metrics for the gateway.
//!
//! Naming follows the Prometheus best practices: `scg_*` prefix,
//! `_total` suffix on counters, `_seconds` suffix on duration
//! histograms, label cardinality kept small.
//!
//! Cardinality budget:
//!
//! * `rpc` — fixed set of 12 Spark Connect RPC names. Bounded.
//! * `code` — gRPC status codes ("OK", "Unauthenticated", …). ~16 values.
//! * `reason` — auth failure reason. Small fixed set.
//! * **No** `user_id`, `tenant`, `session_id` labels — those would
//!   blow up cardinality. Per-user metrics belong in a logging
//!   pipeline (Loki / Splunk), not in Prometheus.

use std::sync::Arc;
use std::time::Instant;

use prometheus::{
    register_histogram_vec_with_registry, register_int_counter_vec_with_registry,
    register_int_gauge_with_registry, HistogramTimer, HistogramVec, IntCounterVec, IntGauge,
    Registry,
};

#[derive(Debug, thiserror::Error)]
pub enum MetricsError {
    #[error("registering metric: {0}")]
    Register(#[from] prometheus::Error),
}

/// Cheap-to-clone handle to the gateway's Prometheus registry and
/// per-RPC metric families. All metric families are eagerly registered
/// at construction so scrapes always return a complete metric set, even
/// before any traffic has flowed.
#[derive(Clone)]
pub struct Metrics {
    inner: Arc<MetricsInner>,
}

struct MetricsInner {
    registry: Registry,

    // Per-RPC families.
    rpcs_total: IntCounterVec,
    rpc_duration_seconds: HistogramVec,

    // Auth.
    auth_failures_total: IntCounterVec,

    // Pool / streams gauges.
    backend_pool_size: IntGauge,
    active_streams: IntGauge,

    // Rate limit.
    rate_limit_rejected_total: IntCounterVec,
    /// Redis-side errors observed by the distributed rate limiter
    /// (Phase 3.7). Counts errors, not rejects — a fail-open
    /// deployment increments this without firing
    /// `rate_limit_rejected_total`. `reason` is one of a small fixed
    /// set: `tenant_bucket`, `user_bucket`.
    rate_limit_redis_errors_total: IntCounterVec,
}

impl Metrics {
    /// Build a fresh `Metrics` against a new `Registry`. Tests and the
    /// gateway main both use this; every gateway process owns one
    /// `Metrics` for the lifetime of the process.
    pub fn new() -> Result<Self, MetricsError> {
        let registry = Registry::new();
        let rpcs_total = register_int_counter_vec_with_registry!(
            "scg_rpcs_total",
            "Total Spark Connect RPCs handled by the gateway, labelled by method and final gRPC code.",
            &["rpc", "code"],
            registry,
        )?;
        let rpc_duration_seconds = register_histogram_vec_with_registry!(
            "scg_rpc_duration_seconds",
            "Per-RPC handler duration (gateway-side, end-to-end including backend forward).",
            &["rpc"],
            // Buckets: 0.5 ms, 1 ms, 2.5 ms, 5 ms, 10 ms, 25 ms, 50 ms,
            // 100 ms, 250 ms, 500 ms, 1 s, 2.5 s, 5 s, 10 s, 30 s, 60 s.
            // Spans the range from in-memory unary RPCs to long-running
            // ExecutePlan streams.
            vec![
                0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
                10.0, 30.0, 60.0,
            ],
            registry,
        )?;
        let auth_failures_total = register_int_counter_vec_with_registry!(
            "scg_auth_failures_total",
            "Authentication failures, labelled by reason (e.g. missing_token, invalid_token, expired).",
            &["reason"],
            registry,
        )?;
        let backend_pool_size = register_int_gauge_with_registry!(
            "scg_backend_pool_size",
            "Current number of healthy backends the gateway can route to.",
            registry,
        )?;
        let active_streams = register_int_gauge_with_registry!(
            "scg_active_streams",
            "Currently in-flight server-streaming or client-streaming RPCs.",
            registry,
        )?;
        let rate_limit_rejected_total = register_int_counter_vec_with_registry!(
            "scg_rate_limit_rejected_total",
            "RPCs rejected by per-tenant rate limiting, labelled by tenant and scope (tenant|user).",
            &["tenant", "scope"],
            registry,
        )?;
        let rate_limit_redis_errors_total = register_int_counter_vec_with_registry!(
            "scg_rate_limit_redis_errors_total",
            "Backend errors from the Redis-backed rate limiter (Phase 3.7). Counts failures, not rejects.",
            &["tenant", "reason"],
            registry,
        )?;
        Ok(Self {
            inner: Arc::new(MetricsInner {
                registry,
                rpcs_total,
                rpc_duration_seconds,
                auth_failures_total,
                backend_pool_size,
                active_streams,
                rate_limit_rejected_total,
                rate_limit_redis_errors_total,
            }),
        })
    }

    /// Bump `scg_rate_limit_redis_errors_total{tenant, reason}`.
    /// `reason` is a small fixed-cardinality string from the limiter
    /// (`tenant_bucket`, `user_bucket`).
    pub fn record_rate_limit_redis_error(&self, tenant: &str, reason: &str) {
        self.inner
            .rate_limit_redis_errors_total
            .with_label_values(&[tenant, reason])
            .inc();
    }

    /// Bump `scg_rate_limit_rejected_total{tenant, scope}`. `scope`
    /// is the fixed-cardinality string `"tenant"` or `"user"`. The
    /// label cardinality is bounded by the configured-tenant set —
    /// callers should not pass user-input tenants that aren't in
    /// the resolver's allowlist.
    pub fn record_rate_limit_reject(&self, tenant: &str, scope: &str) {
        self.inner
            .rate_limit_rejected_total
            .with_label_values(&[tenant, scope])
            .inc();
    }

    /// Underlying `Registry` — handed to the admin HTTP server so the
    /// `/metrics` endpoint can render it in the Prometheus exposition
    /// format.
    pub fn registry(&self) -> &Registry {
        &self.inner.registry
    }

    /// Start a per-RPC timer + observation guard. The guard records
    /// completion (with the final gRPC code) on Drop, so callers don't
    /// have to remember to call anything from error paths. See
    /// [`RpcGuard::record`] to override the default code.
    pub fn rpc_guard(&self, rpc: &'static str) -> RpcGuard {
        let timer = self
            .inner
            .rpc_duration_seconds
            .with_label_values(&[rpc])
            .start_timer();
        RpcGuard {
            metrics: self.clone(),
            rpc,
            code: "Cancelled", // Replaced before drop unless caller bails early.
            recorded: false,
            _timer: timer,
            started_at: Instant::now(),
        }
    }

    /// Convenience for code paths that observe a final gRPC code
    /// without needing the duration timer (e.g. early auth rejection
    /// after a guard-less code path).
    pub fn record_rpc(&self, rpc: &'static str, code: &str) {
        self.inner.rpcs_total.with_label_values(&[rpc, code]).inc();
    }

    /// Bump the auth-failures counter. `reason` should be a small
    /// closed set: "missing_token", "invalid_token", "expired",
    /// "unknown_kid", "unknown".
    pub fn record_auth_failure(&self, reason: &'static str) {
        self.inner
            .auth_failures_total
            .with_label_values(&[reason])
            .inc();
    }

    /// Set the live backend-pool size. Call from the K8s watcher / on
    /// pool reconfiguration.
    pub fn set_backend_pool_size(&self, n: i64) {
        self.inner.backend_pool_size.set(n);
    }

    /// Increment the active-streams gauge for the lifetime of `_guard`.
    /// Use the returned [`StreamGuard`] for RAII-style accounting.
    pub fn stream_guard(&self) -> StreamGuard {
        self.inner.active_streams.inc();
        StreamGuard {
            metrics: self.clone(),
        }
    }

    /// Read the live `scg_active_streams` count. Used by the
    /// graceful-shutdown loop to decide whether the server can stop.
    pub fn active_streams_value(&self) -> i64 {
        self.inner.active_streams.get()
    }
}

impl std::fmt::Debug for Metrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Metrics")
            .field("registered_families", &self.inner.registry.gather().len())
            .finish()
    }
}

/// Per-RPC observation guard. Records the final gRPC code via
/// [`RpcGuard::record`]; if the guard is dropped without `record`, the
/// outcome is recorded as `Cancelled` so we don't lose the request from
/// the totals.
#[must_use = "RpcGuard records on Drop; capture it for the lifetime of the handler"]
pub struct RpcGuard {
    metrics: Metrics,
    rpc: &'static str,
    code: &'static str,
    recorded: bool,
    _timer: HistogramTimer,
    started_at: Instant,
}

impl RpcGuard {
    /// Override the final gRPC code reported by this guard. Common
    /// values: "OK", "Unauthenticated", "Unavailable", "Internal".
    /// Calling `record` more than once is a no-op after the first.
    pub fn record(&mut self, code: &'static str) {
        if self.recorded {
            return;
        }
        self.recorded = true;
        self.code = code;
    }

    /// Elapsed time since the guard was created. Useful in log lines
    /// where you want both the metric and a per-request log.
    pub fn elapsed(&self) -> std::time::Duration {
        self.started_at.elapsed()
    }
}

impl Drop for RpcGuard {
    fn drop(&mut self) {
        // If the handler returned early without calling `record`, we
        // still want a count so the user can see the totals match
        // request volume. Default code "Cancelled" stays.
        self.metrics.record_rpc(self.rpc, self.code);
        // _timer also drops here, recording the histogram observation.
    }
}

/// RAII guard that decrements `scg_active_streams` on drop.
#[must_use = "drop the guard at the end of the streaming RPC"]
pub struct StreamGuard {
    metrics: Metrics,
}

impl Drop for StreamGuard {
    fn drop(&mut self) {
        self.metrics.inner.active_streams.dec();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read_counter(m: &Metrics, name: &str, labels: &[(&str, &str)]) -> u64 {
        let mfs = m.registry().gather();
        for mf in mfs {
            if mf.name() != name {
                continue;
            }
            for metric in mf.get_metric() {
                let matches = labels.iter().all(|(k, v)| {
                    metric
                        .get_label()
                        .iter()
                        .any(|l| l.name() == *k && l.value() == *v)
                });
                if matches {
                    return metric.get_counter().value() as u64;
                }
            }
        }
        0
    }

    fn read_gauge(m: &Metrics, name: &str) -> i64 {
        let mfs = m.registry().gather();
        for mf in mfs {
            if mf.name() == name {
                if let Some(metric) = mf.get_metric().first() {
                    return metric.get_gauge().value() as i64;
                }
            }
        }
        0
    }

    #[test]
    fn rpc_guard_records_ok_on_record() {
        let m = Metrics::new().unwrap();
        {
            let mut g = m.rpc_guard("Config");
            g.record("OK");
        }
        assert_eq!(
            read_counter(&m, "scg_rpcs_total", &[("rpc", "Config"), ("code", "OK")]),
            1
        );
    }

    #[test]
    fn rpc_guard_defaults_to_cancelled_on_early_drop() {
        let m = Metrics::new().unwrap();
        {
            let _g = m.rpc_guard("ExecutePlan");
            // simulate handler bailing without explicit record
        }
        assert_eq!(
            read_counter(
                &m,
                "scg_rpcs_total",
                &[("rpc", "ExecutePlan"), ("code", "Cancelled")]
            ),
            1
        );
    }

    #[test]
    fn auth_failure_increments() {
        let m = Metrics::new().unwrap();
        m.record_auth_failure("invalid_token");
        m.record_auth_failure("invalid_token");
        m.record_auth_failure("missing_token");
        assert_eq!(
            read_counter(
                &m,
                "scg_auth_failures_total",
                &[("reason", "invalid_token")]
            ),
            2
        );
        assert_eq!(
            read_counter(
                &m,
                "scg_auth_failures_total",
                &[("reason", "missing_token")]
            ),
            1
        );
    }

    #[test]
    fn pool_size_gauge_is_settable() {
        let m = Metrics::new().unwrap();
        m.set_backend_pool_size(5);
        assert_eq!(read_gauge(&m, "scg_backend_pool_size"), 5);
        m.set_backend_pool_size(0);
        assert_eq!(read_gauge(&m, "scg_backend_pool_size"), 0);
    }

    #[test]
    fn stream_guard_increments_then_decrements() {
        let m = Metrics::new().unwrap();
        let g1 = m.stream_guard();
        let g2 = m.stream_guard();
        assert_eq!(read_gauge(&m, "scg_active_streams"), 2);
        drop(g1);
        assert_eq!(read_gauge(&m, "scg_active_streams"), 1);
        drop(g2);
        assert_eq!(read_gauge(&m, "scg_active_streams"), 0);
    }
}
