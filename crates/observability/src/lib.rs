//! Metrics, structured spans, and the admin HTTP server.
//!
//! The crate exposes three things:
//!
//! * [`Metrics`] — a `Clone`-able handle owning a Prometheus `Registry`
//!   and the per-RPC counters / histograms. Hand one to anything that
//!   needs to instrument requests.
//! * [`request_id`] — generates a per-RPC UUID v4 used as the
//!   correlation ID stamped into both the `tracing::Span` and the
//!   outbound gRPC `x-request-id` metadata.
//! * [`admin`] — a small Hyper-based HTTP server that serves
//!   `/metrics`, `/healthz`, `/readyz` on a separate admin port.

pub mod admin;
pub mod metrics;
pub mod tracing;

pub use admin::{serve_admin, AdminConfig, ReadinessProbe};
pub use metrics::{Metrics, MetricsError, RpcGuard, StreamGuard};
#[cfg(feature = "testing")]
pub use tracing::install_test_subscriber;
pub use tracing::{
    extract_parent, init_tracing, inject_context, TracingConfig, TracingError, TracingHandle,
    TRACEPARENT_HEADER, TRACER_NAME,
};

use uuid::Uuid;

/// Generate a fresh correlation ID for the current RPC.
pub fn request_id() -> String {
    Uuid::new_v4().to_string()
}

/// gRPC metadata key under which the gateway forwards correlation IDs
/// to backends. Public so the proxy crate can use the same constant.
pub const REQUEST_ID_HEADER: &str = "x-request-id";
