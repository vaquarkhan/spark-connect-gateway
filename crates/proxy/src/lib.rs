//! gRPC proxy for the Spark Connect service.
//!
//! Phase 1 design:
//!
//! 1. Accept inbound `spark.connect.SparkConnectService` traffic via tonic.
//! 2. For each RPC, derive a [`SessionKey`] (and an `operation_id` where
//!    applicable) from the request.
//! 3. Ask the [`Router`] for the backend address. The router consults the
//!    affinity store first; on miss, it picks from the pool and records the
//!    decision so subsequent calls stick to the same backend.
//! 4. Open or reuse a tonic [`Channel`] to that backend (via [`Dialer`]),
//!    forward the request, and pump any response stream back to the client.
//!
//! All twelve RPCs in the Spark Connect surface are forwarded; new RPCs
//! added by upstream Spark will surface here as `Unimplemented` until they
//! are wired in (no silent passthrough yet — Phase 4 may add a generic
//! tower-level fallback).

mod dial;
mod handler;

pub use dial::Dialer;
pub use handler::SparkConnectProxy;
