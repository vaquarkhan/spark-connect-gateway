//! Generated tonic bindings for the upstream `spark.connect.*` proto surface.
//!
//! The proto files under `proto/spark/connect/` are a read-only mirror of the
//! corresponding files in `apache/spark`. Bindings are regenerated at build
//! time by `build.rs`.
//!
//! Lints disabled here are intrinsic to prost-generated code, not bugs we
//! should fix:
//! * `large_enum_variant` — Spark Connect oneofs are inherently sized by
//!   their largest variant; boxing every variant would be invasive and
//!   would make the proto API noticeably worse to use.
//! * `clippy::all` more broadly: generated code is not human-authored, so
//!   lint findings are not actionable.

#![allow(clippy::all)]
#![allow(clippy::pedantic)]

pub mod spark {
    pub mod connect {
        tonic::include_proto!("spark.connect");
    }
}

pub use spark::connect as pb;
