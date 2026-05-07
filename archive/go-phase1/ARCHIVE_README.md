# Go Phase 1 — Archived Reference Implementation

This directory contains the original Go implementation of Phase 1 of the
Spark Connect Gateway. It is **archived** in favour of the Rust rewrite at
the repository root.

## Why the rewrite

- **HTTP/2 trailing-header handling**: `hyper` is the most complete HTTP/2
  stack for gRPC; trailers are first-class. Other ecosystems either lack
  trailer support or require careful wiring.
- **Memory footprint and tail latency** for sustained streaming RPCs
  (`ExecutePlan`, `ReattachExecute`) — Rust's async/await + Tokio yields
  a smaller per-stream cost than Go's goroutine model.
- **Alignment with Kimahriman/spark-connect-proxy**, the only existing
  OSS Spark-Connect-native proxy, also Rust.

## What this archive is good for

- Reading the **design intent** of each Phase 1 module — the in-line
  comments capture the reasoning behind session affinity, op-id reverse
  indexing, the `forwardServerStream` helper, and the static-pool
  round-robin invariants.
- Cross-checking Rust behaviour against Go behaviour during the rewrite —
  any discrepancy is either a bug in the Rust port or a deliberate change
  worth documenting.

## What this archive is *not*

- It is not the production code. Do not deploy it.
- It will not be kept in sync with future plan changes.
