# Observability guide

What the gateway exposes, what it means, and where to look when
something is off. Operator-facing — pair with
[`runbook.md`](runbook.md) for "what do I do about it" and
[`deployment.md`](deployment.md) for "how do I configure it."

The gateway exposes three observation surfaces:

* **Prometheus metrics** at `/metrics` on the admin port (default
  `:9090`)
* **Structured JSON logs** to stdout, with a per-RPC correlation ID
  (`rid`)
* **OpenTelemetry traces** over OTLP/gRPC, when `tracing.enabled`

All three carry the same correlation ID for any given RPC, so you
can pivot from a metric anomaly to the corresponding log line to
the corresponding span.

## Suggested SLIs / SLOs

A starting point — adjust thresholds to your traffic.

| SLI | Definition | Starting SLO |
|---|---|---|
| **Availability** | `1 - rate(scg_rpcs_total{code!~"OK\|Cancelled\|InvalidArgument\|NotFound\|AlreadyExists\|FailedPrecondition\|OutOfRange\|Unimplemented\|Unauthenticated\|PermissionDenied"}[5m]) / rate(scg_rpcs_total[5m])` | 99.9% over 30d |
| **Tail latency** | `histogram_quantile(0.99, sum by (le, rpc) (rate(scg_rpc_duration_seconds_bucket{rpc="ExecutePlan"}[5m])))` | < 1s for non-streaming RPCs |
| **Auth failure rate** | `rate(scg_auth_failures_total[5m])` | Baseline + alarm on 5x departure |
| **Backend availability** | `scg_backend_pool_size > 0` | Always |

The status-code filter on availability deliberately excludes client
errors (`InvalidArgument`, `Unauthenticated`, etc.) — they are
*correct* gateway responses to bad input.

## Prometheus metrics

Cardinality budget: total label cardinality of `scg_*` is bounded
at chart-install time (12 RPC names × 17 status codes + small
fixed sets). No per-tenant labels, no per-session-id labels — those
belong in logs and traces, not metrics.

### `scg_rpcs_total{rpc, code}` — counter

Total per-RPC RPCs handled, labelled by RPC method and final gRPC
status code.

```promql
# Throughput
sum by (rpc) (rate(scg_rpcs_total[5m]))

# Error breakdown
sum by (rpc, code) (rate(scg_rpcs_total{code!="OK"}[5m]))

# Per-RPC error rate
sum by (rpc) (rate(scg_rpcs_total{code!="OK"}[5m]))
  / sum by (rpc) (rate(scg_rpcs_total[5m]))
```

### `scg_rpc_duration_seconds{rpc}` — histogram

Gateway-side end-to-end duration including the backend forward.
Buckets: 0.5ms, 1ms, 2.5ms, 5ms, 10ms, 25ms, 50ms, 100ms, 250ms,
500ms, 1s, 2.5s, 5s, 10s, 30s, 60s.

```promql
# p99 by RPC
histogram_quantile(0.99,
  sum by (le, rpc) (rate(scg_rpc_duration_seconds_bucket[5m])))

# Mean
sum(rate(scg_rpc_duration_seconds_sum[5m]))
  / sum(rate(scg_rpc_duration_seconds_count[5m]))
```

`ExecutePlan` and `ReattachExecute` are server-streaming RPCs;
their `_duration_seconds` measures the entire stream lifetime.
Long-running queries will produce histogram entries in the 30s+
buckets — that's expected, not a failure mode.

### `scg_auth_failures_total{reason}` — counter

Authentication failures, labelled by a small fixed-cardinality
reason: `missing_token`, `invalid_token`, `expired`, `unknown_kid`,
`unknown`.

```promql
# Auth failure rate, by reason
sum by (reason) (rate(scg_auth_failures_total[5m]))

# Spike in unknown_kid suggests upstream IdP key rotation the
# gateway hasn't picked up yet
rate(scg_auth_failures_total{reason="unknown_kid"}[5m]) > 0.1
```

`reason="unknown_kid"` is the canary for IdP key rotation: a fresh
JWKS arrived at the IdP, the gateway's cache hasn't refreshed
because the floor-rate-limit is in effect. The OIDC authenticator
self-heals — it'll refresh on the next call after the floor expires.

### `scg_backend_pool_size` — gauge

Current count of healthy backends the gateway will route to. Set
by:
* `static` pool: at startup, equal to `len(addresses)`. Never
  changes.
* `k8s` pool: updated whenever the Endpoints watcher emits an
  event. Starts at 0 until the first list event arrives.

```promql
# Alarm: pool went empty
scg_backend_pool_size == 0

# Alarm: pool shrank significantly (k8s discovery only)
delta(scg_backend_pool_size[5m]) < -2
```

A 0 pool size is the fast-path explanation for `/readyz` returning
503 and clients seeing `UNAVAILABLE`; see the
[runbook](runbook.md#readyz-stuck-on-503).

### `scg_active_streams` — gauge

In-flight streaming RPCs (`ExecutePlan`, `ReattachExecute`,
`AddArtifacts`). Useful for capacity planning — sustained high
values mean clients have many concurrent long-running queries.

```promql
# Streams per replica
sum by (pod) (scg_active_streams)

# Stream churn (drives goroutine churn under tonic)
rate(scg_active_streams[1m])
```

Spikes here without a matching spike in `scg_rpc_duration_seconds`
mean clients are opening streams but not reading from them — usually
a sign of buggy client code or a misbehaving gRPC keepalive.

## Structured JSON logs

Every RPC produces one or more log lines. The default formatter is
`tracing_subscriber::fmt().json()`, which yields one JSON object
per line. Send to stdout (the chart does this); aggregate with your
log pipeline of choice.

### Anatomy of a log line

```json
{
  "timestamp": "2026-05-09T08:33:35.146218Z",
  "level": "INFO",
  "fields": {
    "message": "forwarding",
    "rid": "ac54ca1b-894e-44e5-b9c9-8af38a3c1735",
    "rpc": "Config",
    "user": "alice",
    "session": "sess-1",
    "addr": "spark-connect-1.svc.cluster.local:15002"
  },
  "span": {
    "rpc_method": "Config",
    "rpc_service": "spark.connect.SparkConnectService",
    "rpc_system": "grpc",
    "scg_rid": "ac54ca1b-894e-44e5-b9c9-8af38a3c1735",
    "name": "scg_rpc"
  }
}
```

Fields you'll grep for:

| Field | Use |
|---|---|
| `rid` | Correlation ID per RPC. Same value appears in: outbound `x-request-id` metadata to the backend, OTLP span attributes (`scg_rid`), and the response trailers in some failure paths. **The single best primary key for cross-service investigation.** |
| `rpc` | Spark Connect RPC name (`Config`, `ExecutePlan`, …) |
| `user` | Verified `user_id` from the authenticator (not the client-supplied claim) |
| `session` | `session_id` from the request body |
| `addr` | Backend the gateway forwarded to |
| `error` | Present on failure log lines; carries the inner Status message |

### Useful queries

Loki / Grafana Logs:

```logql
# Trace one RPC end-to-end
{namespace="spark-connect"} | json | rid="ac54ca1b-..."

# Auth failures with reason
{namespace="spark-connect"} |= "auth" | json | level="WARN"

# All forwards to one backend
{namespace="spark-connect"} | json | addr="spark-connect-1.svc.cluster.local:15002"
```

Splunk:

```spl
index=k8s namespace="spark-connect" | spath "fields.rid" | search "fields.rid"="ac54ca1b-*"
```

## Distributed tracing

When `tracing.enabled: true`, every RPC opens a `scg_rpc` span on
the gateway and (a) parents it to any inbound W3C `traceparent`
header (when present), (b) injects a fresh `traceparent` for the
backend hop. Spans carry `rpc_method`, `rpc_system="grpc"`,
`rpc_service`, and `scg_rid` matching the log line.

### What you see in Tempo / Jaeger

For each RPC:

* The gateway span (`scg_rpc`, `rpc_method=ExecutePlan`)
* If the backend itself participates in tracing, a child span on
  the backend joined via the propagated `traceparent`
* Internal h2 / hyper / tonic spans below — useful for protocol-level
  debugging, noisy for application work. Filter by `target=scg_proxy`
  in your tracing UI to see only application spans.

### Known limitation: inbound-traceparent path

When a client sends an inbound `traceparent` header (i.e. it
already participates in distributed tracing upstream of the
gateway), the gateway's `scg_rpc` span is currently dropped before
reaching the OTLP exporter due to a versioning mismatch in
`tracing-opentelemetry`. The JSON log line still records the span
attributes; only the OTel export path is affected.

What this means in practice:

* PySpark clients **do not send `traceparent`** by default, so the
  common path produces full traces.
* If your pipeline puts the gateway behind another OTel-instrumented
  service mesh / API gateway that auto-injects `traceparent`, the
  gateway → backend hop will be visible in logs and metrics but the
  span won't appear in the trace UI.

This is tracked as a known issue from Phase 2.13. The structured
log line is the authoritative record for now.

## Suggested alerts (starting points)

Adjust thresholds to your traffic baseline.

```yaml
- alert: SCGGatewayDown
  expr: up{job="scg"} == 0
  for: 2m
  severity: critical

- alert: SCGNoHealthyBackends
  expr: scg_backend_pool_size == 0
  for: 5m
  severity: critical

- alert: SCGHighErrorRate
  expr: |
    sum by (rpc) (rate(scg_rpcs_total{code!~"OK|Cancelled|InvalidArgument|NotFound|AlreadyExists|FailedPrecondition|OutOfRange|Unimplemented|Unauthenticated|PermissionDenied"}[5m]))
      / sum by (rpc) (rate(scg_rpcs_total[5m]))
      > 0.05
  for: 10m
  severity: warning

- alert: SCGAuthFailureSpike
  expr: rate(scg_auth_failures_total[5m]) > 5 * avg_over_time(rate(scg_auth_failures_total[5m])[1h:5m])
  for: 5m
  severity: warning

- alert: SCGUnknownKidPersistent
  expr: rate(scg_auth_failures_total{reason="unknown_kid"}[15m]) > 0.1
  for: 15m
  severity: warning
  # OIDC self-heals via JWKS refresh; persistent unknown_kid suggests
  # the IdP rotated keys and our refresh floor is too aggressive.

- alert: SCGTailLatency
  expr: |
    histogram_quantile(0.99,
      sum by (le, rpc) (rate(scg_rpc_duration_seconds_bucket{rpc!~"ExecutePlan|ReattachExecute|AddArtifacts"}[5m])))
      > 1
  for: 10m
  severity: warning
  # Excludes streaming RPCs whose latency is the query lifetime, not gateway latency.
```

## Tying it together: investigating a failure

Typical flow when a metric alert fires:

1. **Metric** identifies the *what*: `SCGHighErrorRate` on
   `rpc=ExecutePlan`.
2. **Logs** identify *who and why*: filter to `rpc="ExecutePlan"`
   level `ERROR`, find a recent `rid`, look at the line — usually
   includes `error="<Status message>"` and `addr` of the offending
   backend.
3. **Backend logs** for that `addr` filtered by the same `rid`
   (forwarded as `x-request-id`) tell you whether it's a Spark-side
   failure (e.g. plan analysis error) or a network failure.
4. **Trace UI** with the same `rid` (in span attribute `scg_rid`)
   lets you see latency breakdown by phase — useful when the
   distinction between "gateway is slow" and "backend is slow"
   matters.

If step 1 shows pool size = 0, jump straight to the
[runbook entry](runbook.md#readyz-stuck-on-503) — the rest of the
investigation chain doesn't apply when nothing was forwarded.
