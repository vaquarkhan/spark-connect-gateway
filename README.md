# spark-connect-gateway

A stateless gRPC proxy that fronts a pool of [Apache Spark Connect][1]
servers, providing session affinity, multi-tenant routing, and (in later
phases) auth and observability features the open-source server intentionally
leaves out.

> **Status: Phase 1 (MVP) complete.** Rust workspace built with
> [`tonic`](https://github.com/hyperium/tonic). Phase 1 is a passthrough
> proxy — no auth, no HA, no observability. See [the implementation plan][2]
> for the full roadmap.

## What it does

- Accepts `sc://` traffic on `:15003`.
- Forwards every Spark Connect RPC (`ExecutePlan`, `AnalyzePlan`, `Config`,
  `AddArtifacts`, `Interrupt`, `ReattachExecute`, `ReleaseExecute`,
  `ReleaseSession`, `FetchErrorDetails`, `CloneSession`, `ArtifactStatus`,
  `GetStatus`) to a chosen backend.
- Pins each `(user_id, session_id)` to the same backend for the lifetime of
  the session — required because open-source Spark Connect keeps
  `SparkSession` state in driver-local memory.
- Routes `ReattachExecute` / `ReleaseExecute` / `Interrupt` by `operation_id`
  via a reverse index, so reconnecting clients reach the backend that owns
  the operation even if the session affinity has expired.
- Round-robins new sessions across a static list of backend addresses.

## Why a gateway?

Open-source Spark Connect ships an excellent client-server protocol but
deliberately leaves multi-instance coordination out of scope. For anything
beyond a single Spark driver — multi-tenant platforms, HA, fleet-level
observability — you need a layer in front of the servers. See
[`OPEN_SOURCE_SPARK_CONNECT_GATEWAY_ANALYSIS.md`][3] in the plan repo for
the full motivation and competitive landscape.

## Why Rust?

- gRPC streaming proxy is exactly Rust's sweet spot — async/await + Tokio
  yield lower memory footprint and tail latency than Go for sustained
  `ExecutePlan` streams.
- `hyper` has best-in-class HTTP/2 trailing-header support, which gRPC
  requires.
- Aligns with [`Kimahriman/spark-connect-proxy`][5], the only existing OSS
  Spark-Connect-native proxy.

## Quick start

### Build and test

```bash
cargo build --workspace
cargo test --workspace
```

### Run locally against a Spark Connect server

1. Start a Spark Connect server on `localhost:15002` (see
   [`test/integration/README.md`](test/integration/README.md) for a Docker
   one-liner).

2. Write `config.yaml`:

   ```yaml
   bind_addr: ":15003"
   backends:
     - "127.0.0.1:15002"
   ```

3. Run the gateway:

   ```bash
   cargo run --bin gateway -- --config config.yaml
   ```

4. Point a Spark Connect client at it:

   ```python
   from pyspark.sql import SparkSession
   spark = SparkSession.builder.remote("sc://localhost:15003").getOrCreate()
   spark.range(10).count()  # → 10
   ```

### Observability

The gateway exposes a Prometheus `/metrics` endpoint, plus
`/healthz` and `/readyz` probes, on a separate `admin_addr`
(default `:9090`). Set `admin_addr: null` to disable.

Metric set (snake_case, `scg_` prefix, label cardinality bounded):

| Metric | Type | Labels | What |
|---|---|---|---|
| `scg_rpcs_total` | counter | `rpc`, `code` | Per-RPC totals tagged by final gRPC status code |
| `scg_rpc_duration_seconds` | histogram | `rpc` | Gateway-side end-to-end duration |
| `scg_auth_failures_total` | counter | `reason` | Failed auth (`missing_token`, `invalid_token`, `expired`, `unknown_kid`, `unknown`) |
| `scg_backend_pool_size` | gauge | — | Current healthy-backend count |
| `scg_active_streams` | gauge | — | In-flight streaming RPCs (`ExecutePlan`, `ReattachExecute`, `AddArtifacts`) |

Per-RPC structured logs include a correlation ID (`rid`); the same
ID is forwarded to the backend via `x-request-id` metadata so backend
logs can be joined.

#### Distributed tracing (OTLP)

Off by default. Add a `tracing:` block to enable OpenTelemetry span
export to an OTLP/gRPC collector:

```yaml
tracing:
  endpoint: "http://otel-collector:4317"
  service_name: "spark-connect-gateway"
  sample_ratio: 1.0           # ParentBased(TraceIdRatioBased(N))
  export_timeout_secs: 10
```

Each RPC opens an `info`-level `scg_rpc` span carrying
`rpc_method`, `rpc_system="grpc"`, `rpc_service`, and the same
`scg_rid` correlation ID surfaced in the JSON logs. Inbound W3C
`traceparent` metadata becomes the parent of that span; the gateway
re-injects its own `traceparent` (alongside `x-request-id`) on the
outbound request, so a Spark Connect server that participates in
tracing joins the same trace.

When the `endpoint` is omitted or the whole `tracing:` block is
absent, the gateway runs with structured JSON logs only — no OTel
SDK is initialized.

> **Known limitation (root spans only).** Today, only RPCs where the
> gateway is the trace root (no inbound `traceparent`) export their
> spans reliably end-to-end via OTLP. RPCs that arrive *with* a W3C
> `traceparent` set by an upstream caller continue to log to JSON
> (with the inbound `scg_rid` correlation ID), but their `scg_rpc`
> span is dropped before reaching the OTLP exporter due to a
> versioning mismatch in the `tracing-opentelemetry` ↔
> `opentelemetry_sdk` interaction around `Context::with_remote_span_context`.
> Tracking upstream — distributed-trace continuity from upstream
> caller → gateway → backend will return once the SDK fix lands; the
> gateway → backend hop already injects `traceparent` correctly so
> the backend half of the trace will start working as soon as the
> gateway-side path is fixed.

Scrape config example for Prometheus:

```yaml
scrape_configs:
  - job_name: spark-connect-gateway
    kubernetes_sd_configs:
      - role: pod
    relabel_configs:
      - source_labels: [__meta_kubernetes_pod_container_port_name]
        regex: admin
        action: keep
```

### Authentication

By default the gateway runs without authentication (every caller is
`user_id: anonymous`). Production deployments configure one of three
authenticators:

```yaml
# Bearer-token allowlist (dev / single-team):
auth:
  type: static
  tokens:
    - { token: "alice-secret", user_id: "alice", tenant: "team-a", groups: ["devs"] }
    - { token: "bob-secret",   user_id: "bob" }
```

```yaml
# JWT signed by a known IdP, verified against a local public key:
auth:
  type: jwt
  algorithms: ["RS256"]
  issuer: "https://idp.example.com"
  audience: "spark-connect-gateway"
  key:
    kind: pem_file
    path: /etc/gateway/idp-pub.pem
```

```yaml
# OIDC / JWKS — gateway fetches keys from the IdP, refreshes on rotation:
auth:
  type: oidc
  algorithms: ["RS256"]
  discovery_url: "https://idp.example.com/.well-known/openid-configuration"
  audience: "spark-connect-gateway"
```

In all three cases the gateway *replaces* whatever `user_id` the client
declares in `UserContext` with the verified identity from the
authenticator — clients cannot impersonate other users.

Clients pass the credential via gRPC metadata:

```python
# PySpark Spark Connect client picks up the token from the URI:
spark = SparkSession.builder.remote(
    "sc://localhost:15003/;token=alice-secret"
).getOrCreate()
```

### Sharing affinity across gateway replicas (Redis)

The default in-memory affinity store works only for a single gateway
replica — every replica keeps its own `(user_id, session_id) -> backend`
table, so a client that lands on replica B after replica A bound it
to backend X gets re-bound to a different backend, breaking the
Spark Connect per-driver session invariant.

To run the gateway with `replicas > 1` (e.g. behind a Kubernetes
`Service` for HA), point the gateway at a Redis instance:

```yaml
bind_addr: ":15003"
backends: ["spark-connect-1:15002", "spark-connect-2:15002"]

affinity_store:
  type: redis
  url: "redis://redis-cluster:6379"        # supports redis://:pw@host:6379/2
  key_prefix: "scg-prod"                   # default "scg"
  session_ttl_secs: 3600                   # default 1h, refreshed on reads
  op_ttl_secs: 900                         # default 15min
```

Both stickiness invariants are preserved across replicas:

* `bind_session_if_absent` uses Redis `SET … NX EX` — when two
  replicas race on the same session, exactly one wins; the loser
  reads the winner's value back and routes to the same backend.
* The op-id reverse index (used by `ReattachExecute` /
  `ReleaseExecute` / `Interrupt`) lives in `{prefix}:o:{op_id}`,
  so a client reconnecting through a different replica still
  reaches the original driver.

If Redis becomes unreachable, the gateway logs `warn!` per failed
operation and degrades to pool-only routing — sessions land on
whatever backend the pool picks each time, which is exactly the
Phase-1 in-memory behaviour seen by a single replica. Service
remains available; HA stickiness recovers as soon as Redis does.

`affinity_store` defaults to `type: memory` (no entry needed),
matching Phase-1 behaviour.

#### Verifying HA locally

`crates/proxy/examples/ha_smoke.rs` spins up two real
`SparkConnectProxy` instances backed by one Redis and a shared pool
of fake backends, then drives RPCs through different replicas to
prove three invariants:

* **Shared state** — a session bound through replica A resolves to
  the same backend through replica B.
* **Failover** — after replica A is killed, the same session through
  replica B still hits the original backend.
* **Op-id reverse index across replicas** — `ReattachExecute(op_id,
  session_id="different")` arriving at replica B (after A is gone)
  still reaches the backend that ran the original `ExecutePlan`.

Run with a Redis listening on `:6399` (or override via `REDIS_URL`):

```bash
redis-server --port 6399 --daemonize yes
cargo run -p scg-proxy --example ha_smoke
```

Exits zero on success; panics with a descriptive assertion message
on any failure, so a CI script can wrap it directly.

### Deploy on Kubernetes (Helm)

A Helm chart is shipped at [`deploy/helm/scg/`](deploy/helm/scg/).
Quickstart:

```bash
helm install scg ./deploy/helm/scg \
  --namespace spark-connect --create-namespace
```

Defaults give you 2 gateway replicas + a bundled Redis StatefulSet
(AOF-persisted), with a static backend list pointing at
`spark-connect-{1,2}.svc.cluster.local:15002`. Switch to K8s
service-watch discovery, JWT auth, OTLP tracing, or an external
managed Redis with a few values flips — see the chart's
[`values.yaml`](deploy/helm/scg/values.yaml) and
[`README.md`](deploy/helm/scg/README.md) for the full reference.

The chart fails template-time if you set `replicaCount > 1` together
with `affinityStore.type: memory`, since that combination silently
breaks Spark Connect's per-driver session invariant — `redis` is the
default for exactly that reason.

### Run on Kubernetes (auto-discovery)

See [`deploy/examples/spark-connect-server/`](deploy/examples/spark-connect-server/)
for sample manifests that stand up two Spark Connect servers via the upstream
[`apache/spark-kubernetes-operator`][4].

Once those servers (and a fronting `Service`) exist, point the gateway at the
Service's `Endpoints` and let the gateway pick up backends automatically:

```yaml
bind_addr: ":15003"
backend_discovery:
  type: k8s
  namespace: spark-connect
  service_name: spark-connect
  port: 15002
```

The gateway watches the `Endpoints` object via `kube-rs`. When pods are added,
removed, or restarted, the gateway's backend list updates within seconds —
no `kubectl rollout` of the gateway, no config edit. The gateway pod needs a
`ServiceAccount` with `get`, `list`, and `watch` on `endpoints` in the target
namespace.

Phase 2 will add a Helm chart that wires up the `ServiceAccount` and `Role`
for you.

## Architecture

```
client (sc://) ──▶ gateway ──┬──▶ Spark Connect server #1
                             ├──▶ Spark Connect server #2
                             └──▶ Spark Connect server #N
```

- **No state in the gateway process** beyond an in-memory affinity cache.
  Phase 2 moves that state to Redis or Postgres so multiple gateway replicas
  can share it.
- **No interpretation of Spark Connect plans.** The gateway forwards every
  message verbatim, which means it stays compatible with whatever upstream
  Spark Connect adds in future versions.

## Workspace layout

```
crates/
  gateway/       # binary entry point
  proxy/         # SparkConnectService impl that forwards every RPC
  routing/       # SessionKey, Pool/AffinityStore traits, Router
  store-memory/  # Phase-1 in-memory affinity store
  pool-static/   # static backend pool (round-robin)
  config/        # YAML config loader
  genproto/      # tonic-generated bindings for spark.connect.*
proto/spark/connect/
  *.proto        # vendored read-only mirror of upstream
deploy/examples/
  spark-connect-server/  # K8s manifests (apache/spark-kubernetes-operator)
test/integration/
  README.md, client_smoke.py  # real PySpark E2E
archive/go-phase1/
  …              # original Go MVP, kept as design reference
```

## Regenerating proto bindings

The `crates/genproto/build.rs` script invokes `tonic-prost-build` on every
`cargo build`. To force a regeneration:

```bash
cargo clean -p scg-genproto
cargo build -p scg-genproto
```

`protoc` must be on `$PATH` (e.g. `brew install protobuf`).

## Roadmap

- **Phase 1 — MVP (this).** Streaming proxy, static pool, in-memory
  affinity, in-process tests against fake backends.
- **Phase 2 — Production.** JWT/OIDC auth, K8s service-watch backend pool,
  Redis/Postgres affinity store, Prometheus metrics, OpenTelemetry tracing,
  Helm chart, multi-replica HA.
- **Phase 3 — Multi-tenant.** Per-tenant backend pools, cold-start
  provisioning, warm pools, rate limiting, audit logging.

See [the full plan][2] for details.



If `cargo` fails with TLS errors against `crates.io` or `index.crates.io`,
the registry is blocked at the network level. Configure
`~/.cargo/config.toml` to use the internal proxy as documented at
your internal documentation.

## License

Apache 2.0 (planned). The vendored Spark Connect protos under
`proto/spark/connect/` are themselves under the Apache 2.0 license held by
the Apache Software Foundation.

[1]: https://spark.apache.org/docs/latest/spark-connect-overview.html
[2]: ../plans/IMPLEMENTATION-PLAN-OSS-Spark-Connect-Gateway.md
[3]: ../plans/OPEN_SOURCE_SPARK_CONNECT_GATEWAY_ANALYSIS.md
[4]: https://github.com/apache/spark-kubernetes-operator
[5]: https://github.com/Kimahriman/spark-connect-proxy
