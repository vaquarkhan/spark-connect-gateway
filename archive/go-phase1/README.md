# spark-connect-gateway

A stateless gRPC proxy that fronts a pool of [Apache Spark Connect][1]
servers, providing session affinity, multi-tenant routing, and (in later
phases) auth and observability features the open-source server intentionally
leaves out.

> **Status: Phase 1 (MVP) complete.**
> Phase 1 is a proof-of-concept passthrough proxy. No auth, no HA, no
> observability — see [the implementation plan][2] for the full roadmap.

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

## Quick start

### Build and test

```bash
make build
make test
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
   ./bin/gateway --config config.yaml
   ```

4. Point a Spark Connect client at it:

   ```python
   from pyspark.sql import SparkSession
   spark = SparkSession.builder.remote("sc://localhost:15003").getOrCreate()
   spark.range(10).count()  # → 10
   ```

### Run on Kubernetes

See [`deploy/examples/spark-connect-server/`](deploy/examples/spark-connect-server/)
for sample manifests that stand up two Spark Connect servers via the upstream
[`apache/spark-kubernetes-operator`][4]. Phase 2 will add a Helm chart for
the gateway itself.

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

## Development

### Layout

```
cmd/gateway/        # main
internal/
  proxy/            # gRPC server + per-RPC forwarding
  routing/          # session/op routing decisions
  pool/static/      # static backend list
  store/memory/     # in-memory affinity store (Phase 1)
  config/           # YAML config
  genproto/         # generated Spark Connect Go bindings
proto/spark/connect/  # vendored .proto files (read-only mirror of upstream)
deploy/             # K8s reference manifests
test/integration/   # E2E test docs and scripts
docs/               # architecture / deployment docs (Phase 2+)
scripts/            # tooling (proto regeneration, …)
```

### Regenerating Go bindings

```bash
GOPROXY=https://YOUR-INTERNAL-GO-PROXY \
  go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
GOPROXY=https://YOUR-INTERNAL-GO-PROXY \
  go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
make proto-gen
```

## Roadmap

- **Phase 1 — MVP (this).** Streaming proxy, static pool, in-memory affinity,
  in-process tests against fake backends.
- **Phase 2 — Production.** JWT/OIDC auth, K8s service-watch backend pool,
  Redis/Postgres affinity store, Prometheus metrics, OpenTelemetry tracing,
  Helm chart, multi-replica HA.
- **Phase 3 — Multi-tenant.** Per-tenant backend pools, cold-start
  provisioning, warm pools, rate limiting, audit logging.

See [the full plan][2] for details.

## License

Apache 2.0 (planned). The vendored Spark Connect protos under
`proto/spark/connect/` are themselves under the Apache 2.0 license held by
the Apache Software Foundation.

[1]: https://spark.apache.org/docs/latest/spark-connect-overview.html
[2]: ../plans/IMPLEMENTATION-PLAN-OSS-Spark-Connect-Gateway.md
[3]: ../plans/OPEN_SOURCE_SPARK_CONNECT_GATEWAY_ANALYSIS.md
[4]: https://github.com/apache/spark-kubernetes-operator
