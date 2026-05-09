# Deployment guide

This guide takes you from "I have a Kubernetes cluster" to "PySpark
clients are landing on the right Spark Connect drivers and staying
there across gateway-pod restarts." It is written for the operator
who will own the deployment day-2 — the README explains *what* the
gateway is; this doc explains *how to run it well*.

## Prerequisites

| | Required | Why |
|---|---|---|
| Kubernetes | ≥ 1.24 | Chart uses `discovery.k8s.io/v1` EndpointSlices (GA in 1.21) and probe HTTP/2 fields stabilized in 1.24 |
| Helm | ≥ 4.0 | Chart uses Helm 4 schema. Earlier Helm versions will refuse to install. |
| Spark Connect servers | ≥ 1 reachable from the gateway pods | The gateway is a stateless proxy; it needs *something* to forward to |
| Redis | Optional but **strongly recommended** for HA | Multi-replica gateway needs shared affinity state — see [Redis decision](#picking-the-redis-backing) |

The gateway itself does not need any cluster-scoped permissions. The
only RBAC it ever needs is namespace-scoped read on `endpoints` /
`endpointslices`, and only when `backendDiscovery.type=k8s`. The
chart attaches that automatically.

## Day 0: a minimal install

Pick a namespace and use the chart defaults:

```bash
helm install scg ./deploy/helm/scg \
  --namespace spark-connect --create-namespace
```

Defaults give you:

* 2 gateway replicas (the smallest meaningful HA configuration)
* 1 bundled Redis StatefulSet with AOF persistence + 1Gi PVC
* A static backend list pointing at
  `spark-connect-1.svc.cluster.local:15002` and
  `spark-connect-2.svc.cluster.local:15002`
* No authentication (every caller is `user_id="anonymous"`)
* No OTel tracing (structured JSON logs only)

This is **enough for a smoke test on a trusted in-cluster network**.
It is *not* a production configuration; see the rest of this guide
for how to harden it.

Verify the install:

```bash
kubectl -n spark-connect rollout status deployment/scg
kubectl -n spark-connect get pods -l app.kubernetes.io/name=scg
kubectl -n spark-connect port-forward svc/scg 9090:9090 &
curl http://localhost:9090/readyz   # expect: 200 ready
curl http://localhost:9090/healthz  # expect: 200 ok
```

If `/readyz` returns 503, the backend pool is empty. See the
[runbook](runbook.md#readyz-stuck-on-503).

## Picking the backend discovery mode

The gateway needs a list of Spark Connect server addresses. Two
ways:

### `static` — explicit list

```yaml
backendDiscovery:
  type: static
  static:
    addresses:
      - "spark-connect-1.svc.cluster.local:15002"
      - "spark-connect-2.svc.cluster.local:15002"
```

Use this when:

* Spark Connect servers run *outside* this Kubernetes cluster (VMs,
  another cluster, an EMR-managed driver).
* You're scripting the address list from outside Helm anyway (CI
  produces it).
* You want zero RBAC on the gateway pod.

### `k8s` — Endpoints watch

```yaml
backendDiscovery:
  type: k8s
  k8s:
    namespace: spark-connect
    serviceName: spark-connect
    port: 15002
```

Use this when Spark Connect servers run as pods in *this* cluster
and you scale them up/down — the gateway picks up additions and
removals within seconds without a Helm rollout. This is the natural
match for the
[`apache/spark-kubernetes-operator`](https://github.com/apache/spark-kubernetes-operator)
deployment pattern (see
[`deploy/examples/spark-connect-server/`](../deploy/examples/spark-connect-server/)
in this repo).

The chart automatically attaches a `Role` and `RoleBinding` granting
`endpoints/endpointslices` get/list/watch in the target namespace
when `type: k8s`. If your namespaced backend lives in a *different*
namespace from the gateway release, the chart puts the Role in the
backend namespace and the RoleBinding pointing at the gateway's
ServiceAccount in the release namespace.

## Picking the Redis backing

The gateway maps `(user_id, session_id) -> backend` and pins each
session to that backend forever. Where that map lives is the most
important production decision.

### `affinityStore.type: memory` — single-replica only

`HashMap` in the gateway process. Simple, fast, lost on pod
restart. **Only safe with `replicaCount: 1`** — multiple replicas
each have their own map, and a load balancer that fans out across
replicas will repeatedly re-pin sessions to different backends,
breaking Spark Connect's per-driver `SparkSession` invariant
(temp views, cached frames, conf settings disappear).

The chart enforces this at template time: `replicaCount > 1` with
`affinityStore.type: memory` causes `helm install` to fail.

Use case: dev / PoC / single-tenant on a single-pod box. Not for
HA.

### `affinityStore.type: redis` — recommended

The default. The map lives in Redis; all replicas read/write the
same dataset.

Two flavors:

**Bundled** (`redis.enabled: true`, default):

```yaml
redis:
  enabled: true
  persistence:
    enabled: true
    size: 1Gi
```

A single-replica StatefulSet with AOF persistence. Good for dev,
staging, and small-scale production. The dataset is tiny (one entry
per active Spark session, a few hundred bytes), 1Gi is over-sized
for any realistic workload.

Limitations:

* Single replica — Redis itself is a SPOF. Restart drops in-flight
  reads briefly; AOF means the dataset survives.
* No auth, no TLS — fine on a private namespace, not for shared
  clusters.

**External managed Redis** (`redis.enabled: false`):

```yaml
redis:
  enabled: false
affinityStore:
  type: redis
  redis:
    url: rediss://elasticache.aws.example:6379
    keyPrefix: scg-prod
    sessionTtlSecs: 3600
    opTtlSecs: 900
```

Use this for any production deployment that takes uptime seriously
— ElastiCache, Cloud Memorystore, Bitnami Redis Helm chart with
Sentinel, etc. The gateway only requires basic single-node
semantics; no Redis Cluster features are used.

What URL format is supported:

* `redis://host:port`
* `redis://:password@host:port`
* `redis://:password@host:port/<db-index>`
* `rediss://host:port` (TLS)

### What happens when Redis is unreachable

The gateway logs a `warn!` per failed Redis call and **degrades
gracefully** — lookups return `None`, binds quietly drop. The
visible effect is that session stickiness stops working; sessions
land on whatever the pool picks each time, the same behaviour you'd
see with the in-memory store on a single replica.

Service stays up; HA stickiness recovers as soon as Redis does.
This is a deliberate design choice — failing requests because
Redis is slow would be worse than a brief stickiness gap.

## Picking the auth mode

| `auth.type` | When to use |
|---|---|
| `none` | Trusted in-cluster network only. Every caller is `user_id="anonymous"`. The gateway's own metrics are usable but you cannot tell tenants apart. |
| `static` | Dev / single-team. Bearer tokens listed in `values.yaml`. Tokens are sealed-secret-able but rotation requires `helm upgrade`. |
| `jwt` | You already issue your own JWTs and have the verification key handy (PEM file or HMAC secret). Audience + issuer claims are checked. |
| `oidc` | Production with an external IdP (Okta, Auth0, Google, Keycloak). The gateway fetches JWKS from `discovery_url`, caches keys, refreshes on `kid` miss with a configurable floor. |

For all four, the gateway *replaces* the client-supplied
`UserContext.user_id` on the forwarded request with the verified
identity, so a malicious client cannot impersonate another user just
by lying in `UserContext`.

Production rule of thumb: `oidc` if you have an IdP, `jwt` if you're
issuing JWTs in-house, `static` for a closed dev setup, `none` only
for namespace-isolated clusters.

## Day 1: hardening

The defaults give you function; production needs all of:

1. **Switch off `auth.type: none`.** External traffic to a
   gateway running with `none` is a "we trust the network" bet that
   ages badly.
2. **Use external Redis.** The bundled Redis is a SPOF. Pointing at
   ElastiCache / Memorystore costs little and removes the SPOF.
3. **Set resource requests/limits.** The chart defaults
   (100m CPU / 128Mi RAM) are reasonable for low traffic; size up
   to your real load.
4. **Turn on tracing if you have a collector.** The gateway exports
   OTLP/gRPC; logs already include the same correlation ID, but
   spans on a UI like Tempo or Jaeger make multi-hop investigations
   fast. See [`tracing.md`](observability.md#distributed-tracing)
   for the known limitation around inbound `traceparent`.
5. **Pin the image tag.** `image.tag: ""` resolves to the chart's
   `appVersion`. For production, set it to a specific digest:
   ```yaml
   image:
     repository: ghcr.io/<your-mirror>/spark-connect-gateway
     tag: "0.1.0@sha256:abc123..."
   ```
6. **Wire probes to your platform.** The chart already configures
   `/healthz` and `/readyz` on the admin port; if your platform
   does extra synthetic checks, point them at `/readyz` rather than
   the gRPC port (the gRPC port has no HTTP health endpoint, by
   gRPC convention).

## Day 2: upgrades

`helm upgrade` re-renders templates and rolls the Deployment. Two
things that catch operators:

### ConfigMap changes don't always trigger pod rolls — but the chart
makes them.

The Deployment template includes a `checksum/config` annotation
that hashes the rendered ConfigMap. Any value change → annotation
change → pod template change → rolling restart. You should *not*
need to manually `kubectl rollout restart`.

### Redis affinity state survives rolling upgrades.

Because affinity lives in Redis (or the in-memory store, which
dies with the pod anyway), a `helm upgrade` rolling restart does
*not* reshuffle existing sessions — they stay pinned to their
original backends. The
[`session_ttl_secs`](https://github.com/.../scg-store-redis/src/lib.rs)
is refreshed on every read, so active sessions don't expire during
the rollout window.

Sessions that haven't been touched for `session_ttl_secs` *will*
expire and be re-picked on next access; tune this if your client
idle pattern is unusual.

### Rollback

`helm rollback scg <revision>` works as expected. The Redis
StatefulSet's PVC is **not** deleted on uninstall (intentional —
see below), so a rollback that brings back an older Helm release
finds its data.

If a config change broke things, prefer `helm rollback` over
`kubectl edit` of the Deployment — the latter doesn't roll the
ConfigMap back in lockstep.

## Uninstall

```bash
helm uninstall scg -n spark-connect
```

This removes the Deployment, Service, ConfigMap, ServiceAccount,
RBAC, and the Redis StatefulSet itself — but **leaves the Redis
PVC behind**. This is intentional: a re-install picks up where the
old one left off, and unwanted PVCs are easy to spot in
`kubectl get pvc`.

To wipe Redis state explicitly:

```bash
kubectl -n spark-connect delete pvc -l app.kubernetes.io/component=redis
```

If you used external Redis (`redis.enabled: false`), the data
isn't ours to clean up — flush the prefix yourself:

```bash
redis-cli -u "$REDIS_URL" --scan --pattern "scg:*" | xargs redis-cli -u "$REDIS_URL" del
```

## Validation: end-to-end test against your cluster

The repo ships [`crates/proxy/examples/ha_smoke.rs`](../crates/proxy/examples/ha_smoke.rs)
which validates the three multi-replica HA invariants (shared
state, failover after replica death, op-id reverse index across
replicas) against any reachable Redis. To run it against your
deployed cluster:

```bash
kubectl -n spark-connect port-forward svc/scg-redis 6399:6379 &
REDIS_URL=redis://127.0.0.1:6399 cargo run -p scg-proxy --example ha_smoke
```

This spins up *its own* in-process gateway pair against your
deployed Redis, which proves the Redis is well-configured and the
chart's value mapping is correct. It does not exercise the
deployed *gateway pods* themselves; for that, see
[`observability.md`](observability.md) for what to watch in
metrics during a soak test.
