# Integration tests

Two flavours.

## In-process tests (`internal/proxy/proxy_test.go`)

These run against in-process fake Spark Connect backends. They cover:

- Unary RPC forwarding (`Config`)
- Server-streaming forwarding emits all upstream messages (`ExecutePlan`)
- Session affinity stickiness across many calls
- Op-id reverse index — a `ReattachExecute` lacking the original `session_id`
  still routes to the backend that handled the original `ExecutePlan`

Run them with the rest of the unit suite:

```bash
go test -race -count=1 ./...
```

## Real Spark Connect E2E (manual)

This validates the gateway against a real Spark Connect server using a real
PySpark client. It requires Docker (or a local Spark install).

### 1. Start a Spark Connect server

```bash
docker run --rm -d --name sc-server \
  -p 15002:15002 \
  apache/spark:4.0.0 \
  /opt/spark/sbin/start-connect-server.sh \
  --packages org.apache.spark:spark-connect_2.13:4.0.0 \
  --conf spark.connect.grpc.binding.port=15002
```

Wait ~10s for the server to come up.

### 2. Configure and start the gateway

`config.yaml`:

```yaml
bind_addr: ":15003"
backends:
  - "127.0.0.1:15002"
```

```bash
go run ./cmd/gateway --config config.yaml
```

### 3. Run the PySpark client

```bash
pip install pyspark==4.0.0
python3 test/integration/client_smoke.py
```

The script asserts that:

- A trivial `spark.range(10).count()` returns `10`
- A 3-column DataFrame round-trips through the gateway with correct schema and rows

### 4. Cleanup

```bash
docker rm -f sc-server
```

## Why two layers?

The in-process tests prove the gateway's gRPC forwarding logic is correct
without depending on a Spark distribution. The real-Spark E2E test catches
issues that only show up against an actual Spark Connect server (e.g. Arrow
batch encoding, large message handling, header propagation).

CI runs the in-process suite on every commit. The real-Spark E2E is run on
demand and as part of release validation.
