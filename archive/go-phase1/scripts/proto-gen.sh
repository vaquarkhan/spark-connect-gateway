#!/usr/bin/env bash
# Regenerate Go bindings for the vendored spark.connect.* protos.
#
# Requires: protoc, protoc-gen-go, protoc-gen-go-grpc on PATH.
# protoc-gen-{go,go-grpc} install via:
#   GOPROXY=https://YOUR-INTERNAL-GO-PROXY go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
#   GOPROXY=https://YOUR-INTERNAL-GO-PROXY go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

set -euo pipefail

cd "$(dirname "$0")/.."

MODULE="github.com/liangchi-hsieh/spark-connect-gateway"
OUT_DIR="internal/genproto"
PROTO_DIR="proto"

# We rewrite the upstream go_package option to land under our module.
# Each .proto file in proto/spark/connect/ has `option go_package = "internal/generated"`.
# We override with --go_opt=Mfile=path so generated files go where we want.

GO_OPTS=""
GRPC_OPTS=""
for f in "$PROTO_DIR"/spark/connect/*.proto; do
    rel=${f#"$PROTO_DIR"/}
    GO_OPTS+=" --go_opt=M${rel}=${MODULE}/${OUT_DIR}/spark/connect"
    GRPC_OPTS+=" --go-grpc_opt=M${rel}=${MODULE}/${OUT_DIR}/spark/connect"
done

mkdir -p "$OUT_DIR"

# shellcheck disable=SC2086
protoc \
    -I "$PROTO_DIR" \
    --go_out="$OUT_DIR" \
    --go_opt=paths=source_relative \
    $GO_OPTS \
    --go-grpc_out="$OUT_DIR" \
    --go-grpc_opt=paths=source_relative \
    $GRPC_OPTS \
    "$PROTO_DIR"/spark/connect/*.proto

echo "Generated Go bindings under $OUT_DIR/"
