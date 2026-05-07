.PHONY: all build test lint fmt vet clean proto-gen run

BINARY := bin/gateway
PKG := ./...

all: fmt vet build test

build:
	@mkdir -p bin
	go build -o $(BINARY) ./cmd/gateway

test:
	go test -race -count=1 $(PKG)

lint:
	go vet $(PKG)
	gofmt -l . | tee /dev/stderr | (! read)

fmt:
	gofmt -w .

vet:
	go vet $(PKG)

clean:
	rm -rf bin

run: build
	$(BINARY) --config config.yaml

# Regenerate Go bindings from vendored .proto files.
# Requires protoc + protoc-gen-go + protoc-gen-go-grpc on PATH.
proto-gen:
	./scripts/proto-gen.sh
