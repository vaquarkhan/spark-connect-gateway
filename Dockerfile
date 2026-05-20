FROM rust:1.82-slim AS build
WORKDIR /src
RUN apt-get update && apt-get install -y --no-install-recommends \
    protobuf-compiler libprotobuf-dev pkg-config libssl-dev && \
    rm -rf /var/lib/apt/lists/*
COPY . .
RUN cargo build --release --bin gateway

FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=build /src/target/release/gateway /gateway
EXPOSE 15003
ENTRYPOINT ["/gateway"]
