# Multi-stage Docker build for moqui-device-gateway (Rust)
FROM rust:1.85-bookworm AS builder

WORKDIR /usr/src/moqui-device-gateway
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY tests ./tests

RUN cargo build --release

# Final runtime image
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /deployments

# Copy compiled binary
COPY --from=builder /usr/src/moqui-device-gateway/target/release/moqui-device-gateway /usr/local/bin/moqui-device-gateway

# Setup directories for logs, data, and configuration
RUN mkdir -p /deployments/logs /deployments/data /deployments/config \
    && useradd -u 1000 -m -s /bin/bash moqui \
    && chown -R moqui:moqui /deployments

USER moqui

ENV HTTP_HOST=0.0.0.0 \
    HTTP_PORT=8081 \
    GATEWAY_DEVICE_ID=GW_EDGE_01

EXPOSE 8081

HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
  CMD curl -f http://127.0.0.1:8081/q/health/ready || exit 1

ENTRYPOINT ["moqui-device-gateway"]
