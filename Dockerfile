# Multi-stage Dockerfile: build with Rust toolchain, run in minimal runtime image.

# Stage 1: Builder
FROM rust:1-bookworm AS builder

WORKDIR /build

# Copy manifests and lock file to leverage Docker layer caching.
COPY Cargo.toml Cargo.lock ./

# Create a dummy main.rs to fetch and compile dependencies.
RUN mkdir -p src && echo "fn main() {}" > src/main.rs && \
    cargo build --release && \
    rm -rf src

# Copy the actual source and rebuild.
COPY src ./src
# Touch main.rs so cargo rebuilds over the cached dummy binary.
RUN touch src/main.rs && cargo build --release

# Stage 2: Runtime
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

# Create non-root user.
RUN useradd -m -u 1000 trader

WORKDIR /app

# Copy the binary from builder.
COPY --from=builder /build/target/release/uge /usr/local/bin/uge

# Create state, keys, and logs directories as volumes with correct ownership.
RUN mkdir -p /app/state /app/keys /app/logs && \
    chown -R trader:trader /app

# Switch to non-root user.
USER trader

# Volume mounts for state, keys, and logs.
VOLUME ["/app/state", "/app/keys", "/app/logs"]

# Environment variables from paths.rs.
# These defaults point to the mounted volumes; override in your docker run/compose.
ENV UGE_ROOT=/app/state \
    UGE_KEY_DIR=/app/keys \
    UGE_LOG_DIR=/app/logs

# Run the engine by default (no arguments = engine loop).
# Use --dry-run for testing without placing orders.
ENTRYPOINT ["uge"]
