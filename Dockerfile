# Multi-stage Dockerfile optimized for caching and minimal final image size
ARG RUST_VERSION=latest

# Stage 1: Plan dependencies for optimal caching
FROM rust:${RUST_VERSION} AS planner
WORKDIR /app
RUN cargo install cargo-chef 
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# Stage 2: Build dependencies (cached separately from source code)
FROM rust:${RUST_VERSION} AS dependencies
WORKDIR /app
RUN cargo install cargo-chef
COPY --from=planner /app/recipe.json recipe.json
# Build dependencies - this is the caching layer
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo chef cook --release --recipe-path recipe.json

# Stage 3: Build the application
FROM rust:${RUST_VERSION} AS builder
WORKDIR /app

# Install required system dependencies
RUN apt-get update && apt-get install -y \
    cmake \
    pkg-config \
    libssl-dev \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy over the cached dependencies
COPY --from=dependencies /app/target target
COPY --from=dependencies /usr/local/cargo /usr/local/cargo

# Copy source code
COPY . .

# Build the application with optimizations
ENV CARGO_PROFILE_RELEASE_DEBUG=false
ENV CARGO_PROFILE_RELEASE_LTO=true
ENV CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --release --bin embucketd

# Stage 4: Final runtime image
FROM gcr.io/distroless/cc-debian12 AS runtime

# Set working directory
USER nonroot:nonroot
WORKDIR /app

# Copy the binary and required files
COPY --from=builder /app/target/release/embucketd ./embucketd
COPY --from=builder /app/rest-catalog-open-api.yaml ./rest-catalog-open-api.yaml

# Expose port (adjust as needed)
EXPOSE 8080
EXPOSE 3000

# Default command
CMD ["./embucketd"]
