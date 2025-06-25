# Multi-stage Dockerfile optimized for caching and minimal final image size
ARG RUST_VERSION=1.87.0

FROM rust:${RUST_VERSION} AS builder
WORKDIR /app

# Install required system dependencies
RUN apt-get update && apt-get install -y \
    cmake \
    pkg-config \
    libssl-dev \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy source code
COPY . .

# Build the application with optimizations
RUN cargo build --release --bin embucketd

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
