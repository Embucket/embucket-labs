# Multi-stage Dockerfile optimized for caching and minimal final image size
FROM rust:bookworm AS builder

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

# Build the application without experimental features
RUN cargo build --release --bin embucketd --no-default-features

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

ENV OBJECT_STORE_BACKEND=file
ENV FILE_STORAGE_PATH=data/
ENV BUCKET_HOST=0.0.0.0
ENV JWT_SECRET=63f4945d921d599f27ae4fdf5bada3f1

# Default command
CMD ["./embucketd"]
