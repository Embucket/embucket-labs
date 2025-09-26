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

# Copy all source code, including the pre-built frontend and entrypoint script
COPY . .

RUN chmod +x entrypoint.sh
RUN tar -xf /app/ui/dist.tar -C ui

# Build the application with optimizations
RUN cargo build --release --bin embucketd

# Stage 2: Final runtime image
FROM gcr.io/distroless/cc-debian12 AS runtime

WORKDIR /app

# Copy the compiled binary, API spec, frontend build, and entrypoint script
COPY --from=builder /app/target/release/embucketd ./embucketd
COPY --from=builder /app/rest-catalog-open-api.yaml ./rest-catalog-open-api.yaml
COPY --from=builder /app/ui/dist ./dist
COPY --from=builder /app/entrypoint.sh /usr/local/bin/entrypoint.sh

# Make the script executable and ensure the nonroot user can modify app files
# no chown chroot executables
# RUN chown -R nonroot:nonroot /app

# Switch to a non-privileged user
USER nonroot:nonroot

# Expose port (adjust as needed)
EXPOSE 8080
EXPOSE 3000

ENV OBJECT_STORE_BACKEND=file
ENV FILE_STORAGE_PATH=data/
ENV BUCKET_HOST=0.0.0.0
ENV JWT_SECRET=63f4945d921d599f27ae4fdf5bada3f1

# Set the entrypoint to our script
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]

CMD ["./embucketd"]
