FROM rust:latest AS builder

RUN update-ca-certificates

WORKDIR /app

COPY ./ .
#COPY .env .env
COPY .env.example .env
COPY rest-catalog-open-api.yaml rest-catalog-open-api.yaml

RUN cargo build --release

####################################################################################################
## Final image
####################################################################################################

FROM debian:bookworm-slim

# Copy the binary from the builder stage
WORKDIR /app

RUN apt-get update && apt-get install -y openssl && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/nexus ./
COPY --from=builder /app/.env.example .env
#COPY --from=builder /app/.env .env
COPY --from=builder /app/rest-catalog-open-api.yaml rest-catalog-open-api.yaml

CMD ["./nexus"]
