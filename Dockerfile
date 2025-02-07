FROM rust:latest AS builder
#FROM --platform=linux/arm64 amazonlinux:2023 AS builder

RUN update-ca-certificates

#RUN dnf install rust cargo

WORKDIR /app

COPY ./ .
COPY rest-catalog-open-api.yaml rest-catalog-open-api.yaml

#RUN cargo build --release
RUN cargo build --release

####################################################################################################
## Final image
####################################################################################################

FROM debian:bookworm-slim
#FROM --platform=linux/arm64 amazonlinux:2023

# Copy the binary from the builder stage
WORKDIR /app

RUN apt-get update && apt-get install -y openssl && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/icehutd ./
COPY --from=builder /app/rest-catalog-open-api.yaml rest-catalog-open-api.yaml

CMD ["./icehutd"]
