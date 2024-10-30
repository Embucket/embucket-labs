FROM rust:latest AS builder

RUN update-ca-certificates

WORKDIR /app

COPY ./ .

RUN cargo build --release

####################################################################################################
## Final image
####################################################################################################
FROM debian:bookworm-slim

COPY --from=builder /app/target/release/nexus /usr/local/bin/nexus

WORKDIR /usr/local/bin

CMD ["nexus"]
