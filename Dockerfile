FROM rust:1-bookworm AS builder
COPY . /build
WORKDIR /build
RUN apt update && \
    apt install -y build-essential cmake clang
RUN cargo build --release

FROM debian:bookworm
ENV RUST_LOG=info
RUN apt update && \
    apt install -y ca-certificates && \
    update-ca-certificates

COPY --from=builder /build/target/release/pgdog /pgdog/pgdog
COPY pgdog.toml /pgdog/pgdog.toml
COPY users.toml /pgdog/users.toml

WORKDIR /pgdog
CMD ["/pgdog/pgdog"]
