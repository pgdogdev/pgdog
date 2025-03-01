FROM ubuntu:latest AS builder

RUN apt update && \
    apt install -y build-essential cmake clang curl

# Install Rust.
# We are not using rust:1 because
# bindgen is producing weird pointer types there.
RUN curl https://sh.rustup.rs -sSf | sh -s -- -y

COPY . /build
WORKDIR /build

RUN rm /bin/sh && ln -s /bin/bash /bin/sh
RUN source ~/.cargo/env && \
    cargo build --release

FROM ubuntu:latest
ENV RUST_LOG=info
RUN apt update && \
    apt install -y ca-certificates && \
    update-ca-certificates

COPY --from=builder /build/target/release/pgdog /pgdog/pgdog
COPY pgdog.toml /pgdog/pgdog.toml
COPY users.toml /pgdog/users.toml

WORKDIR /pgdog
STOPSIGNAL SIGINT
CMD ["/pgdog/pgdog"]
