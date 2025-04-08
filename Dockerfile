FROM ubuntu:latest AS builder

RUN apt update && \
    apt install -y build-essential cmake clang curl pkg-config libssl-dev

# Install Rust.
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

COPY --from=builder /build/target/release/pgdog /usr/local/bin/pgdog

WORKDIR /pgdog
STOPSIGNAL SIGINT
CMD ["/usr/local/bin/pgdog"]
