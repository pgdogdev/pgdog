FROM ubuntu:latest AS builder

RUN apt update && \
    apt install -y build-essential cmake clang curl pkg-config libssl-dev git mold

# Install Rust.
RUN curl https://sh.rustup.rs -sSf | sh -s -- -y

COPY . /build
COPY .git /build/.git
WORKDIR /build

RUN rm /bin/sh && ln -s /bin/bash /bin/sh
RUN source ~/.cargo/env && \
    if [ "$(uname -m)" = "aarch64" ] || [ "$(uname -m)" = "arm64" ]; then \
        export RUSTFLAGS="-Ctarget-feature=+lse"; \
    fi && \
    cd pgdog && \
    cargo build --release

FROM ubuntu:latest
ENV RUST_LOG=info
RUN apt update && \
    apt install -y curl ca-certificates ssl-cert && \
    update-ca-certificates

RUN install -d /usr/share/postgresql-common/pgdg && \
    curl -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc --fail https://www.postgresql.org/media/keys/ACCC4CF8.asc && \
    . /etc/os-release && \
    sh -c "echo 'deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] https://apt.postgresql.org/pub/repos/apt $VERSION_CODENAME-pgdg main' > /etc/apt/sources.list.d/pgdg.list"

RUN apt update && apt install -y postgresql-client-18
RUN apt remove -y curl

COPY --from=builder /build/target/release/pgdog /usr/local/bin/pgdog

WORKDIR /pgdog
STOPSIGNAL SIGINT
CMD ["/usr/local/bin/pgdog"]
