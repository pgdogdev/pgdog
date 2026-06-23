ARG BUILDER_BASE=ghcr.io/pgdogdev/pgdog-base-builder:latest
ARG RUNTIME_BASE=ghcr.io/pgdogdev/pgdog-base-runtime:latest

FROM ${BUILDER_BASE} AS builder

COPY . /build
COPY .git /build/.git
WORKDIR /build

RUN rm /bin/sh && ln -s /bin/bash /bin/sh
RUN source ~/.cargo/env && \
    if [ "$(uname -m)" = "aarch64" ] || [ "$(uname -m)" = "arm64" ]; then \
        export RUSTFLAGS="-Ctarget-feature=+lse"; \
    fi && \
    cd pgdog && \
    cargo build --release && \
    cd .. && \
    cargo build --release -p pgdog-primary-only-tables

FROM ${RUNTIME_BASE}
ENV RUST_LOG=info

COPY --from=builder /build/target/release/pgdog /usr/local/bin/pgdog
COPY --from=builder /build/target/release/libpgdog_primary_only_tables.so /usr/lib/libpgdog_primary_only_tables.so

WORKDIR /pgdog
# PgDog drains gracefully on both SIGINT and SIGTERM, so the STOPSIGNAL value no
# longer affects shutdown behavior. Keep SIGINT to preserve the historical
# `docker stop` signal; SIGTERM (Kubernetes default, systemd, manual kill) is
# handled the same way regardless.
STOPSIGNAL SIGINT
CMD ["/usr/local/bin/pgdog"]
