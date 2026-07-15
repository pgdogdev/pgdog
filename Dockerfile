ARG BUILDER_BASE=ghcr.io/pgdogdev/pgdog-base-builder:latest
ARG RUNTIME_BASE=ghcr.io/pgdogdev/pgdog-base-runtime:latest

FROM ${BUILDER_BASE} AS builder
ARG FEATURES=""

COPY . /build
COPY .git /build/.git
WORKDIR /build

RUN rm /bin/sh && ln -s /bin/bash /bin/sh
RUN source ~/.cargo/env && \
    if [ "$(uname -m)" = "aarch64" ] || [ "$(uname -m)" = "arm64" ]; then \
        export RUSTFLAGS="-Ctarget-feature=+lse"; \
    fi && \
    cargo_features=(); \
    if [ -n "${FEATURES}" ]; then \
        cargo_features=(--no-default-features --features "${FEATURES}"); \
    fi && \
    cd pgdog && \
    cargo build --release "${cargo_features[@]}" && \
    cd .. && \
    cargo build --release -p pgdog-primary-only-tables "${cargo_features[@]}"

FROM ${RUNTIME_BASE}
ENV RUST_LOG=info

COPY --from=builder /build/target/release/pgdog /usr/local/bin/pgdog
COPY --from=builder /build/target/release/libpgdog_primary_only_tables.so /usr/lib/libpgdog_primary_only_tables.so

WORKDIR /pgdog
STOPSIGNAL SIGINT
CMD ["/usr/local/bin/pgdog"]
