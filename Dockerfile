ARG BUILDER_BASE=ghcr.io/pgdogdev/pgdog-base-builder:latest
ARG RUNTIME_BASE=ghcr.io/pgdogdev/pgdog-base-runtime:latest

FROM ${BUILDER_BASE} AS builder
ARG FEATURES=""
ARG TARGETARCH

COPY . /build
COPY .git /build/.git
WORKDIR /build

RUN rm /bin/sh && ln -s /bin/bash /bin/sh
# ARM64 Linux artifacts must support kernels with up to 64K pages.
RUN source ~/.cargo/env && \
    if [ "${TARGETARCH}" = "arm64" ]; then \
        export JEMALLOC_SYS_WITH_LG_PAGE=16; \
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
