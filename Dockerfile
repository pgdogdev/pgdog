ARG BUILDER_BASE=ghcr.io/pgdogdev/pgdog-base-builder:latest
ARG RUNTIME_BASE=ghcr.io/pgdogdev/pgdog-base-runtime:latest

FROM ${BUILDER_BASE} AS builder
ARG FEATURES=""

COPY . /build
COPY .git /build/.git
WORKDIR /build

RUN rm /bin/sh && ln -s /bin/bash /bin/sh
# The AWS-LC FIPS module (FEATURES=fips) needs Go to build.
RUN if [[ " ${FEATURES//,/ } " == *" fips "* ]]; then \
        apt-get update && \
        apt-get install -y --no-install-recommends golang-go && \
        rm -rf /var/lib/apt/lists/*; \
    fi
# FEATURES are pgdog's; the plugin has none of them.
RUN source ~/.cargo/env && \
    cargo_features=(); \
    if [ -n "${FEATURES}" ]; then \
        cargo_features=(--no-default-features --features "${FEATURES}"); \
    fi && \
    pgo_args=(); \
    if [ -f pgo.profdata ]; then \
        pgo_args=(--config "target.'cfg(all())'.rustflags=['-Cprofile-use=/build/pgo.profdata']"); \
    fi && \
    cd pgdog && \
    cargo build --release "${cargo_features[@]}" "${pgo_args[@]}" && \
    cd .. && \
    cargo build --release -p pgdog-primary-only-tables

FROM ${RUNTIME_BASE}
ENV RUST_LOG=info

COPY --from=builder /build/target/release/pgdog /usr/local/bin/pgdog
COPY --from=builder /build/target/release/libpgdog_primary_only_tables.so /usr/lib/libpgdog_primary_only_tables.so

WORKDIR /pgdog
STOPSIGNAL SIGINT
CMD ["/usr/local/bin/pgdog"]
