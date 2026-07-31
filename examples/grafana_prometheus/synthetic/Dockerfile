FROM rust:1-slim AS builder
WORKDIR /app
# We copy like this to not invalidate cache on Dockerfile changes.
# You can get similar behavior with `COPY . .` paired with a .dockerignore
COPY Cargo.toml ./
COPY src ./src
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/synthetic /usr/local/bin/synthetic
CMD ["synthetic"]
