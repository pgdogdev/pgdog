# PgDog Documentation

PgDog is a proxy for scaling PostgreSQL. It supports connection pooling, load balancing queries, and sharding entire databases. Written in Rust, PgDog is fast, secure, and can manage thousands of connections on commodity hardware.

## Quick Start

- Full product docs: [docs.pgdog.dev](https://docs.pgdog.dev/)
- Local demo with Docker Compose: run `docker-compose up` from the repository root
- Default connection example: `PGPASSWORD=postgres psql -h 127.0.0.1 -p 6432 -U postgres`

## Core Features

- Connection pooling (session and transaction mode)
- Query load balancing across primary/replica hosts
- Database sharding with configurable routing
- Authentication support (SCRAM-SHA-256, MD5, plain, AWS RDS IAM)
- Failover-aware routing based on replication state

## Available Pages

- [Plugin System](PLUGIN_SYSTEM.md)