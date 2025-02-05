# Architecture overview

PgDog is written in the [Rust](https://rust-lang.org) programming language. It is also asynchronous, powered by the [Tokio](https://tokio.rs) runtime. This allows PgDog to serve hundreds of thousands of connections on one machine and to take advantage of multiple CPUs.

## Plugins

[Plugins](../features/plugins/index.md) are shared libraries (`.so` on Linux, `.dylib` on Mac, `.dll` on Windows) loaded at startup. This allows to
change many aspects of PgDog functionality without altering or recompiling internal source code.

## PostgreSQL protocol

PgDog speaks the PostgreSQL [frontend/backend](https://www.postgresql.org/docs/current/protocol.html) protocol. This allows it to act as an
application layer (OSI Level 7) proxy and multiplex client/server connections. It can also alter connection state
to suit operational needs, e.g. rolling back unfinished transactions, changing server settings, clearing session variables.


## Learn more

- [Features](../features/index.md)
- [Configuration](../configuration/index.md)
- [Benchmarks](benchmarks.md)
