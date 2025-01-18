# pgDog - PostgreSQL Load Balancer

[![Documentation](https://img.shields.io/badge/documentation-blue?style=flat)](https://pgdog.dev)
[![CI](https://github.com/levkk/pgdog/actions/workflows/ci.yml/badge.svg)](https://github.com/levkk/pgdog/actions/workflows/ci.yml)

pgDog is a PostgreSQL proxy and transaction pooler written in Rust.
Spiritual successor to [pgcat](https://github.com/levkk/pgcat), pgDog comes with a lot of
classic features like basic sharding, load balancing and failover. In addition, pgDog makes improvements to query performance, and adds new features like plugins, cross-shard queries, and async protocol support.

## Documentation

&#128216; pgDog documentation can be **[found here](https://pgdog.dev).**

## Features summary

| Feature | Status | Summary |
|---------|--------|---------|
| [Load balancer](https://pgdog.dev/features/load-balancer) | Operational | Spread query traffic across multiple databases automatically. |
| [Transaction pooling](https://pgdog.dev/features/transaction-mode) | Operational | Identical to pgbouncer, allows for thousands of clients to reuse a handful of server connections. |
| [Session pooling](https://pgdog.dev/features/session-mode) | Operational | Exclusive use of server connections for clients needing session-level features. |
| [Plugins](https://pgdog.dev/features/plugins/) | Operational | Control how pgDog routes queries and what results it sends to clients, through loading shared libraries at runtime. |
| [Sharding](https://pgdog.dev/features/sharding/) | Work in progress | Automatically split data and queries between multiple databases, scaling writes horizonally. |
| [Authentication](https://pgdog.dev/features/authentication/) | Supports `scram-sha-256` | Suppport for various PostgreSQL authentication mechanisms, like SCRAM, MD5, and LDAP. |
| [Configuration](https://pgdog.dev/configuration/) | Operational | Configure pgDog without restarting the pooler or breaking connections. |

## Getting started

Install the latest version of the Rust compiler from [rust-lang.org](https://rust-lang.org).
Once you have Rust installed, clone this repository and build the project in release mode:

```bash
cargo build --release
```

It's important to use the release profile if you're deploying to production or want to run
performance benchmarks.

### Configuration

pgDog has two configuration files:multi

* `pgdog.toml` which contains general settings and PostgreSQL servers information
* `users.toml` which contains users and passwords

Most options have reasonable defaults, so a basic configuration for a single user
and database deployment is easy to setup:

**`pgdog.toml`**

```toml
[general]
host = "0.0.0.0"
port = 6432

[[servers]]
name = "pgdog"
host = "127.0.0.1"
```

**`users.toml`**

```toml
[[users]]
database = "pgdog"
name = "pgdog"
password = "pgdog"
```

This configuration assumes the following:

* You have a PostgreSQL server running on `localhost`
* It has a database called `pgdog`
* You have created a user called `pgdog` with the password `pgdog`, and it can connect
  to the server.

If you'd like to try this out, you can set it up like so:

```postgresql
CREATE DATABASE pgdog;
CREATE USER pgdog PASSWORD 'pgdog' LOGIN;
```

### Running pgDog

Running pgDog can be done with Cargo:

```bash
cargo run --release --bin pgdog
```

Connecting to the pooler can be done with psql or any other PostgreSQL client:

```bash
psql postgres://pgdog:pgdog@127.0.0.1:6432/pgdog
```

Note that you're connecting to port `6432` where pgDog is running, not directly to Postgres.

## Features

### Load balancer

pgDog is an application layer (OSI Level 7) load balancer for PostgreSQL. It can proxy multiple replicas (and primary) and distribute transactions. It comes with support for multiple strategies, including round robin and random.

&#128216; **[Load balancer](https://pgdog.dev/features/load-balancer)**

#### Healthchecks and failover

pgDog maintains a real time list of healthy and unhealthy hosts in its database configuration.
When a host becomes unhealthy due to a healthcheck failure, it's removed from active rotation
and all query traffic is rerouted to other healthy databases. This is analogous to modern HTTP
load balancing, except it's at the database layer.

In the presence of multiple replicas, query re-routing maximizes database availability and
protects against intermittent issues like spotty network connectivity and other temporary hardware issues.

&#128216; **[Healthchecks](https://pgdog.dev/features/healthchecks)**

### Transaction pooling

Like other PostgreSQL poolers, pgDog supports transaction-level connection pooling, allowing
thousands (if not hundreds of thousands) of clients to re-use a handful of PostgreSQL server connections.

&#128216; **[Transactions](https://pgdog.dev/features/transaction-mode)**

### Plugins

pgDog comes with its own plugin system which allows them to be loaded at runtime using a shared library interface.
As long as the plugin can expose a predefined C API, it can be written in any language, including C/C++, Rust, Zig, Go, Python, Ruby, Java, and many more.

Plugins can be used to route queries to specific databases in a sharded configuration, or to
split traffic between writes and reads in a mixed (primary & replicas) deployment. The plugin
interface allows code execution at multiple stages of the request/response lifecycle, and can
go as far as block or intercept queries and return custom results to the client.

Examples of plugins can be found in [examples](https://github.com/levkk/pgdog/tree/main/examples) and [plugins](https://github.com/levkk/pgdog/tree/main/plugins).

&#128216; **[Plugins](https://pgdog.dev/features/plugins/)**

### Sharding

_This feature is a work in progress._

pgDog is able to handle deployments with multiple shards by routing queries automatically to one or more shards. The `pgdog-routing` plugin parses
queries, extracts tables and columns, and figures out which shard(s) the query should go to based on the parameters. Not all operations are supported, but
a lot of common use cases are covered.

&#128216; **[Sharding](https://pgdog.dev/features/sharding/)**

#### Local testing

The configuration files for a sharded database are provided in the repository. To make it work locally, setup the required databases like so:

```postgresql
CREATE DATABASE shard_0;
CREATE DATABASE shard_1;

GRANT CONNECT ON DATABASE shard_0 TO pgdog;
GRANT CONNECT ON DATABASE shard_1 TO pgdog;
```

Once the databases are created, you can launch pgDog with the sharded configuration:

```bash
cargo run -- --config pgdog-sharded.toml --users users-sharded.toml
```

### Configuration

pgDog is highly configurable and many aspects of its operation can be tweaked at runtime, without having
to restart the proxy or break PostgreSQL connections.

&#128216; **[Configuration](https://pgdog.dev/configuration/)**

## &#128678; Status &#128678;

While a lot of "classic" features of pgDog, like load balancing and healthchecks, have been well tested in production and at scale, the current codebase has not. This project is just getting started and early adopters are welcome to try pgDog internally.

Status on features stability will be [updated regularly](https://pgdog.dev/features/).

## Performance

pgDog does its best to minimize its impact on overall database performance. Using Rust and Tokio is a great start for a fast network proxy, but additional
care is also taken to perform as few operations as possible while moving data between client and server sockets. Some benchmarks are provided
to help set a baseline.

&#128216; **[Architecture & benchmarks](https://pgdog.dev/architecture/)**

## License

pgDog is free and open source software, licensed under the AGPL v3. While often misunderstood, this license is very permissive
and allows the following without any additional requirements from you or your organization:

* Internal use
* Private modifications for internal use without sharing any source code

You can freely use pgDog to power your PostgreSQL databases without having to
share any source code, including proprietary work product or any pgDog modifications you make.

AGPL was written specifically for organizations that offer pgDog _as a public service_ (e.g. database cloud providers) and require
those organizations to share any modifications they make to pgDog, including new features and bug fixes.

## Contributions

Contributions are welcome. If you see a bug, feel free to submit a PR with a fix or an issue to discuss. For any features,
please open an issue to discuss first.

The code has tests, make sure they pass first with:

```
cargo nextest run && \
cargo fmt --check --all
```

`cargo-nextest` is better because it runs tests in parallel and can help surface concurrency bugs.
