<p align="center">
    <img src="./.github/logo-white.png" height="128" width="auto">
</center>

[![CI](https://github.com/levkk/pgdog/actions/workflows/ci.yml/badge.svg)](https://github.com/levkk/pgdog/actions/workflows/ci.yml)

PgDog is a transaction pooler and logical replication manager than can shard PostgreSQL.
Spiritual successor to [pgcat](https://github.com/levkk/pgcat) and written in Rust, PgDog comes with a lot of
the same features, like load balancing, failover and connection pooling. In addition, PgDog improves performance and adds new features like cross-shard queries, async protocol support, and sharded `COPY`.

## Documentation

&#128216; PgDog documentation can be **[found here](https://docs.pgdog.dev/).**

## Quick start

You can try PgDog by using Docker. Make sure to install [Docker Compose](https://docs.docker.com/compose/) and run the following:

```
docker-compose up
```

It will take a few minutes to build PgDog from source and launch the containers. Once started, you can connect to PgDog with psql (or any other PostgreSQL client):

```
PGPASSWORD=postgres psql -h 127.0.0.1 -p 6432 -U postgres
```

The demo comes with 3 shards and 2 sharded tables:

```sql
INSERT INTO users (id, email) VALUES (1, 'admin@acme.com');
INSERT INTO payments (id, user_id, amount) VALUES (1, 1, 100.0);

SELECT * FROM users WHERE id = 1;
SELECT * FROM payments WHERE user_id = 1;
```


## Features summary

| Feature | Status | Summary |
|---------|--------|---------|
| [Load balancer](https://docs.pgdog.dev/features/load-balancer) | Operational | Spread `SELECT` queries across multiple replicas automatically, using algorithms like round robin. |
| [Transaction pooling](https://docs.pgdog.dev/features/transaction-mode) | Operational | Identical to pgbouncer, allows for thousands of clients to reuse a handful of server connections. |
| [Session pooling](https://docs.pgdog.dev/features/session-mode) | Operational | Exclusive use of server connections for clients needing session-level features. |
| [Plugins](https://docs.pgdog.dev/features/docs/plugins/) | Operational | Control how PgDog routes queries and what results it sends to clients, through loading shared libraries at runtime. |
| [Sharding](https://docs.pgdog.dev/features/sharding/) | Work in progress | Automatically split data and queries between multiple databases, scaling writes horizonally. |
| [Authentication](https://docs.pgdog.dev/features/authentication/) | Supports `scram-sha-256` and `trust` | Suppport for various PostgreSQL authentication mechanisms, like SCRAM, MD5, and LDAP. |
| [Configuration](https://docs.pgdog.dev/configuration/) | Operational | Configure PgDog without restarting the pooler or breaking connections. |

## Getting started

Install the latest version of the Rust compiler from [rust-lang.org](https://rust-lang.org).
Once you have Rust installed, clone this repository and build the project in release mode:

```bash
cargo build --release
```

It's important to use the release profile if you're deploying to production or want to run
performance benchmarks.

### Configuration

PgDog has two configuration files:

* `pgdog.toml` which contains general settings and PostgreSQL servers information
* `users.toml` for users and passwords

Most options have reasonable defaults, so a basic configuration for a single user
and database running on the same machine is pretty short:

**`pgdog.toml`**

```toml
[general]
host = "0.0.0.0"
port = 6432

[[databases]]
name = "pgdog"
host = "127.0.0.1"
```

**`users.toml`**

```toml
[[users]]
name = "pgdog"
password = "pgdog"
database = "pgdog"
```

If you'd like to try this out, you can set it up like so:

```postgresql
CREATE DATABASE pgdog;
CREATE USER pgdog PASSWORD 'pgdog' LOGIN;
```

### Running PgDog

Running PgDog can be done with Cargo:

```bash
cargo run --release
```

You can connect to PgDog with psql or any other PostgreSQL client:

```bash
psql postgres://pgdog:pgdog@127.0.0.1:6432/pgdog
```

## Features

### Load balancer

PgDog is an application layer (OSI Level 7) load balancer for PostgreSQL. It can proxy multiple replicas (and primary) and distribute transactions. It comes with support for multiple strategies, including round robin and random. Additionally, it can parse queries and send `SELECT` queries to replicas and all others to the primary. This allows to proxy all databases behind a single PgDog deployment.

&#128216; **[Load balancer](https://docs.pgdog.dev/features/load-balancer)**

#### Healthchecks and failover

PgDog maintains a real time list of healthy hosts in its database configuration.
When a host fails a healthcheck, it's removed from active rotation
and queries are rerouted to other replicas. This is analogous to modern HTTP
load balancing, except it's at the database layer.

Failover maximizes database availability and protects against intermittent issues like spotty network connectivity and temporary downtime.

&#128216; **[Healthchecks](https://docs.pgdog.dev/features/healthchecks)**

### Transaction pooling

Like pgbouncer, PgDog supports transaction-level connection pooling, allowing
1000s (even 100,000s) of clients to reuse just a few PostgreSQL server connections.

&#128216; **[Transactions](https://docs.pgdog.dev/features/transaction-mode)**

### Plugins

PgDog comes with its own plugin system that loads them at runtime using a shared library interface.
If a plugin can expose a predefined C API, it can be written in any language, including C/C++, Rust, Zig, Go, Python, Ruby, Java, and many more.

Plugins can be used to route queries to specific databases in a sharded configuration, or to
split traffic between writes and reads in a mixed (primary & replicas) deployment. The plugin
interface allows code execution at multiple stages of the request/response lifecycle, and can
go as far as block or intercept queries and return custom results to the client.

Examples of plugins can be found in [examples](https://github.com/levkk/pgdog/tree/main/examples) and [plugins](https://github.com/levkk/pgdog/tree/main/plugins).

&#128216; **[Plugins](https://docs.pgdog.dev/features/plugins/)**

### Sharding

_This feature is a work in progress._

PgDog is able to handle databases with multiple shards by routing queries automatically to one or more shards. The `pgdog-routing` plugin parses
queries, extracts tables and columns information, and calculates which shard(s) the query should go to based on the parameters. Not all operations are supported, but
a lot of common use cases are working.

&#128216; **[Sharding](https://docs.pgdog.dev/features/sharding/)**

#### Local testing

The configuration files for a sharded database are provided in the repository. To make it work locally, create the required databases:

```postgresql
CREATE DATABASE shard_0;
CREATE DATABASE shard_1;

GRANT CONNECT ON DATABASE shard_0 TO pgdog;
GRANT CONNECT ON DATABASE shard_1 TO pgdog;
```

You can launch PgDog with the sharded configuration using the files provided in the repository:

```bash
cargo run -- --config pgdog-sharded.toml --users users-sharded.toml
```

### Configuration

PgDog is highly configurable and many aspects of its operation can be tweaked at runtime, without having
to restart the process and break PostgreSQL connections. If you've used pgbouncer (or pgcat) before, the options
will be familiar. If not, options are documented with examples.

&#128216; **[Configuration](https://docs.pgdog.dev/configuration/)**

## &#128678; Status &#128678;

While a lot of "classic" features of PgDog, like load balancing and healthchecks, have been well tested in production and at scale, the current codebase has not. This project is just getting started and early adopters are welcome to try PgDog internally.

Status on features stability will be [updated regularly](https://docs.pgdog.dev/features/).

## Performance

PgDog does its best to minimize its impact on overall database performance. Using Rust and Tokio is a great start for a fast network proxy, but additional
care is also taken to perform as few operations as possible while moving data between client and server sockets. Some benchmarks are provided
to help set a baseline.

&#128216; **[Architecture & benchmarks](https://docs.pgdog.dev/architecture/)**

## License

PgDog is free and open source software, licensed under the AGPL v3. While often misunderstood, this license is very permissive
and allows the following without any additional requirements from you or your organization:

* Internal use
* Private modifications for internal use without sharing any source code

You can freely use PgDog to power your PostgreSQL databases without having to
share any source code, including proprietary work product or any PgDog modifications you make.

AGPL was written specifically for organizations that offer PgDog _as a public service_ (e.g. database cloud providers) and require
those organizations to share any modifications they make to PgDog, including new features and bug fixes.

## Contributions

Contributions are welcome. If you see a bug, feel free to submit a PR with a fix or an issue to discuss. For any features,
please open an issue to discuss first.

The code has tests, make sure they pass first with:

```
cargo nextest run && \
cargo fmt --check --all && \
cargo clippy
```

`cargo-nextest` is better because it runs tests in parallel and can help surface concurrency bugs.
