<p align="center">
    <img src="/.github/logo2-white.png" height="128" width="auto">
</p>

[![CI](https://github.com/levkk/pgdog/actions/workflows/ci.yml/badge.svg)](https://github.com/levkk/pgdog/actions/workflows/ci.yml)

PgDog is a transaction pooler and logical replication manager that can shard PostgreSQL. Written in Rust, PgDog is fast, secure and can manage hundreds of databases and hundreds of thousands of connections.

## Documentation

&#128216; PgDog documentation can be **[found here](https://docs.pgdog.dev/)**. Any questions? Join our **[Discord](https://discord.com/invite/CcBZkjSJdd)**.

## Quick start

### Kubernetes

Helm chart is **[here](https://github.com/pgdogdev/helm)**. To install it, run:

```bash
git clone https://github.com/pgdogdev/helm && \
cd helm && \
helm install -f values.yaml pgdog ./
```

### Docker

You can try PgDog quickly using Docker. Install [Docker Compose](https://docs.docker.com/compose/) and run:

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

### Monitoring

PgDog exposes both the standard "PgBouncer"-style admin database and an OpenMetrics endpoint. The admin database isn't 100% compatible with Pgb,
so we recommend you use OpenMetrics for monitoring. Example Datadog configuration and dashboard are [included](examples/datadog).

## Features


### Load balancer

PgDog is an application layer (OSI Level 7) load balancer for PostgreSQL. It can proxy multiple replicas (and primary) and distribute transactions evenly between databases. It supports multiple strategies, including round robin, random, least active connections, etc. PgDog can also inspect queries and send `SELECT` queries to replicas, and all others to the primary. This allows to proxy all databases behind a single PgDog deployment.

&#128216; **[Load balancer](https://docs.pgdog.dev/features/load-balancer)**

#### Healthchecks and failover

PgDog maintains a real-time list of healthy hosts. When a host fails a healthcheck, it's removed from active rotation and queries are rerouted to other databases. This is similar to HTTP load balancing, except it's at the database layer.

Failover maximizes database availability and protects against bad network connections, temporary hardware failures or misconfiguration.

&#128216; **[Healthchecks](https://docs.pgdog.dev/features/healthchecks)**

### Transaction pooling

Like PgBouncer, PgDog supports transaction (and session) pooling, allowing
100,000s of clients to use just a few PostgreSQL server connections.

&#128216; **[Transactions](https://docs.pgdog.dev/features/transaction-mode)**

### Sharding

PgDog is able to handle databases with multiple shards by routing queries automatically to one or more shards. Using the native PostgreSQL parser, PgDog understands queries, extracts sharding keys and determines the best routing strategy. For cross-shard queries, PgDog assembles results in memory and sends them all to the client transparently.

#### Using `COPY`

PgDog comes with a CSV parser and can split COPY commands between all shards automatically. This allows clients to ingest data into sharded PostgreSQL without preprocessing.

#### Logical replication

PgDog understands the PostgreSQL logical replication protocol and can split data between databases in the background and without downtime. This allows to shard existing databases and add more shards to existing clusters in production, without impacting database operations.

&#128216; **[Sharding](https://docs.pgdog.dev/features/sharding/)**

### Configuration

PgDog is highly configurable and many aspects of its operation can be tweaked at runtime, without having
to restart the process and break PostgreSQL connections. If you've used PgBouncer (or PgCat) before, the options
will be familiar. If not, they are documented with examples.

&#128216; **[Configuration](https://docs.pgdog.dev/configuration/)**
-
## Running PgDog locally

Install the latest version of the Rust compiler from [rust-lang.org](https://rust-lang.org).
Clone this repository and build the project in release mode:

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

#### Try sharding

The configuration files for a sharded database are provided in the repository. To make it work locally, create the required databases:

```postgresql
CREATE DATABASE shard_0;
CREATE DATABASE shard_1;

GRANT ALL ON DATABASE shard_0 TO pgdog;
GRANT ALL ON DATABASE shard_1 TO pgdog;
```

### Start PgDog

Running PgDog can be done with Cargo:

```bash
cargo run --release
```

You can connect to PgDog with psql or any other PostgreSQL client:

```bash
psql postgres://pgdog:pgdog@127.0.0.1:6432/pgdog
```

## &#128678; Status &#128678;

This project is just getting started and early adopters are welcome to try PgDog internally. Status on features stability will be [updated regularly](https://docs.pgdog.dev/features/). Most features have tests and are benchmarked regularly for performance regressions.

## Performance

PgDog does its best to minimize its impact on overall database performance. Using Rust and Tokio is a great start for a fast network proxy, but additional care is also taken to perform as few operations as possible while moving data between client and server sockets. Some benchmarks are provided to help set a baseline.

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

Please read our [Contribution Guidelines](CONTRIBUTING.md).
