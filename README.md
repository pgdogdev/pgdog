<p align="center">
    <picture>
      <source media="(prefers-color-scheme: dark)" srcset="/.github/logo2-white.png" height="128" width="auto">
      <source media="(prefers-color-scheme: light)" srcset="/.github/logo2_wide.png" height="128" width="auto">
      <img alt="Fallback image description" src="/.github/logo2-white.png" height="128" width="auto">
    </picture>
</p>

[![CI](https://github.com/levkk/pgdog/actions/workflows/ci.yml/badge.svg)](https://github.com/levkk/pgdog/actions/workflows/ci.yml)

PgDog is a transaction pooler and logical replication manager that can shard PostgreSQL. Written in Rust, PgDog is fast, secure and can manage hundreds of databases and hundreds of thousands of connections.

## Documentation

&#128216; PgDog documentation can be **[found here](https://docs.pgdog.dev/)**. Any questions? Join our **[Discord](https://discord.com/invite/CcBZkjSJdd)**.

## Quick start

### Kubernetes

Helm chart is **[here](https://github.com/pgdogdev/helm)**. To install it, run:

```bash
helm repo add pgdogdev https://helm.pgdog.dev
helm install pgdog pgdogdev/pgdog
```

### Try in Docker

You can try PgDog quickly using Docker. Install [Docker Compose](https://docs.docker.com/compose/) and run:

```
docker-compose up
```

It will take a few minutes to build PgDog from source and launch the containers. Once started, you can connect to PgDog with psql (or any other PostgreSQL client):

```
PGPASSWORD=postgres psql -h 127.0.0.1 -p 6432 -U postgres gssencmode=disable
```

The demo comes with 3 shards and 2 sharded tables:

```sql
INSERT INTO users (id, email) VALUES (1, 'admin@acme.com');
INSERT INTO payments (id, user_id, amount) VALUES (1, 1, 100.0);

SELECT * FROM users WHERE id = 1;
SELECT * FROM payments WHERE user_id = 1;
```

### Monitoring

PgDog exposes both the standard PgBouncer-style admin database and an OpenMetrics endpoint. The admin database isn't 100% compatible,
so we recommend you use OpenMetrics for monitoring. Example Datadog configuration and dashboard are [included](examples/datadog).

## Features

### Transaction pooling

Like PgBouncer, PgDog supports transaction (and session) pooling, allowing
100,000s of clients to use just a few PostgreSQL server connections.

&#128216; **[Transactions](https://docs.pgdog.dev/features/transaction-mode)**

### Load balancer

PgDog is an application layer (OSI Level 7) load balancer for PostgreSQL. It understands the Postgres protocol, can proxy multiple replicas (and primary) and distributes transactions evenly between databases. It supports multiple strategies, like round robin, random and least active connections.

&#128216; **[Load balancer](https://docs.pgdog.dev/features/load-balancer/)**

**Example**:

```toml
[[databases]]
name = "prod"
host = "10.0.0.1"
role = "primary"

[[databases]]
name = "prod"
host = "10.0.0.2"
role = "replica"
```

#### Healthchecks

PgDog maintains a real-time list of healthy hosts. When a database fails a healthcheck, it's removed from the active rotation and queries are re-routed to other replicas. This works like an HTTP load balancer, except it's for your database.

Healthchecks maximize database availability and protect against bad network connections, temporary hardware failures or misconfiguration.

&#128216; **[Healthchecks](https://docs.pgdog.dev/features/load-balancer/healthchecks/)**

#### Single endpoint

PgDog uses [`pg_query`](https://github.com/pganalyze/pg_query.rs), which bundles the PostgreSQL native parser. By parsing queries, PgDog can detect write queries (e.g. `INSERT`, `UPDATE`, `CREATE TABLE`, etc.) and send them to the primary, leaving the replicas to serve reads (`SELECT`). This allows applications to connect to one PgDog deployment for both reads and writes.

&#128216; **[Single endpoint](https://docs.pgdog.dev/features/load-balancer/#single-endpoint)**

##### Transactions

Transactions can execute multiple statements, so in a primary & replica configuration, PgDog routes them to the primary. Clients however can indicate a transaction is read-only, in which case PgDog will send it to a replica, for example:

```postgresql
BEGIN READ ONLY;
SELECT * FROM users LIMIT 1;
COMMIT;
```

&#128216; **[Load balancer + Transactions](https://docs.pgdog.dev/features/load-balancer/transactions/)**

#### Failover

PgDog keeps track of Postgres replication and can automatically redirect writes to a different database if a replica is promoted. This doesn't replace tools like Patroni which actually orchestrate failovers, but you can use PgDog alongside Patroni (or AWS RDS, or any other managed Postgres host), to gracefully failover live traffic.

&#128216; **[Failover](https://docs.pgdog.dev/features/load-balancer/replication-failover/)**

**Example**:

To enable failover, set all database `role` attributes to `auto`:

```toml
[general]
lsn_check_delay = 0

[[databases]]
name = "prod"
host = "10.0.0.1"
role = "auto"

[[databases]]
name = "prod"
host = "10.0.0.2"
role = "auto"
```

### Sharding

PgDog is able to handle databases with multiple shards by routing queries automatically to one or more databases. By using the PostgreSQL parser, PgDog understands queries, extracts sharding keys and determines the best routing strategy for each query.

For cross-shard queries, PgDog assembles and transforms results in memory, sending them all to the client as if they are coming from a single database.

**Example**

```toml
[[databases]]
name = "prod"
host = "10.0.0.1"
shard = 0

[[databases]]
name = "prod"
host = "10.0.0.2"
shard = 1
```

&#128216; **[Sharding](https://docs.pgdog.dev/features/sharding/)**


#### Sharding functions

PgDog has two main sharding algorithms:

1. Using PostgreSQL partition functions (`HASH`, `LIST`, `RANGE`)
2. Using schemas

##### Partition-based sharding

Partition-based sharding functions are taken directly from Postgres source code. This choice intentionally allows to shard data both with PgDog and with Postgres [foreign tables](https://www.postgresql.org/docs/current/sql-createforeigntable.html) and [`postgres_fdw`](https://www.postgresql.org/docs/current/postgres-fdw.html), which should help with migrating over to PgDog.

**Examples**

The `PARTITION BY HASH` algorithm is used by default:

```toml
[[sharded_tables]]
database = "prod"
column = "user_id"
```

List-based sharding (analogous to `PARTITION BY LIST`) can be configured as follows:

```toml
[[sharded_tables]]
database = "prod"
column = "user_id"

[[sharded_mapping]]
database = "prod"
column = "user_id"
values = [1, 2, 3, 4]
shard = 0

[[sharded_mapping]]
database = "prod"
column = "user_id"
values = [5, 6, 7, 8]
shard = 1
```

For range-based sharding, replace `values` with a range, e.g.:

```toml
start = 0
end = 5
```

&#128216; **[Sharding functions](https://docs.pgdog.dev/features/sharding/sharding-functions/)**

##### Schema-based sharding

Schema-based sharding works on the basis of PostgreSQL schemas. Tables under the same schema are placed on the same shard and all queries that refer to those tables are routed to that shard automatically.

**Example**

```toml
[[sharded_schemas]]
database = "prod"
name = "customer_a"
shard = 0

[[sharded_schemas]]
database = "prod"
name = "customer_b"
shard = 1
```

Queries that refer to `customer_a` schema will be sent to shard 0, for example:

```postgresql
INSERT INTO customer_a.orders (id, user_id, amount)
VALUES ($1, $2, $3);
```

Tables have to be fully qualified or the schema must be set in the `search_path` session variable:

```postgresql
SET search_path TO public, customer_a;
-- All subsequent queries will be sent to shard 0.
SELECT * FROM orders LIMIT 1;
```

You can set the `search_path` for the duration of a single transaction, using `SET LOCAL`, ensuring only that transaction is sent to the desired shard.

&#128216; **[Schema-based sharding](https://docs.pgdog.dev/configuration/pgdog.toml/sharded_schemas/)**

#### Direct-to-shard queries

Queries that contain a sharding key are sent to one database only. This is the best case scenario for sharded databases, since the load is uniformly distributed across the cluster.

**Example**:

```postgresql
-- user_id is the sharding key.
SELECT * FROM users WHERE user_id = $1;
```

&#128216; **[Direct-to-shard queries](https://docs.pgdog.dev/features/sharding/query-routing/)**

#### Cross-shard queries

Queries with multiple sharding keys or without one are sent to all databases and results are post-processed and assembled in memory. PgDog then sends the final result to the client.

&#128216; **[Cross-shard queries](https://docs.pgdog.dev/features/sharding/cross-shard-queries/)**

Currently, support for certain SQL features in cross-shard queries is limited. However, the list of supported ones keeps growing:

| Feature | Supported | Notes |
|-|-|-|
| Aggregates | Partial | `count`, `min`, `max`, `stddev`, `variance`, `sum`, `avg` are supported. |
| `ORDER BY` | Partial | Column in `ORDER BY` clause must be present in the result set. |
| `GROUP BY` | Partial | Same as `ORDER BY`, referenced columns must be present in result set. |
| Multi-tuple `INSERT` | Supported | |
| Sharding key `UPDATE` | Supported | |
| Subqueries | No | The same subquery is executed on all shards. |
| CTEs | No | The same CTE is executed on all shards. |

- &#128216; **[`SELECT`](https://docs.pgdog.dev/features/sharding/cross-shard-queries/select/)**
- &#128216; **[`INSERT`](https://docs.pgdog.dev/features/sharding/cross-shard-queries/insert/)**
- &#128216; **[`UPDATE` and `DELETE`](https://docs.pgdog.dev/features/sharding/cross-shard-queries/update/)**
- &#128216; **[DDL](https://docs.pgdog.dev/features/sharding/cross-shard-queries/ddl/)**

#### Using `COPY`

PgDog ships with a text, CSV & binary `COPY` parser and can split rows sent via `COPY` command between all shards automatically. This allows clients to ingest data into sharded PostgreSQL without preprocessing.

&#128216; **[Copy](https://docs.pgdog.dev/features/sharding/cross-shard-queries/copy/)**

#### Consistency (two-phase transactions)

To make sure cross-shard writes are atomic, PgDog supports Postgres' [two-phase transactions](https://www.postgresql.org/docs/current/two-phase.html). When enabled, PgDog will handle `COMMIT` statements sent by clients and execute the 2pc exchange on their behalf:

```postgresql
PREPARE TRANSACTION '__pgdog_unique_id';
COMMIT PREPARED '__pgdog_unique_id';
```

In case the client disconnects or Postgres crashes during the 2pc exchange, PgDog will automatically rollback the prepared transaction:

```postgresql
ROLLBACK PREPARED '__pgdog_unique_id';
```

&#128216; **[Two-phase commit](https://docs.pgdog.dev/features/sharding/2pc/)**

#### Unique identifiers

While applications can use `UUID` (v4 and now v7) to generate unique primary keys, PgDog supports generating unique `BIGINT` identifiers, without using a sequence:

```postgresql
SELECT pgdog.unique_id();
```

This uses the Snowflake-like timestamp-based algorithm and can produce millions of unique numbers per second.

&#128216; **[Unique IDs](https://docs.pgdog.dev/features/sharding/unique-ids/)**

#### Re-sharding

PgDog understands the PostgreSQL logical replication protocol and can split data between databases in the background and without downtime. This allows to shard existing databases and add more shards to existing clusters in production, without impacting database operations.

&#128216; **[Re-sharding](https://docs.pgdog.dev/features/sharding/resharding/)**

### Configuration

PgDog is highly configurable and most aspects of its operation can be tweaked at runtime, without having
to restart the process or break connections. If you've used PgBouncer (or PgCat) before, the options
will be familiar. If not, they are documented with examples.

&#128216; **[Configuration](https://docs.pgdog.dev/configuration/)**

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

Sharded database clusters are set in the config. For example, to set up a 2 shard cluster, you can:

**`pgdog.toml`**

```toml
[[databases]]
name = "pgdog_sharded"
host = "127.0.0.1"
database_name = "shard_0"
shard = 0

[[databases]]
name = "pgdog_sharded"
host = "127.0.0.1"
database_name = "shard_1"
shard = 1
```

Don't forget to specify a user:

**`users.toml`**

```toml
[[users]]
database = "pgdog_sharded"
name = "pgdog"
password = "pgdog"
```

And finally, to make it work locally, create the required databases:

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

#### Command-line options

PgDog supports several command-line options:

- `-c, --config <CONFIG>`: Path to the configuration file (default: `"pgdog.toml"`)
- `-u, --users <USERS>`: Path to the users.toml file (default: `"users.toml"`)
- `-d, --database_url <DATABASE_URL>`: Connection URL(s). Can be specified multiple times to add multiple database connections. When provided, these URLs override database configurations from the config file.

Example using database URLs directly:

```bash
cargo run --release -- -d postgres://user:pass@localhost:5432/db1 -d postgres://user:pass@localhost:5433/db2
```

You can connect to PgDog with `psql` or any other PostgreSQL client:

```bash
psql "postgres://pgdog:pgdog@127.0.0.1:6432/pgdog?gssencmode=disable"
```

## &#128678; Status &#128678;

PgDog is used in production and at scale. Most features are stable, while some are experimental. Check [documentation](https://docs.pgdog.dev/features/) for more details.

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
