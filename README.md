# pgDog - PostgreSQL pooler and load balancer

[![Documentation](https://img.shields.io/badge/documentation-blue?style=flat)](https://pgdog.dev)
[![Latest crate](https://img.shields.io/crates/v/pgdog.svg)](https://crates.io/crates/pgdog)
[![Reference docs](https://img.shields.io/docsrs/pgdog)](https://docs.rs/rwf/latest/pgdog/)

pgDog is a PostgreSQL pooler, load balancer and sharding proxy, written in Rust.
Spiritual successor to [pgcat](https://github.com/levkk/pgcat), pgDog comes with a lot of
similar features, better performance, and introduces new features like plugins.

## Documentation

&#128216; pgDog documentation can be **[found here](https://pgdog.dev).**

## Features

### Plugins

pgDog comes with its own plugin system which allows plugins to be loaded at runtime using a shared library interface. As long as the plugin can expose a predefined C API, it can be written in any language, including C/C++, Rust, Java, or Go.

Plugins can be used to route queries to specific databases in a sharded configuration, or to
split traffic between writes and reads in a mixed (primary & replicas) deployment. The plugin
interface allows code execution at multiple stages of the request/response lifecycle, and can
go as far as block or intercept queries and return custom results to the client.

Examples of plugins can be found in [examples](https://github.com/levkk/pgdog/tree/main/examples) and [plugins](https://github.com/levkk/pgdog/tree/main/plugins).

&#128216; [Plugins documentation](https://pgdog.dev/features/plugins/).

### Load balancer

pgDog is an application layer (OSI Level 7) load balancer for PostgreSQL. It can proxy multiple replicas (and primary) and distribute query workloads. It comes with support for multiple strategies, including round robin and random.

&#128216; [Load balancer documentation](https://pgdog.dev/features/load-balancer).

### Healthchecks and query re-routing

pgDog maintains a real time list of healthy and unhealthy hosts in its database configuration.
When a host becomes unhealthy due to a healthcheck failure, it's removed from active rotation
and all query traffic is rerouted to other healthy databases. This is analogous to modern HTTP
load balancing, except it's at the database layer.

In the presence of multiple replicas, query re-routing maximize database availability and
protect against intermittent issues like spotty network connectivity and other temporary hardware issues.

&#128216; [Healthchecks documentation](https://pgdog.dev/features/healthchecks).

## Getting started

Install the latest version of the Rust compiler from [rust-lang.org](https://rust-lang.org).
Once you have Rust installed, clone this repository and build the project in release mode:

```bash
cargo build --release
```

It's important to use the release profile if you're deploying to production or want to run
performance benchmarks.

## Configuration

pgDog has two configuration files:

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

## Running pgDog

Running pgDog can be done with Cargo:

```bash
cargo run --release --bin pgdog
```

Connecting to the pooler can be done with psql or any other PostgreSQL client:

```bash
psql postgres://pgdog:pgdog@127.0.0.1:6432/pgdog
```

Note that you're connecting to port `6432` where pgDog is running, not directly Postgres.

## &#128678; Status &#128678;

While a lot of "classic" features of pgDog, like load balancing and healthchecks, have been well tested in production and at scale, the current code base has not. This project is just getting started and early adopters are welcome to try pgDog internally.

Status on features stability will be [updated regularly](https://pgdog.dev/features/).
