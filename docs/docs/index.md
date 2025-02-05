# PgDog

[PgDog](https://github.com/levkk/pgdog) is a PostgreSQL query router, pooler, proxy and load balancer written in Rust. Spiritual successor to
[pgcat](https://github.com/levkk/pgcat), PgDog comes with a lot of similar features, better performance,
and introduces new features like plugins and cross-shard queries.

PostgreSQL deployments of any size can be proxied by PgDog, ranging from a single database to hundreds of primaries and replicas in a sharded configuration.

## Installation

PgDog is easily compiled from source. Before proceeding, make sure you have the latest version of the Rust
compiler, available from [rust-lang.org](https://rust-lang.org).

### Checkout the code

PgDog source code can be downloaded from [GitHub](https://github.com/levkk/pgdog):

```bash
git clone https://github.com/levkk/pgdog && \
cd pgdog
```

### Compile PgDog

PgDog should be compiled in release mode to make sure you get all performance benefits. You can do this with Cargo:

```bash
cargo build --release
```

### Configuration

PgDog is [configured](configuration/index.md) via two files:

* [`pgdog.toml`](configuration/index.md) which contains general pooler settings and PostgreSQL server information
* [`users.toml`](configuration/users.toml/users.md) which contains passwords for users allowed to connect to the pooler

The passwords are stored in a separate file to simplify deployments in environments where
secrets can be safely encrypted, like Kubernetes or AWS EC2.

Both files can to be placed in the current working directory (CWD) for PgDog to detect them. Alternatively,
you can pass the `--config` and `--secrets` arguments with their locations when starting PgDog.

#### Example `pgdog.toml`

Most PgDog configuration options have sensible defaults. This allows a basic primary-only configuration to be pretty short:

```toml
[general]
host = "0.0.0.0"
port = 6432

[[databases]]
name = "postgres"
host = "127.0.0.1"
```

#### Example `users.toml`

This configuration file contains a mapping between databases, users and passwords. Users not specified in this file
won't be able to connect to PgDog:

```toml
[[users]]
name = "alice"
database = "postgres"
password = "hunter2"
```

### Launch the pooler

Starting the pooler can be done by running the binary in `target/release` folder or with Cargo:


=== "Command"
    ```bash
    cargo run --release
    ```

=== "Output"
    ```
    INFO 🐕 PgDog 0.1.0
    INFO loaded pgdog.toml
    INFO loaded users.toml
    INFO loaded "pgdog_routing" plugin [1.0461ms]
    INFO 🐕 PgDog listening on 0.0.0.0:6432
    INFO new server connection [127.0.0.1:5432]
    ```

## Next steps

* [Features](features/index.md)
* [Configuration](configuration/index.md)
* [Architecture](architecture/index.md)
