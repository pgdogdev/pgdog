# pgDog

[pgDog](https://github.com/levkk/pgdog) is a PostgreSQL query router, pooler, proxy and load balancer written in Rust. Spiritual successor to
[pgcat](https://github.com/levkk/pgcat), pgDog comes with a lot of similar features, better performance,
and introduces new features like plugins.

PostgreSQL deployments of any size can be proxied by pgDog, ranging from a single database to hundreds of primaries and replicas in a sharded configuration.

## Getting started

pgDog is easily compiled from source. Before proceeding, make sure you have the latest version of the Rust
compiler, available from [rust-lang.org](https://rust-lang.org).

### Checkout the code

pgDog source code can be downloaded from [GitHub](https://github.com/levkk/pgdog):

```bash
git clone https://github.com/levkk/pgdog && \
cd pgdog
```

### Compile pgDog

pgDog should be compiled in release mode to make sure you get all performance benefits. You can do this with Cargo:

```bash
cargo build --release
```

### Configuration

pgDog is configured via two configuration files:

* `pgdog.toml` which contains general pooler settings and PostgreSQL server information
* `users.toml` which contains passwords for users allowed to connect to the pooler

The passwords are stored in a separate file to simplify deployments in environments where
secrets can be safely encrypted, like Kubernetes or AWS EC2.

Both files need to be placed in the current working directory (CWD) for pgDog to detect them. Alternatively,
you can pass the `--config` and `--secrets` arguments with their locations when starting the pooler.

#### Example `pgdog.toml`

Most pgDog configuration options have sensible defaults. This allows a basic primary-only configuration to be pretty short.

```toml
[general]
host = "0.0.0.0"
port = 6432
default_pool_size = 10
pooler_mode = "transaction"

[[databases]]
name = "production"
role = "primary"
host = "127.0.0.1"
port = 5432
database_name = "postgres"
```

#### Example `users.toml`

This configuration file contains a mapping between databases, users and passwords. Users not specified in this file
won't be able to connect to the pooler.

```toml
[[users]]
name = "alice"
database = "production"
password = "hunter2"
```

### Launch the pooler

Starting the pooler can be done by executing the binary or with Cargo:


=== "Command"
    ```bash
    cargo run --release --bin pgdog
    ```

=== "Output"

    ```
    🐕 pgDog 0.1.0
    Loaded pgdog.toml
    Loaded "pgdog_routing" plugin
    Listening on 0.0.0.0:6432
    New server connection [127.0.0.1:5432]
    ```

## Next steps

* [Features](features/index.md)
* [Architecture](architecture/index.md)
* [Configuration](configuration/index.md)
