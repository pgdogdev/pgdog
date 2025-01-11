# Session mode

In session mode, pgDog allocates one PostgreSQL server connection per client. This ensures that all PostgreSQL features work as expected, including persistent session variables, settings, and
process-based features like `LISTEN`/`NOTIFY`. Some batch-based tasks, like ingesting large amounts of data, perform better in session mode.

## Enable session mode

Session mode can be enabled globally or on a per-user basis:

=== "pgdog.toml"
    ```toml
    [general]
    pooler_mode = "session"
    ```
=== "users.toml"
    ```toml
    [[users]]
    name = "pgdog"
    database = "pgdog"
    pooler_mode = "session"
    ```

## Performance

Unlike [transaction mode](transaction-mode.md), session mode doesn't allow for client/server connection multiplexing, so the maximum number of allowed client connections
is controlled by the `default_pool_size` (and `pool_size`) settings. For example, if your database pool size is 15,
only 15 clients will be able to connect and use the database at any given moment.

!!! note
    In session mode, when the connection pool reaches full capacity, a client has to disconnect before another one can connect to pgDog.
