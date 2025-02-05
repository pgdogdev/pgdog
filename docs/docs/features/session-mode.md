# Session mode

In session mode, PgDog allocates one PostgreSQL server connection per client. This ensures that all PostgreSQL features work as expected, including persistent session variables, settings, and
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
    In session mode, when the connection pool reaches full capacity, a client has to disconnect before another one can connect to PgDog.


### Benefits of session mode

Using PgDog in session mode is still an improvement over connecting to PostgreSQL directly. Since the proxy maintains a pool of open server connections,
when a client disconnects, the PostgreSQL server connection remains intact and can be reused by another client.

#### Lazy connections
Until a client issues their first query, PgDog doesn't attach it to a server connection. This allows one set of clients to connect before the previous set disconnects,
which is common when using zero-downtime deployment strategies like blue/green[^1].

[^1]: [https://docs.aws.amazon.com/whitepapers/latest/overview-deployment-options/bluegreen-deployments.html](https://docs.aws.amazon.com/whitepapers/latest/overview-deployment-options/bluegreen-deployments.html)
