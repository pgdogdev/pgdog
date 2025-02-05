# Transaction mode

In transaction mode, PgDog is able to multiplex client transactions with several PostgreSQL backend servers. This
allows the pooler to serve thousands of clients using only dozens of actual server connections. This feature is essential for at-scale PostgreSQL deployments since Postgres is not able to maintain
more than a few thousand concurrently open connections.

<center>
  <img src="/images/transaction-mode.png" width="65%" alt="Load balancer" />
  <p><i>In transaction mode, multiple clients can reuse one Postgres connection.</i></p>
</center>


## Enable transaction mode

Transaction mode is **enabled** by default. This is controllable via configuration, at the global
and user level:

=== "pgdog.toml"
    ```toml
    [general]
    pooler_mode = "transaction"
    ```
=== "users.toml"
    ```toml
    [[users]]
    name = "alice"
    database = "prod"
    pooler_mode = "transaction"
    ```

## Session state

!!! note
    This feature is a work in progress.

Since clients in transaction mode reuse PostgreSQL server connections, it's possible for session-level variables and state to leak between clients. PgDog keeps track of connection state modifications and can automatically clean up server connections after a transaction. While this helps prevent session variables leakage between clients, this does have a small performance overhead.

To avoid this, clients using PgDog in transaction mode should avoid the usage of `SET` statements and use `SET LOCAL` inside an explicit transaction instead:

```postgresql
BEGIN;
SET LOCAL statement_timeout = '30s';
SELECT * FROM my_table;
COMMIT;
```
