# Administration overview

PgDog keeps track of clients, servers and connection pools. It provides real time statistics on its internal operations for system
administrators to keep track of and integrate with monitoring tools like Datadog.

Just like pgbouncer, PgDog has a special "admin" database clients can connect to and run custom SQL commands
to get statistics.

## Admin database

The admin database name is [configurable](../configuration/pgdog.toml/admin.md). By default, the database is called `admin`. It supports a number of commands, documented below.

### Commands

| Command | Description |
|---------|-------------|
| [`SHOW CLIENTS`](clients.md) | Clients connected to PgDog with real time statistics. |
| [`SHOW SERVERS`](servers.md) | Server connections made by PgDog to PostgreSQL. |
| [`SHOW POOLS`](pools.md) | Connection pools used to multiplex clients and servers. |
| [`SHOW CONFIG`](config.md) | Currently loaded values from `pgdog.toml`. |
| `SHOW PEERS` | List of PgDog processes running on the same network. Requires service discovery to be enabled. |
| `RELOAD` | Reload configuration from disk. See [pgdog.toml](../configuration/pgdog.toml/general.md) and [users.toml](../configuration/users.toml/users.md) for which options can be changed at runtime. |
| `RECONNECT` | Re-create all server connections using existing configuration. |
| `PAUSE` | Pause all pools. Clients will wait for connections until pools are resumed. Can be used for gracefully restarting PostgreSQL servers. |
| `RESUME` | Resume all pools. Clients are able to check out connections again. |

## Shutting down PgDog

When you need to shutdown PgDog, e.g. to deploy a new version, you can do so gracefully by issuing `SIGINT` (e.g. Ctrl-C) to the `pgdog` process.
PgDog will stop listening for new connections and give connected clients some time to finish their transactions and disconnect.

The amount of time PgDog will wait is [configurable](../configuration/pgdog.toml/general.md#shutdown_timeout). By default, PgDog will wait 60 seconds.

#### Example

```
$ pkill pgdog -SIGINT
```
