# Administration overview

pgDog keeps track of clients, servers and connection pools. It provides real time statistics on its internal operations for system
administrators to keep track of and integrate with monitoring tools like Datadog.

Just like pgbouncer, pgDog has a special "admin" database clients can connect to and run custom SQL commands
to get statistics.

## Admin database

The admin database name is [configurable](../configuration/pgdog.toml/admin.md). By default, the database is called `admin`. It supports a number of commands, documented below.

### Commands

| Command | Description |
|---------|-------------|
| [`SHOW CLIENTS`](clients.md) | Clients connected to pgDog with real time statistics. |
| [`SHOW SERVERS`](servers.md) | Server connections made by pgDog to PostgreSQL. |
| [`SHOW POOLS`](pools.md) | Connection pools used to multiplex clients and servers. |
| [`SHOW CONFIG`](config.md) | Currently loaded values from `pgdog.toml`. |
