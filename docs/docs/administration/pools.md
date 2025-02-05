# Pools

`SHOW POOLS` is a command to show real time statistics on connection pools used to [multiplex](../features/transaction-mode.md) PgDog clients and PostgreSQL servers. For example:

```
admin=> \x
Expanded display is on.

admin=> SHOW POOLS;
-[ RECORD 1 ]---+--------------
host            | 127.0.0.1
port            | 5432
database        | pgdog
user            | pgdog
idle            | 1
active          | 0
total           | 1
clients_waiting | 0
paused          | f
banned          | f
errors          | 0
out_of_sync     | 0
-[ RECORD 2 ]---+--------------
host            | 127.0.0.1
port            | 5432
database        | pgdog
user            | pgdog_session
idle            | 1
active          | 0
total           | 1
clients_waiting | 0
paused          | f
banned          | f
errors          | 0
out_of_sync     | 0
```

## Statistics

| Name | Description |
|------|-------------|
| `host` | IP address or DNS name of the PostgreSQL server. |
| `port` | TCP port of the PostgreSQL server. |
| `database` | Name of the PostgreSQL database. |
| `user` | User used to connect to the database. |
| `idle` | Number of idle server connections in the pool. |
| `active` | Number of checked out (used) server connections in the pool. |
| `total` | Total number of server connections in the pool. |
| `clients_waiting` | Number of clients waiting for a connection from this pool. |
| `paused` | The pool is paused and won't issue connections until resumed. |
| `banned` | The pool is blocked from serving more clients. |
| `errors` | Number of connections returned to the pool in a bad state, e.g. network connectivity broken. |
| `out_of_sync` | Number of connections returned to the pool by clients that left it in a bad state, e.g. by issuing a query and not waiting for the result. |
