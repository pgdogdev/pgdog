# Servers

`SHOW SERVERS` is a command to show real time statistics on PostgreSQL server connections created by [connection pools](pools.md). For example:

```
admin=> \x
Expanded display is on.

admin=> SHOW SERVERS;
-[ RECORD 1 ]-------+----------
host                | 127.0.0.1
port                | 5432
state               | idle
transactions        | 58
queries             | 58
rollbacks           | 0
prepared_statements | 0
healthchecks        | 58
errors              | 0
bytes_received      | 638
bytes_sent          | 406
age                 | 1719733
-[ RECORD 2 ]-------+----------
host                | 127.0.0.1
port                | 5432
state               | idle
transactions        | 58
queries             | 58
rollbacks           | 0
prepared_statements | 0
healthchecks        | 58
errors              | 0
bytes_received      | 638
bytes_sent          | 406
age                 | 1719734
```

## Statistics

| Name | Description |
|------|-------------|
| `host` | IP address or DNS name of the server. |
| `port` | TCP port of the server. |
| `state` | Server connection state, e.g. `active`, `idle in transaction`, etc. |
| `transactions` | Number of transactions completed by this server connection. |
| `queries` | Number of queries executed by this server connection. |
| `rollbacks` | Number of automatic rollbacks executed on this server connection by PgDog to clean up after idle transactions left by clients. |
| `prepared_statements` | Number of prepared statements created on this server connection. |
| `healthchecks` | Number of healthchecks executed on this server connection. |
| `errors` | Number of errors this connection has produced e.g. syntax errors. |
| `bytes_received` | Number of bytes received over the network. |
| `bytes_sent` | Number of bytes sent over the network. |
| `age` | How long ago this connection was created (in ms). |
