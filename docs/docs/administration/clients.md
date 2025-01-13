# Clients

`SHOW CLIENTS` is a command to show currently connected clients and their real time statistics like number of queries/transactions executed, network activity, and state. For example:

```
admin=> \x
Expanded display is on.

admin=> SHOW CLIENTS;
-[ RECORD 1 ]----+----------
host             | 127.0.0.1
port             | 60798
state            | active
queries          | 2
transactions     | 2
wait_time        | 0.00000
query_time       | 0.09521
transaction_time | 0.09624
bytes_received   | 57
bytes_sent       | 965
errors           | 0
```

## Statistics

| Name | Description |
|------|-------------|
| `host` | IP address of the client. |
| `port` | TCP port client is connected from. |
| `state` | Real time client state, e.g. `active`, `idle`, etc. |
| `queries` | Number of queries executed. |
| `transactions` | Number of completed transactions executed. |
| `wait_time` | How long the client had to wait to get a connection from the pool. This value increases monotonically if the client is waiting for a pool that's too busy to serve transactions. |
| `query_time` | Total time this client's queries took to run on a server. |
| `transaction_time` | Total time this client's transactions took to execute on the server, including idle in transaction time. |
| `bytes_sent` | Number of bytes sent over the network to the client. |
| `bytes_received` | Number of bytes received over the network from the client. |
| `errors` | Number of errors the client has received, e.g. query syntax errors. |
