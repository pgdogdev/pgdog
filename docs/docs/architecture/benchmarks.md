# Benchmarks

PgDog does its best to minimize its impact on database performance. Great care is taken to make sure as few operations are possible are performed
when passing data between clients and servers. All benchmarks listed below were done on my local system and should be taken with a grain of salt.
Real world performance is impacted by factors like network speed, query complexity and especially by hardware used for running PgDog and PostgreSQL servers.

## pgbench

The simplest way to test PostgreSQL performance is with `pgbench`. It comes standard with all PostgreSQL installations (Mac and Linux):

```
$ pgbench --version
pgbench (PostgreSQL) 16.4 (Postgres.app)
```

A standard pgBench benchmark will run `INSERT`, `UPDATE`, `SELECT` and `DELETE` queries to get an overall view of database performance. Since we are only testing the performance of PgDog, we are going to run `SELECT` queries only and minimize the impact of hard disk I/O on this test.

This benchmark can be reproduced by passing the `-S` flag to `pgbench`. The results below were performed using the configuration found in [`pgdog.toml`](https://github.com/levkk/pgdog/blob/main/pgdog.toml).

### Results

Numbers below are for a single primary benchmark in transaction mode. No plugins are in use and PgDog is configured to use only 1 CPU core (`workers = 0`).

| Clients | Throughput (/s) | Latency |
|---------|--------------|------------|
| 1 | 17,865.08 | 0.056 ms |
| 10 | 70,770.09 | 0.136 ms |
| 100 | 54,649.23 | 1.686 ms |

#### With `pgdog-routing` enabled

These results are with `pgdog_routing` plugin enabled and parsing all queries with `pg_query.rs`. Parsing queries
has some noticeable overhead. Enabling multi-threading improved performance by over 50% in some cases.

| Clients | Throughput (/s) | Average latency | Workers |
|---------|-----------------|-----------------|---------|
| 1 | 12,902.98 | 0.077 ms | 0 |
| 10 | 35,861.21 | 0.269 ms | 0 |
| 100 | 32,982.90 | 2.733 ms | 0 |
| 1 | 14229.39 | 0.070 ms | 2 |
| 10 | 52379.48 | 0.136 ms | 2 |
| 100 | 57657.4 | 1.723 ms | 4 |


### Interpretation

#### 1 client

Benchmarks with `-c 1` (1 client) are a good baseline for what's possible under the best possible circumstances. There is no contention on resources
and PgDog effectively receives data in one socket and pushes it out the other.

#### 10 clients

With 10 clients actively querying the database, the connection pool is at full capacity. While there are no clients waiting for connections, the pool
has to serve clients without any slack in the system. This benchmark should produce the highest throughput numbers.

#### 100 clients

With over 10x more clients connected than available servers, connections are fighting for resources and PgDog has to make sure everyone gets served in a fair way. Consistent throughput in this benchmark demonstrates our ability to timeshare server connections effectively.

### In the real world

In production, PostgreSQL clients are expected to be mostly idle. For example, web applications spend a lot of their time parsing HTTP requests, running code and waiting on network I/O. This leaves plenty of time for PgDog (and PostgreSQL) to serve queries for thousands of clients.

#### Hardware impact

Benchmark results will vary widely with hardware. For example, these numbers will be better on new Apple M chips and slower on older Intel CPUs. This benchmark was ran on the Apple M1 chip. Expect yours to vary, but the overall trend to be directionally similar.

### pgbench configuration

```bash
export PGPASSWORD=pgdog
pgbench -P 1 -h 127.0.0.1 -p 6432 -U pgdog pgdog -c 10 -t 100000 -S
```
