# Benchmarks

pgDog does its best to minimize its impact on database performance. Great care is taken to make sure as few operations are possible are performed
when passing data between clients and servers. All benchmarks listed below were done on my local system and should be taken with a grain of salt.
Real world performance is impacted by factors like network speed, query complexity and especially the hardware used by both pgDog and PostgreSQL servers.

## pgBench

The simplest way to test PostgreSQL performance is with `pgbench`. It comes standard with all PostgreSQL installations (Mac and Linux):

```bash
$ pgbench --version
pgbench (PostgreSQL) 16.4 (Postgres.app)
```

By default, pgBench will be executing `INSERT`, `UPDATE`, `SELECT` and `DELETE` queries to get an overall view of how
well the database is performing. Since we are only testing the performance of pgDog, we are going to be running `SELECT` queries only,
as to minimize the impact of hard disks (and IO) on this test.

This can be reproduced by passing the `-S` flag to `pgbench`. The tests below were performed using the configuration found in [`pgdog.toml`](https://github.com/levkk/pgdog/blob/main/pgdog.toml).

### Results

| Clients | Transactions | Throughput (/s) | Average latency |
|---------|--------------|------------|-----------------|
| 1 | 1,000 | 8633.93 | 0.116 ms |
| 1 | 10,000 | 13698.08| 0.073 ms |
| 1 | 100,000 | 12902.98 | 0.077 ms |
| 10 | 1,000 | 31397.46 | 0.307 ms |
| 10 | 10,000 | 35500.05 | 0.272 ms |
| 10 | 100,000 | 35861.21 | 0.269 ms |
| 100 | 1,000 | 2916.22 | 2.725 ms |
| 100 | 10,000 | 33181.99 | 2.718 ms |
| 100 | 100,000 | 32982.90 | 2.733 ms |


### Interpretation

#### One client

The first three (3) tests were performed with a single client connection (`-c 1` pgBench option). This test was meant to demonstrate
the best case scenario performance, with no resource contention. We increased the number of transactions in each test to average out outliers and to show that performance stays consistent (or improves) as more queries are executed.

#### Ten clients

The next three (3) tests were performed with 10 clients (`-c 10`) to demonstrate what happens when the connection pool is at full capacity. The result
of note is the average latency which increased from 0.073 ms to 0.272 ms. It's a bit hard to interpret this result as-is since it can be attributed
to PostgreSQL itself having to serve more concurrent transactions (and that's why all benchmarks are flawed).

In either case, this shows what is the expected performance when using pgDog on the same machine as the PostgreSQL server.

#### Hundred clients

The last three (3) tests were performed with 100 clients, which is 10 times more than there are server connections. This demonstrates what happens
to pgDog when clients are fighting for scarce resources and what impact that has on query throughput and latency. While latency of this test increased over the previous result, the overall throughput remained roughly the same.

This is a good indicator that transaction pooling is doing its job correctly
and pgDog can handle peak load gracefully.

##### In the real world

In production, it's expected that PostgreSQL clients will be idle most of the time. For example, web applications spend a lot of their time parsing HTTP requests, running code and waiting on network I/O. This leaves a lot of time for pgDog (and PostgreSQL) to serve queries and allows to share resources
between thousands of clients.

### Hardware impact

Benchmark results will vary widely with hardware. For example, these numbers will be greater on new Apple M chips and slower on older Intel CPUs. This benchmark used the Apple M1 chip. Expect yours to vary, but the overall trend to be directionally similar.
