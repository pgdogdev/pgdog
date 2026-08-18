# Performance optimizations

1. Use `samply`, it's a great profiler.
2. Any code outside of `read`/`write` syscalls (reading/writing to/from sockets) is overhead to be minimized
3. Memory allocations are expensive at scale
4. Copying memory is expensive at scale

## Standard benchmark

We have three benchmarks:

1. [Connection pooler](pooler): should be the fastest, uses least of the code
2. [Load balancer](lb): a bit slower, does more stuff
3. [Sharding](sharding): the most complex, slowest

Use the `run.sh` script in each benchmark folder to quickly run the test. Use [`../../integration/setup.sql`](../../integration/setup.sql) to configure your local with the necessary databases and users.
