### Setup

1. Make sure you don't have anything running on ports 6433 and 6432.
2. (Optional) Delete all previous images to make sure to pull latest from Hub: `docker rm -vf $(docker ps -aq)` and `docker rmi -f $(docker images -aq)`
3. `docker-compose up`


### Run the benchmark

```bash
export PGHOST=127.0.0.1
export PGPASSWORD=postgres
export PGUSER=postgres
export PGDATABASE=postgres
```

### PgBouncer

```bash
export PGPORT=6432
pgbench -i
pgbench -c 10 -t 100000
```

### PgDog

```bash
export PGPORT=6433
pgbench -i
pgbench -c 10 -t 100000
```
