# Logical replication sharding

Shard Postgres using logical replication.

## Setup

I'm using Postgres.app and have created databases on ports:

- 5432
- 5433
- 5434

On all 3 databases:

```sql
CREATE DATABASE pgdog;
CREATE USER pgdog SUPERUSER PASSWORD 'pgdog' REPLICATION;
\c pgdog
CREATE SCHEMA pgdog;
CREATE TABLE pgdog.books (
    id BIGINT PRIMARY KEY,
    title VARCHAR,
    content VARCHAR
);
```

On the primary (5432):

```sql
CREATE PUBLICATION books FOR TABLE pgdog.books;
```

## Run

```
cargo run -- --from-database source --from-user pgdog --to-database destination --to-user pgdog --publication books
```
