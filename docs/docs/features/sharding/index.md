# Sharding overview

!!! note
    This feature is under active development. It's not ready production use.

Sharding PostgreSQL databases involves splitting the database between multiple machines and routing queries to the right machines using a sharding function. Like its [predecessor](https://github.com/levkk/pgcat), PgDog supports sharded PostgreSQL deployments and can route queries to the correct shards automatically, implemented as a [plugin](../plugins/index.md).

<center style="margin-top: 2rem;">
    <img src="/images/sharding.png" width="70%" alt="Sharding" />
    <p><i>Sharded database routing.</i></p>
</center>

## Architecture

There are two ways for database clients to query sharded databases: by connecting to specific shard, or by querying all shards and aggregating the results. The former is commonly used in OLTP (transactional) systems, e.g. real time applications, and the latter is more commonly used in OLAP (analytical) databases, e.g. batch reports generation.

PgDog has good support for single shard queries, and adding support for aggregates over time[^1].

[^1]: Aggregation can get pretty complex and sometimes requires query rewriting. Examples can be found in the PostgreSQL's [postgres_fdw](https://www.postgresql.org/docs/current/postgres-fdw.html) extension.

### SQL parser

The [`pgdog-routing`](https://github.com/levkk/pgdog/tree/main/plugins/pgdog-routing) plugin parses queries using [`pg_query`](https://docs.rs/pg_query/latest/pg_query/) and can [calculate](automatic-routing.md) the shard based on a column value specified in the query. This allows applications to shard their databases without code modifications. For queries where this isn't possible, clients can specify the desired shard (or sharding key) in a [query comment](manual-routing.md).

### Multi-shard queries

When the sharding key isn't available or impossible to extract from a query, PgDog can route the query to all shards and return results combined in a [single response](cross-shard.md). Clients using this feature are not aware they are communicating with a sharded database and can treat PgDog connections like normal.

## Learn more

- [Multi-shard queries](cross-shard.md)
- [Manual routing](manual-routing.md)
