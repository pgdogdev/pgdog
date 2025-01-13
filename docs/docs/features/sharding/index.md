# Sharding overview

!!! note
    This feature is under active development. It's not ready for testing or production use.

Sharding PostgreSQL databases involves splitting the database between multiple machines and routing read
and write queries to the correct machines using a sharding function. Like its [predecessor](https://github.com/levkk/pgcat), pgDog plans to support sharded PostgreSQL deployments and will route queries to the correct shards automatically using a routing [plugin](../plugins/index.md).

<center style="margin-top: 2rem;">
    <img src="/images/sharding.png" width="70%" alt="Sharding" />
    <p><i>3 primaries in a sharded deployment</i></p>
</center>

## Routing queries

There are two ways for database clients to retrieve data from sharded databases: by querying an individual shard, or by querying all shards and aggregating the results. The former is commonly used in OLTP (transactional) systems, e.g. real time applications, and the latter is more commonly used in OLAP (analytical) databases, e.g. batch reports generation.

pgDog plans to have good support for direct-to-shard queries first, and add limited support for aggregates later on. Aggregation can get pretty complex and require query rewriting[^1].

[^1]: Examples of query rewriting can be found in the PostgreSQL's [postgres_fdw](https://www.postgresql.org/docs/current/postgres-fdw.html) extension.

### Parsing SQL

[`pgdog-routing`](https://github.com/levkk/pgdog/tree/main/plugins/pgdog-routing) parses queries using [`pg_query`](https://docs.rs/pg_query/latest/pg_query/), which allows it to extract semantic meaning directly
from query text, without the user having to provide sharding hints (like sharding hints in query comments, for example). Since the plugin can understand SQL, it can automatically extract column values from queries (and results) and re-route them accordingly.
