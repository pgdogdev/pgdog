# Sharding overview

!!! note
    This feature is under active development. It's not quite ready for production. This documentation
    reflects a future state of the feature.

Sharding PostgreSQL databases involves splitting the database between multiple machines and routing read
and write queries to the correct machines using a sharding function. Like its [predecessor](https://github.com/levkk/pgcat), pgDog supports sharded PostgreSQL deployments and can route queries to the corrent shards automatically using a routing [plugin](../plugins/index.md).

<center style="margin-top: 2rem;">
    <img src="/images/sharding.png" width="70%" alt="Sharding" />
    <p><i>Three (3) sharded primaries</i></p>
</center>

## Routing queries

There are two ways for database clients to retrieve data from sharded databases: by querying an individual shard, or by querying all shards and aggregating the results. The former is commonly used in OLTP (transactional) systems, e.g. real time applications, and the latter is more commonly used in OLAP (analytical) databases, e.g. batch reports generation.

pgDog has good support for querying individual shards using a sharding key extracted automatically from queries.
