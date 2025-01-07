# Sharding overview

!!! note
    This feature is under active development. It's not quite ready for production usage.

Sharding PostgreSQL databases involves splitting the database between multiple machines and routing both read
and write queries to the correct machines based on a sharding function.

Like its [predecessor](https://github.com/levkk/pgcat), pgDog supports sharded deployments by routing queries
automatically. Which shard to use for a particular query is determined by query routing [plugin](../plugins/index.md).
