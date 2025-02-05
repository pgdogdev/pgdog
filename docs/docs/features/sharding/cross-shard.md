# Cross-shard queries

If a client can't or chooses not to provide a sharding key, PgDog can route the query to all shards and combine the results transparently. To the client, this feels like the query executed against a single database.

<center style="margin-top: 2rem;">
    <img src="/images/cross-shard.png" width="70%" alt="Cross-shard queries" />
    <p><i>A query executed across all 3 shards.</i></p>
</center>

## Architecture

Since PgDog speaks the Postgres protocol, it can connect to multiple database servers and collect `DataRow`[^1] messages as they are being sent by each server. Once all servers finish
executing the query, PgDog processes the result and sends it to the client as if all messages came from one server.

While this works for simple queries, others that involve sorting or aggregation are more complex and require special handling.

## Sorting

If the client requests results to be ordered by one or more columns, PgDog can interpret this request and perform the sorting once it receives all data messages from Postgres. For queries that span multiple shards, this feature allows to retrieve results in the correct order. For example:

```postgresql
SELECT *
FROM users
WHERE admin IS true
ORDER BY id DESC;
```

This query has no sharding key, so PgDog will send it to all shards in parallel. Once all shards receive the query, they will filter data from their respective `"users"` table and send
the results ordered by the `"id"` column.

PgDog will receive rows from all shards at the same time, however Postgres is not aware of other shards in the system so the overall sorting order will be wrong. PgDog will collect all rows, and sort them by the `"id"` column before sending the results over to the client.

Two kinds of sorting is supported:

* Order by column name(s)
* Order by column index

### Order by column name

PgDog can extract the column name(s) from the `ORDER BY` clause of a query and match them
to values in `DataRow`[^1] messages based on their position in the `RowDescription`[^1] message received as the query starts executing on the shards.

For example, if the query specifies `ORDER BY id ASC, email DESC`, both `"id"` and `"email"` columns will be present in the `RowDescription` message along with their data types and locations in `DataRow`[^1] messages.

Once the messages are buffered in PgDog, it will sort them using the data extracted from messages and return the sorted result to the client.

#### Example

```postgresql
SELECT id, email, created_at
FROM users
ORDER BY id, created_at
```

### Order by column index

If the client specifies only the position of the column used in sorting, e.g., `ORDER BY 1 ASC, 4 DESC`, the mechanism for extracting data from rows is the same, except this time we don't need to look up column(s) by name: we have their position in the `RowDescription`[^1] message from the query.

The rest of the process is identical and results are returned in the correct order to the client.

#### Example

```postgresql
SELECT id, email, created_at
FROM users
ORDER BY 1, 3
```

[^1]: [PostgreSQL message formats](https://www.postgresql.org/docs/12/protocol-message-formats.html)

## DDL

DDL statements, i.e., queries that modify the database schema like `CREATE TABLE`, are sent to all shards simultaneously. This allows clients to modify all shard schemas at the same time and requires no special changes to systems used for schema management (e.g., migrations).

This assumes that all shards in the cluster have an identical schema. This is typically desired to make management of sharded databases simpler, but in cases where this is not possible, DDL queries can always be routed to specific shards using [manual routing](manual-routing.md).
