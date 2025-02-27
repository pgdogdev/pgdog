# Horizontal vs. functional sharding

*Feb 25, 2025*

Applications built on relational databases, like PostgreSQL, can benefit from multiple scaling techniques. Sharding is the one that scales writes, by improving the performance of INSERT, UPDATE, and DELETE queries.

There are two types of sharding: horizontal and functional. [PgDog](/) aims to automate both of them. This article discusses both of these techniques along with their trade-offs and implementation.

## Horizontal sharding

Horizontal sharding works by splitting data evenly between multiple machines. Each machine runs its own PostgreSQL database and has a unique identifier, called a shard number. Shard numbers typically range from 0 to _n_, where _n_ is the total number of shards in the system.

<center>![Shards](/assets/shards.svg)<br>
<small><i>A system with 3 shards.</i></small>
</center>


### Sharding function

Data is split between shards using something we call a sharding function. A sharding function is just code
that converts some data to a shard number. Typically, a sharding function uses a hashing algorithm and applies a modulus operation to the result:

<center><pre><code>shard_number = hash(data) % num_shards</code></pre></center>


Hashing functions, by design, don't know anything about the data and produce a seeminlgy random result. However, that result is actually deterministic and is evenly distributed along some mathematical function. This ensures that data is evenly split between shards.

The sharding function implemented by PgDog is based off of the hashing function in PostgreSQL declarative partitioning. This choice is intentional: it allows data to be sharded in the pooler and inside the database.

If you have access to a PostgreSQL database, you can test this function directly in psql. For example, given a partitioned table:

<pre><code>CREATE TABLE users (
    id BIGINT,
    email TEXT
) PARTITION BY HASH(id);</code></pre>

You can find out which partition (or shard) a row belongs to by calling the <code>satisfies_hash_parittion</code> function on any value of the <code>id</code> column, with the total number of shards and desired shard as arguments:

<pre><code>SELECT satisfies_hash_partition(
        'users'::regclass, -- Name of the partitioned table.
        2,                 -- Number of shards (aka partitions).
        1,                 -- Shard where the row should go.
        3::bigint          -- Sharding key (the primary key).
);</code></pre>


This function call will return true, which means the row with <code>id = 3</code> should go to shard 1. If the function returned false, this would mean the row should go to a different shard.

### Sharding a database

To shard a database, we first need to pick a the sharding key. A sharding key is a column from some table to which we'll apply the sharding function. Ideally, this column is a primary key so we can use all foreign keys that refer to this table as sharding keys as well.

Take the following schema as an example:

<center>
    ![Schema](/assets/schema.svg)
</center>

The `users` table has an `id` column which is referred to from the <code>payments</code> table through the <code>user_id</code> foreign key. If we chose `id` column as our sharding key, `payments` can also be sharded using the same function.

It's helpful to build an E/R (entity/relationship) diagram, like the one above, to map tables and their relationships. The primary key of the table with the most relationships is often a good choice for a sharding key.

#### Sharding the schema

Data in the `users` table will be split evenly between shards, by applying the sharding function to the `id` column, while data in the `payments` table will be split by applying the same function to the `user_id` column. When it comes to data integrity, it's always better to measure twice. Since PgDog uses the same sharding function as PostgreSQL partitions, we can use partition data tables to validate
that data on each shard is stored correctly.

Given 3 shards in our system, we can create 3 data partitions, 1 on each shard, along with the parent table, for each table in the schema:

<small><strong>All shards</strong></small>

<pre><code>CREATE TABLE users (
    id BIGINT PRIMARY KEY,
    email VARCHAR,
    created_at TIMESTAMPTZ
) PARTITION BY HASH(id);

CREATE TABLE payments (
    id BIGINT,
    user_id BIGINT REFERENCES users(id),
    amount DOUBLE PRECISION,
    created_at TIMESTAMPTZ,
    PRIMARY KEY (id, user_id)
) PARTITION BY HASH(user_id);</code></pre>

<small><strong>Shard 0</strong></small>

<pre><code>CREATE TABLE users_0
PARTITION OF users FOR VALUES WITH (modulus 3, remainder <strong>0</strong>);

CREATE TABLE payments_0
PARTITION OF payments FOR VALUES WITH (modulus 3, remainder <strong>0</strong>);
</code></pre>

<small><strong>Shard 1</strong></small>

<pre><code>CREATE TABLE users_1
PARTITION OF users FOR VALUES WITH (modulus 3, remainder <strong>1</strong>);

CREATE TABLE payments_1
PARTITION OF payments FOR VALUES WITH (modulus 3, remainder <strong>1</strong>);</code></pre>

<small><strong>Shard 2</strong></small>

<pre><code>CREATE TABLE users_2
PARTITION OF users FOR VALUES WITH (modulus 3, remainder <strong>2</strong>);

CREATE TABLE payments_2
PARTITION OF payments FOR VALUES WITH (modulus 3, remainder <strong>2</strong>);</code></pre>

Recall the definition of our sharding function. Since each shard only has 1 data partition (of 3), if we attempt to insert a row that doesn't belong (i.e. for which, `satisfies_hash_partition` returns false), Postgres will raise an error.

#### Adding data

PgDog shards data, so to add it to the shards, we need to pass it through the pooler. For example, to shard data in the `users` table, you can copy it using just psql:

<pre><code>psql -c '\copy users TO STDOUT' | psql postgres://pgdog-connection-url</code></pre>

PgDog will parse the command, connect to all shards, apply the same hashing function to all rows, splitting them evenly between all 3 shards. Doing the same to the `payments` table will shard that data as well.

For data in a live database, which changes constantly, PgDog is working to support logical replication to copy, synchronize and shard data in real time.

## Functional sharding

Functional sharding is arguably simpler: it works by taking whole tables from your primary database and moving them to another database. Your application will need to connect to both and explicitely choose the right database to talk to based on the tables in each query.

<center>
    ![Functional sharding](/assets/functional-sharding.svg)<br>
    <small><i>Functional sharding by moving tables<br> into their own database.</i></small>
</center>

### Implementation
Functional sharding is the most popular, since it's pretty easy (relatively speaking) to implement by hand. However, once the tables are moved to their own databases, operations like joins between your other tables become difficult. Joining data between two databases requires doing so in the application and suffers from performance and accuracy problems, as each join has to be done manually with code and by pulling large amounts of the data off of each database.


### Cross-shard queries

Cross-shard (or all-shard) queries fetch data from multiple databases (shards) at the same time. They often lack a sharding key or specify multiple values for the key. For example, if you sharded your database on `"users"."id"` column, a query that doesn't reference that column will have to go to all shards:

<pre><code>SELECT * FROM payments
WHERE created_at < NOW() - INTERVAL '1 day';</code></pre>
