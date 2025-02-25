# Horizontal vs. functional sharding

*Feb 25, 2025*

Applications built on relational databases, like PostgreSQL, can benefit from multiple scaling techniques. Sharding is the one that scales writes, by improving the performance of INSERT, UPDATE, and DELETE queries.

There are two types of sharding: horizontal and functional. [PgDog](/) aims to automate both of them. This article discusses both of these techniques along with their trade-offs and implementation.

## Types of sharding

### Horizontal sharding

Horizontal sharding works by splitting data evenly between multiple machines. Each machine runs its own PostgreSQL database and has a unique identifier, called a shard number. Shard numbers typically range from 0 to _n_, where _n_ is the total number of shards in the system.

<center>![Shards](/assets/shards.svg)<br>
<small><i>A system with 3 shards.</i></small>
</center>


#### Sharding function

Data is split between shards using something we call a sharding function. A sharding function is just code
that converts some data to a shard number. Typically, a sharding function uses a hashing algorithm and applies a modulus operation to the result:

<center><pre><code>shard_number = hash(data) % num_shards</code></pre></center>


Hashing functions, by design, don't know anything about the data and produce a seeminlgy random result. However, that result is actually deterministic and is evenly distributed along some mathematical function. This ensures that data is evenly split between shards.

The sharding function implemented by PgDog is based off of the hashing function in PostgreSQL declarative partitions. This choice was intentional, since this allows data to be sharded both at the pooler and inside the database. Additionally, each shard can use partitions to ensure only the correct rows are inserted.

If you have access to a PostgreSQL database, you can test this function directly in psql. For example, given a partitioned table:

<pre><code>CREATE TABLE users (
    id BIGINT,
    email TEXT
) PARTITION BY HASH(id);</code></pre>

You can find out which partition (or shard) a row belongs to by calling the <code>satisfies_hash_parittion</code> function on any value of the <code>id</code> column, with the total number of shards and the desired shard as arguments:

<pre><code>SELECT satisfies_hash_partition(
        'users'::regclass, -- Name of the partitioned table.
        2,                 -- Number of shards (aka partitions).
        1,                 -- The shard where the row should go.
        3::bigint          -- The value of the sharded column (the primary key).
);</code></pre>


This function call will return true, which means the row with <code>id = 3</code> should go to shard 1. If the function returned false, this would mean the row should go to a different shard.

#### Sharding an existing database

Sharding a database requires we first pick a the sharding key. A sharding key is a column from some table in our database to which we'll apply our sharding function. Ideally this column is a primary key so we can use all foreign keys that refer to this table as sharding keys as well.

Take the following schema as an example:

<center>
    ![Schema](/assets/schema.svg)
</center>

The `users` table has an `id` column which is referred to from the <code>payments</code> table through the <code>user_id</code> foreign key. If we were to choose `id` column as our sharding key, `payments` can also be sharded using the same function.

Before sharding your database, it's helpful to build an E/R (entity/relationship) diagram, like the one above, to map out tables and their relationships. The primary key of the table with the most connections is a good choice for a sharding key.

When sharding this schema, data in the `users` table will be split evenly between shards by applying the sharding function to the `id` column, while data in the `payments` table will be split by applying the same function to the `user_id` column.

<center>
    ![Schema](/assets/sharded-schema.svg)
</center>

### Functional

Functional sharding is arguably simpler: it works by taking whole tables from your primary database and moving them to another database. Your application will need to connect to both and explicitely choose the right database to talk to based on the tables in each query.

<center>
    ![Functional sharding](/assets/functional-sharding.svg)<br>
    <small><i>Functional sharding by moving tables<br> into their own database.</i></small>
</center>

#### Implementation
Functional sharding is the most popular, since it's pretty easy (relatively speaking) to implement by hand. However, once the tables are moved to their own databases, operations like joins between your other tables become difficult. Joining data between two databases requires doing so in the application and suffers from performance and accuracy problems, as each join has to be done manually with code and by pulling large amounts of the data off of each database.


### Cross-shard queries

Cross-shard (or all-shard) queries fetch data from multiple databases (shards) at the same time. They often lack a sharding key or specify multiple values for the key. For example, if you sharded your database on `"users"."id"` column, a query that doesn't reference that column will have to go to all shards:

<pre><code>SELECT * FROM payments
WHERE created_at < NOW() - INTERVAL '1 day';</code></pre>
