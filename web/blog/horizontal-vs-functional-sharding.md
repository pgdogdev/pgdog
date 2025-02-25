# Horizontal vs. functional sharding

*Feb 25, 2025*

Applications built on relational databases, like PostgreSQL, can benefit from multiple scaling techniques. Sharding is the one that scales writes, by improving the performance of INSERT, UPDATE, and DELETE queries.

There are two types of sharding: horizontal and functional. [PgDog](/) aims to automate both of them. This article discusses both of these techniques along with their trade-offs and implementation.

## Types of sharding

### Horizontal sharding

Horizontal sharding works by splitting data evenly between multiple machines. It does so by applying a sharding function to a table column and its foreign key references. Applications will need to connect to all machines and, for each query, apply the same function to query parameters.


#### Sharding function

A sharding function transforms any table column to a number. This number is then divided by the number of shards in the system, and the remainder of that division is the shard where that table row should be stored.

<center><pre><code>shard = hash(id) % number of shards</code></pre></center>

The sharding function implemented by PgDog is based off of the hashing function used in PostgreSQL declarative partitions. This choice was intentional, since this allows data to be sharded both at the pooler and inside the database. Additionally, each shard can use partitions to ensure only the correct rows are inserted.

If you have access to a PostgreSQL database, you can test this function directly in psql. For example, you can create a partitioned table like so:

<pre><code>CREATE TABLE users (id BIGINT PRIMARY KEY)
PARTITION BY HASH(id);</code></pre>

Once you have a partitioned table, you can find out which partitoin (or shard) any row belongs to by calling the <code>satisfies_hash_parittion</code> function on any value of the sharded column, with the total number of shards and the desired shard as arguments:

<pre><code>SELECT satisfies_hash_partition(
        'users'::regclass, -- Name of the partitioned table.
        2, -- Number of shards (aka partitions).
        1, -- The shard where the row should go.
        3::bigint -- The value of the sharded column (the primary key).
);</code></pre>


This will return true, which means the row with <code>id = 3</code> should go to shard 1. If the function returned false, this would mean the row should go to another shard.

#### Sharding an existing database

Horizontal sharding requires that the sharding key, i.e. a table column, is representative of how data is accessed by the application. For example, let's assume your database has 2 tables, "users" and "payments", with the following schema:

<center>
    ![Schema](/assets/schema.svg)<br>
    <small><i>E/R diagram describing the database schema.</i></small>
</center>

Before sharding your database, it's helpful to come up with an E/R (entity/relationship) diagram, like the one above, to map out tables and their relationships. The primary key of the table at the top, with the most transitive connections, is a good choice for the sharding key.


Choosing the most referenced table allows all queries to specify a sharding key. For example, if most queries that access these tables reference either the `"users"."id"` column or, by extension, the `"payments"."user_id"` column, this may indicate that  your application tends to access data one user at a time.

When sharding this schema, data in the "users" table will be split by applying the sharding function to the "id" column, while data in the "payments" table will be split by applying the same function to the "user_id" column:

<center>
    ![Schema](/assets/sharded-schema.svg)<br>
    <small><i>Database schema after sharding.</i></small>
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
