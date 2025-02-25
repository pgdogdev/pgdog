# Horizontal vs. functional sharding

*Feb 25, 2025*

Applications built on relational databases, like PostgreSQL, can benefit from multiple scaling techniques. Sharding is one that scales database writes: the performance of INSERT, UPDATE, and DELETE queries.

There are two types of sharding: horizontal and functional. [PgDog](/) aims to automate both of them, making the scaling your PostgreSQL workloads easier. This article discusses both of these techniques along with their trade-offs.

## Types of sharding

### Horizontal

Horizontal sharding works by splitting data in your database tables evenly between multiple machines. It does so using a hashing function and applies it to a table column and all its foreign key references. Your application will need to connect to all machines and pick the right one to query based on the same hashing function, applied to query parameters.

<center>
    ![Horizontal sharding](/assets/horizontal-sharding.svg)<br>
    <small><i>Horizontal sharding using a<br> hashing function applied to a column.</i></small>
</center>

#### Implementation

Horizontal sharding requires that the sharding key, i.e. a table column, is representative of how data is accessed by the application. For example, let's assume your database has 2 tables, "users" and "payments", with the following schema:

<center>
    ![Schema](/assets/schema.svg)<br>
    <small><i>E/R diagram describing the database schema.</i></small>
</center>

Before sharding your database, it's helpful to come up with an E/R (entity/relationship) diagram, like the one above, to map out tables and their relationships. The primary key of the table at the top with the most transitive connections is a good choice for the sharding key.

If most queries that access these tables reference either the `"users"."id"` column or, by extension, the `"payments"."user_id"` column, that would make it a great choice for a sharding key. This would indicate that, more commonly than not, your application accesses data one user at a time.

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
