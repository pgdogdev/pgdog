# Horizontal vs. functional sharding

Applications built on relational databases, like PostgreSQL, can benefit from multiple scaling techniques. Sharding is a technique to scale database writes: the performance of INSERT, UPDATE, and DELETE queries.

There are two types of sharding: horizontal and functional. [PgDog](/) aims to automate both of them, making the scaling your PostgreSQL workloads easier. This article discusses both of these techniques along with their trade-offs and recommended implementation.

## Types of sharding

**Horizontal sharding** works by splitting all your database tables evenly between multiple machines. It uses a hashing function and applies it to a table column and all its foreign key references. Your application will need to connect to all machines and pick the right one to query based on the same hashing function applied to query parameters.

**Functional sharding** is arguably simpler: it works by taking whole tables from your primary database and moving them to another database. Your application will need to connect to both and explicitely choose the right database to talk to based on the tables in the query.

### Traditional trade-offs

Most sharding done today implements both of these techniques in the application layer. Functional sharding is the most popular, since it's pretty easy (relatively speaking) to implement by hand. However, once the tables are moved to their own databases, operations like joins between your other tables become difficult. Joining data between two databases requires doing so in the application and suffers from performance and accuracy problems, as each join has to be done manually with code and by pulling all the data off of each database.

Horizontal sharding requires that the sharding key, a table column, is representative of how data is accessed by the application. If chosen wrong, the percentage of cross-shard (aka all-shard) queries becomes too high and performance benefits brought by splitting data between multiple databases are reduced.



## When to shard?

Sharding should be considered when your database load is about to impact the stability (and performance) of your application. With PostgreSQL workloads, you can usually see this coming by looking out of a few indicators:

- Peak database CPU usage is over 40%
- Average query latency has been growing steadily
