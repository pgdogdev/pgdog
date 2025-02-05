# Manual routing

In cases where the sharding key is not obvious or can't be extracted from the query,
PgDog supports extracting it from a query comment. For example:

```postgresql
/* pgdog_shard: 1 */ SELECT * FROM users WHERE email = $1
```

will be routed to the second shard in the configuration.

## Syntax

Either the shard or the sharding key can be specified in a comment. To specify a shard number directly, write it like so:

```postgresql
/* pgdog_shard: <number> */
```

where `<number>` is the shard number, starting at 0. This annotation can be placed anywhere in
the query, or be added to an existing comment.

### Sharding key

!!! note
    This feature is not built yet. It requires an implementation of a [sharding function](sharding-functions.md) first.

If you don't know the shard number but have a sharding key, e.g., the value of a column used for sharding your database, you can specify it in a comment as well:

```postgresql
/* pgdog_sharding_key: <value> */
```

PgDog will extract this value from the query and apply a [sharding function](sharding-functions.md) to find out the actual shard number.

## Usage in frameworks

Some web frameworks support adding comments to queries easily. For example, if you're using Rails, you can add a comment like so:

=== "Rails"
    ```ruby
    User
      .where(email: "test@test.com")
      .annotate("pgdog_shard: 0")
      .to_sql
    ```

=== "Query"
    ```postgresql
    SELECT "users".* FROM "users" WHERE "email" = $1 /* pgdog_shard: 0 */
    ```

Others make it pretty difficult, but still possible. For example, Laravel has a [plugin](https://github.com/spatie/laravel-sql-commenter) to make it work while SQLAlchemy makes you write some [code](https://github.com/sqlalchemy/sqlalchemy/discussions/11115).

For this reason, it's best to use [automatic routing](automatic-routing.md) as much as possible.
