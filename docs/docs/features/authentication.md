# Authentication

PostgreSQL servers support many authentication mechanisms. PgDog supports a subset of those, with the aim to support all of them over time. Since PostgreSQL 14, `SCRAM-SHA-256` is widely used to encrypt passwords and PgDog supports this algorithm for both client and server connections.

Authentication is **enabled** by default. Applications connecting to PgDog must provide a username and password which is [configured](../configuration/users.toml/users.md) in `users.toml`. For connecting to PostgreSQL databases,
PgDog currently supports only `SCRAM-SHA-256`.


## Add users

`users.toml` follows a simple TOML list structure. To add users, simply add another `[[users]]` section, e.g.:

```toml
[[users]]
name = "pgdog"
database = "pgdog"
password = "hunter2"
```

PgDog will expect clients connecting as `pgdog` to provide the password `hunter2` (hashed with `SCRAM-SHA-256`), and will use the same username and password to connect to PostgreSQL.

#### Override server credentials

You can override the user and/or
password PgDog uses to connect to Postgres by specifying `server_user` and `server_password` in the same configuration:

```toml
server_user = "bob"
server_password = "opensesame"
```

This allows to separate client and server credentials. In case your clients accidentally leak theirs, you only need to rotate them in the PgDog configuration, without having to take downtime to change passwords in PostgreSQL.

## Passthrough authentication

!!! note
    This feature is a work in progress.

Passthrough authentication is a feature where instead of storing passwords in `users.toml`, PgDog connects to the database server and queries it for the password stored in `pg_shadow`. It then matches
this password to what the user supplied, and if they match, authorizes the connection.

Passthrough authentication simplifies PgDog deployments by using a single source of truth for authentication.

Currently, passthrough authentication is a work-in-progress. You can track progress in [issue #6](https://github.com/levkk/pgdog/issues/6).

## Security

Since PgDog stores passwords in a separate configuration file, it's possible to encrypt it at rest without compromising the DevOps experience. For example, Kubernetes provides built-in [secrets management](https://kubernetes.io/docs/concepts/configuration/secret/) to manage this.
