# Database settings

Database settings configure which databases PgDog is proxying. This is a TOML list of hosts, ports, and other settings like database roles (primary or replica). For each database host, add a `[[databases]]` entry to `pgdog.toml`. For example:

```toml
[[databases]]
name = "prod"
host = "10.0.0.1"
port = 5432

[[databases]]
name = "prod"
host = "10.0.0.2"
port = 5432
role = "replica"
```

### `name`

Name of your database. Clients that connect to PgDog will need to use this name to refer to the database. For multiple entries part of
the same cluster, use the same `name`.

Default: **none** (required)


### `host`

IP address or DNS name of the machine where the PostgreSQL server is running. For example:

- `10.0.0.1`
- `localhost`
- `prod-primary.local-net.dev`

Default: **none** (required)

### `port`

The port PostgreSQL is running on. More often than not, this is going to be `5432`.

Default: **`5432`**

### `role`

Type of role this host performs in your database cluster. This can be either `primary` for primary databases that serve writes (and reads),
and `replica` for PostgreSQL replicas that can only serve reads.

Default: **`primary`**

### `database_name`

Name of the PostgreSQL database on the server PgDog will connect to. If not set, this defaults to `name`.

Default: **none** (defaults to `name`)

### `user`

Name of the PostgreSQL user to connect with when creating backend connections from PgDog to Postgres. If not set, this defaults to `name` in [`users.toml`](../users.toml/users.md). This setting is used to override `users.toml` configuration values.

Default: **none** (see [`users.toml`](../users.toml/users.md))

### `password`

Password to use when creating backend connections to PostgreSQL. If not set, this defaults to `password` in [`users.toml`](../users.toml/users.md). This setting is used to override `users.toml` configuration values.

Default: **none** (see [`users.toml`](../users.toml/users.md))
