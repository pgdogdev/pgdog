# Configuration overview

pgDog uses the TOML configuration language for its two configuration files: `pgdog.toml` and `users.toml`. Both are required for pgDog to run, but most settings are optional with sane defaults, so a basic pgDog deployment requires very little work to configure.

Both configuration files should be in the current working directory when running pgDog. Alternatively, you can pass
`--config=<path>` and `--users=<path>` arguments to pgDog on startup.


## `pgdog.toml`

This configuration file contains PostgreSQL server information like hosts, ports, and database names. Additionally,
it contains pooler-wide settings like plugins information and general settings like default pool size for all databases.


### General settings

General settings are relevant to the operations of the pooler itself, or apply to all database pools.

**`host`**

The IP address of the local network interface pgDog will bind to. The default value is **`0.0.0.0`** which is all
interfaces.

**`port`**

The TCP port pgDog will bind to. Default value is **`6432`**.

**`workers`**

Number of Tokio threads to spawn at pooler startup. In multicore systems, the recommended setting is two (2) per
virtual CPU. The value `0` means to spawn no threads and use the main single-thread runtime. This option is better on IO-bound systems where multi-threading is not necessary.

**`default_pool_size`**

Default maximum number of server connections per database pool. The pooler will not open more than this many PostgreSQL database onnections when serving clients. Default value is **`10`**.

**`min_pool_size`**

Default minimum number of connections per database pool to keep open at all times. Keeping some connections
open minimizes cold start time when clients connect to the pooler for the first time. Default value is **`1`**.

**`pooler_mode`**

Default pooler mode to use for database pools. See [Transaction mode](../features/transaction-mode.md) for more details on how this works. Default value is **`transaction`**.

#### Example

```toml
[general]
host = "0.0.0.0"
port = 6432
workers = 0 # Use current_thread runtime.
default_pool_size = 10
min_pool_size = 1
pooler_mode = "transaction"
```

### Databases

Databases contain routing information for PostgreSQL databases proxied by pgDog. Unlike the general settings, databases are a TOML list, which means multiple entries of `[[databases]]` can be made in the configuration file.

#### Example

```toml
[[databases]]
name = "prod"
host = "10.0.0.1"
port = 5432
database_name = "postgres"

[[databases]]
name = "prod"
host = "10.0.0.2"
port = 5432
database_name = "postgres"
role = "replica"
```

#### Reference

**`name`**

Database name visible to clients that connect to pgDog. This name can be different from the actual Postgres database
name and must be unique for each database you want pgDog to proxy.


**`port`**

The port on which the database is listening for connections. Default is **`5432`**.

**`database_name`**

The name of the PostgreSQL database pgDog will connect to. This doesn't have to be the same as the **`name`** setting.

**`role`**

Database role is the type of role this database occupies in the cluster. The two options currently supported are: `primary` and `replica`. The default value for this option is **`primary`**.

**`user`**

Name of the PostgreSQL user pgDog will use to connect to the database server. This setting is optional and by default pgDog will use the user name specified in `users.toml` configuration file.

**`password`**

User password pgDog will provide to the PostgreSQL server when creating connections. This setting is optional and by default pgDog will use the password specified in `users.toml` configuration file.

## `users.toml`

This configuration file contains user-specific settings and sensitive information like passwords. It can be encrypted using system-specific toolkits like Kubernetes secrets or AWS Secrets Manager. This file contains only one section, the TOML list of `[[users]]`.

#### Example

```toml
[[users]]
database = "prod"
name = "alice"
password = "hunter2"

[[users]]
database = "prod"
name = "bob"
password = "super-secret"
```

#### Reference

**`database`**

The name of the database this user belongs to. This is the same as the `name` setting in `[[databases]]`.

**`name`**

The name of the user.


**`password`**

The user's password.
