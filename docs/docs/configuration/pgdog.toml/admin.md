# Admin database settings

Admin database settings control access to the [admin](../../administration/index.md) database which contains real time statistics about internal operations
of PgDog. For example:

```toml
[admin]
password = "hunter2"
```

### `name`

Admin database name.

Default: **`admin`**

### `user`

User allowed to connect to the admin database. This user doesn't have
to be configured in `users.toml`.

Default: **`admin`**

### `password`

Password the user needs to provide when connecting to the admin database. By default, this is randomly
generated so the admin database is locked out unless this value is set.

!!! note
    If this value is not set, admin database access will be restricted.

Default: **random**
