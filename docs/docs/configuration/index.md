# Configuration overview

pgDog uses the [TOML](https://toml.io/en/) configuration language for its two configuration files: `pgdog.toml` and `users.toml`. Both are required for pgDog to run, but most settings are optional with sane defaults, so a basic pgDog deployment requires very little work to configure.

By default, pgDog looks for both configuration files in the current working directory. Alternatively, you can pass
`--config=<path>` and `--users=<path>` arguments to pgDog on startup.

### Hot reload

Most settings can be reloaded without restarting pgDog. This allows to tweak them at runtime without breaking client or server connections. For settings that can't be changed at runtime, a note is added to the documentation.

## Overview

| Name | Description |
|------|-------------|
| [General](pgdog.toml/general.md) | General pooler settings like `host`, `port` and various timeouts. |
| [Databases](pgdog.toml/databases.md) | PostgreSQL databases proxied by pgDog. |
| [Plugins](pgdog.toml/plugins.md) | Plugins configuration. |
| [Users](users.toml/users.md) | List of users (with passwords) that are allowed to connect to pgDog. |
