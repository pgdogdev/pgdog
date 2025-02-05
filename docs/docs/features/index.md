# Features overview

PgDog contains multiple foundational and unique features which make it a great choice
for modern PostgreSQL deployments.

Most features are configurable and can be toggled and tuned. Experimental features are marked
as such, and users are advised to test them before deploying to production. Most foundational features like
load balancing, healthchecks, and query routing have been battle-tested and work well in production.

## Summary


!!! note
    PgDog is just getting started and most features are incomplete. The documentation
    is sometimes written to reflect the desired state. In the case where the feature is not
    complete, a note is added to that effect.

| Feature | Description | State |
|---------|-------------|-------|
| [Transaction mode](transaction-mode.md) | Multiplex transactions and servers for busy PostgreSQL deployments. | ✔️ Good |
| [Load balancer](load-balancer.md) | Split query traffic evenly across multiple databases. | 🔨 Work in progress |
| [Healthchecks](healthchecks.md) | Periodically check databases to ensure they are up and can serve queries. | ✔️ Good |
| [Live configuration reloading](../configuration/index.md) | Update configuration at runtime without having to restart PgDog. | 🔨 Work in progress |
| [Sharding](sharding/index.md) | Automatic query routing using a sharding key to scale writes horizontally. | 🔨 Work in progress |
| [Plugins](plugins/index.md) | Pluggable libraries to parse and route queries, loaded at runtime. | ✔️ Good |
| [Authentication](authentication.md) | Support for various PostgreSQL authentication mechanisms, e.g. `SCRAM-SHA-256`. | 🔨 Work in progress |
| [Session mode](session-mode.md) | Compatibility mode with direct Postgres connections. | 🔨 Work in progress |

## OS support

PgDog doesn't use any OS-specific features and should run on all systems supported by the Rust compiler, e.g. Linux (x86 and ARM64), Mac OS, and Windows.
