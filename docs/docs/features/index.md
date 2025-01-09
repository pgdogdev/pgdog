# Features overview

pgDog contains multiple foundational and unique features which make it a great choice
for modern PostgreSQL deployments.

Most features are configurable and can be toggled and tuned. Experimental features are marked
as such, and users are advised to test them before deploying to production. Most foundational features like
load balancing, healthchecks, and query routing have been battle-tested and work well in production.

## Summary


!!! note
    pgDog is just getting started and most features are incomplete. The documentation
    is sometimes written to reflect the desired state. In the case where the feature is not
    complete, a note is added to that effect.

| Feature | Description | State |
|---------|-------------|-------|
| [Transaction mode](transaction-mode.md) | Multiplex transactions and servers, allowing for high reuse of PostgreSQL server connections. | ✔️ Good |
| [Load balancer](load-balancer.md) | Splits query traffic evenly across multiple databases. | 🔨 Work in progress |
| [Healthchecks](healthchecks.md) | Periodically checks databases to ensure they can serve queries. | ✔️ Good |
| [Live configuration reloading](../configuration/index.md) | Pooler configuration and users can be changed at runtime without restarting the pooler or breaking connections. | 🔨 Work in progress |
| [Sharding](sharding/index.md) | Automatic routing of queries using a sharding key to scale writes horizontally. | 🔨 Work in progress |
| [Plugins](plugins/index.md) | Pluggable libraries to parse and route queries, loaded at runtime. | ✔️ Good |
