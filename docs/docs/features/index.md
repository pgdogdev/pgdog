# Features

pgDog contains multiple foundational and unique features which make it a great choice
for modern PostgreSQL deployments.

Most features are configurable and can be toggled and tuned. Experimental features are marked
as such, and users are advised to test them before deploying to production. Most foundational features like
load balancing, healthchecks, and query routing have been battle-tested and work well in production.

## Summary

| Feature | Description | State |
|---------|-------------|-------|
| [Transaction mode](transaction-mode.md) | Multiplex transactions and servers, allowing for high reuse of PostgreSQL server connections. | ✔️ Good |
| [Load balancer](load-balancer.md) | Splits query traffic evenly across multiple databases. | 🔨 Work in progress |
| [Healthcheks](healthchecks.md) | Periodically checks databases to ensure they can serve queries. | ✔️ Stable |
