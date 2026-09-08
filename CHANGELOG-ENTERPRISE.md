# Changelog EE

This file contains the list of changes made to the Enterprise edition of PgDog. Since it's being developed in a private repository, this seemed like
the most optimal way to share those changes.

### v2026-09-03

**OS version**: [v0.1.57](CHANGELOG.md#v0.1.57)

| Application   | Docker image                                            |
| ------------- | ------------------------------------------------------- |
| PgDog         | `ghcr.io/pgdogdev/pgdog-enterprise:v2026-09-03`         |
| Control plane | `ghcr.io/pgdogdev/pgdog-enterprise/control:v2026-09-03` |

#### Features

- Added a UI in the control plane to monitor resharding of databases. It tracks the resharding tasks in real-time, with progress reports and ETA.
- Query plans are now sent asynchronously to the control plane, as they are captured by PgDog. This makes them available quicker in the control plane UI, and uses less network bandwidth. Corresponding settings were added to the control plane Helm chart `v0.2.18`.
- Slow query indicator in the control plane is now configurable via the `slow_queries_threshold` [setting](https://github.com/pgdogdev/helm-ee/#state-store) instead of relying on the presence of a query plan.
- Added the ability to trigger alerts on slow queries as detected by `slow_queries_threshold`. Requires the `slow_queries` setting to be [enabled](https://github.com/pgdogdev/helm-ee/#alerting).

#### Bug fixes

- Query blocking command in the admin database now uses correctly normalized SQL; it would previously lowercase the query first, potentially causing mismatching
- PgDog would reload its config when a node would join the cluster, irrespective if connection pool autoscaling was enabled. This would cause connection pools to drop connections due to incompatible configuration changes between the new and old pools. This required the control plane to be enabled.
