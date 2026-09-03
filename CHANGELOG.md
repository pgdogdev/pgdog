# Changelog

PgDog is released weekly, on Thursdays. Each release includes tagged Docker images, pre-built binaries and a documented list of changes.

For the [Enterprise edition](https://docs.pgdog.dev/enterprise_edition/), see [CHANGELOG-ENTERPRISE.md](CHANGELOG-ENTERPRISE.md).

### v0.1.57

| Application | Docker image                     |
| ----------- | -------------------------------- |
| PgDog       | `ghcr.io/pgdogdev/pgdog:v0.1.57` |

#### Performance

- SCRAM client authentication (which uses expensive crypto) is now pushed to background threads, which will avoid blocking other clients from running queries during connection storms

#### Features

- Add `listen_backlog` configuration setting to control how many _new_, _concurrent_, TCP connections PgDog will allow until they are silently rejected @ryderhwang
- `DISCARD` now clears client's prepared statement cache, advisory locks & notification channels @murex971
- `CREATE TEMP TABLE` now pins the client connection until the table is dropped or the client disconnects @sgrif
- Count and show number of idle-in-transaction timeouts as a pool statistic @murex971
- Add support for `SCRAM-SHA-256-PLUS` client authentication @abnegate
- Add `banned` gauge to OpenMetrics and OTEL indicating which database is currently blocked from reads by the load balancer @ChrisJr404

#### Bugs

- Fix cross-shard `variance()` crash when empty/null results returned by shards @sgrif
- Prepared statements prepared via simple protocol now respect `prepared_statements_ttl` @jkaczman

#### Code quality

- Refactor resharding task queue to use more types @meskill
- Clean up resharding logging @levkk
- Refactor the query engine code to support cross-shard subqueries and CTEs @jkaczman

### v0.1.56

| Application | Docker image                     |
| ----------- | -------------------------------- |
| PgDog       | `ghcr.io/pgdogdev/pgdog:v0.1.56` |

Hot fix for [v0.1.55](https://github.com/pgdogdev/pgdog/releases/tag/v0.1.55).

#### Bug fixes

- Don't set the thread stack size to 32MB by default and don't warn the user about a deprecated setting at runtime (`query_parser_engine`).

### v0.1.55

| Application | Docker image                     |
| ----------- | -------------------------------- |
| PgDog       | `ghcr.io/pgdogdev/pgdog:v0.1.55` |

#### Security

- Docker images now use keyless cosign attestation @dkarter

#### Features

- Support execution of multi-statement queries sent via the simple protocol, e.g., `SET statement_timeout TO 0; SELECT * FROM users;`. Previously, this would error out. Limitation: we don't automatically start a transaction, so only one DML statement per query is allowed. @levkk
- Switch to our implementation of query normalization instead of using `pg_query` (only relevant if using the Enterprise edition) @sgrif @jkaczman

#### Performance

- Remove more unnecessary allocations @sgrif @meskill @jkaczman
- Don't pin small future in hot path (1% performance improvement or so) @ygxio

#### Bug fixes

- Avoid `NoPrimary` errors when primary election is delayed (`role = "auto"` mode) @meskill
- Preserve query case in admin commands before passing the string to the query parser @meskill
- Proxy shutdown was sometimes delayed by a race condition in the load balancer monitor @meskill
- Support parameter type casting in `EXECUTE` statements @murex971
- Don't run unnecessary `SELECT` query during sharding key update @jkaczman

#### Code quality

- Add `query_parser_engine = "pg_query_protobuf"` deprecation warning and remove the setting from usage @jkaczman
- Dead code removal @sgrif @meskill
- Use `pub(crate)` instead of `pub` to help with dead code analysis @sgrif
- Refactor the resharding task runner to use more types @meskill
- Move pool configuration into `pgdog-config` crate, for easier use in Enterprise code @meskill
- Fix flaky tests @jkaczman

#### New Contributors

- @dkarter made their first contribution in https://github.com/pgdogdev/pgdog/pull/1442

### v0.1.54

| Application | Docker image                     |
| ----------- | -------------------------------- |
| PgDog       | `ghcr.io/pgdogdev/pgdog:v0.1.54` |

#### Performance

- 8% performance improvement in the connection pooler by optimizing our use of Tokio futures @ygxio
- Additional performance improvements by using Tokio futures correctly @sgrif
- Remove several unnecessary memory allocations @ygxio

#### Bugs

- Don't validate replica identity during the schema sync & data sync phases of resharding if caller does not intend to use logical replication to sync real-time data @bhargavtheertham-cb
- Extended protocol pipeline messages were incorrectly ordered in cross-shard queries @meskill
- Schema sharding shouldn't trigger the incomplete omnisharded writes check @jkaczman
- `DISTINCT` state was not reset between cross-shard queries in the same transaction @levkk

#### Features

- Make OpenMetrics host configurable with `openmetrics_host` @murex971
- Block direct-to-shard queries when `SET pgdog.sharding_key` doesn't match a mapped value in list-based and range-based sharding configuration @jkaczman
- Setting `read_only` on a user in `users.toml` will direct that user's traffic to replicas, if configured @jkaczman
- Partial support for `PREPARE` and `EXECUTE` (including sharding); the prepared statement can be only executed with `EXECUTE` at the moment and cannot be executed using the extended protocol @levkk
- Wait for health check to pass before allowing traffic on _newly_ added databases in `pgdog.toml` @sgrif
- Optional FIPS mode (compile time flag: `cargo build -p pgdog --release --features fips`)

#### Testing

- Basic Haskell acceptance tests @levkk
- Ensure we only generate a node id when `NODE_ID` is set @meskill

#### Code quality

- Remove library target and only build binary target, allowing us to more easily detect dead code @sgrif
- Remove a bunch of dead code @sgrif
- Cleanup multi-shard and row decoder code @levkk

### v0.1.53

| Application | Docker image                     |
| ----------- | -------------------------------- |
| PgDog       | `ghcr.io/pgdogdev/pgdog:v0.1.53` |

#### New parser

New parser is now **enabled by default**. Please test this release before launching in production. You should see 6x-8x speed improvements resulting in lower latency for query cache misses, and up to 90% memory usage reduction overall. This is not a typo! We fixed quite a few things here.

This also makes the `query_parser_engine` setting redundant and obsolete. It will now be ignored.

By @sgrif

#### Bug fixes

- Fix negative interval text format parsing in cross-shard aggregates @bryanmehall
- Connection pools were giving clients closed server connections, causing intermittent errors @nsavvide
- In `role = "auto"` mode, set all hosts to `role = "replica"` until proven otherwise; this unblocks reads faster during configuration changes @murex971
- [New parser] Fix `GRANT` and `CREATE STATISTICS` DDL handling @levkk
- When using schema-based sharding, ignore the inconsistent omnisharded tables write check @levkk
- Include `sv_idle_xact` calculation into the example Datadog dashboard @zacharywelch
- Fix crash when a cross-shard `avg()` calculation was done on empty tables @sgrif
- `RESET PREPARED` admin command wasn't doing anything @IgorOhrimenko
- Fix Mac OS build for the new parser @jkaczman
- Fix pipelined queries using Postgres portals; specifically affected the Node JS Postgres drivers @nsavvide
- Sending portal close messages could potentially close prepared statements that were identically named @meskill
- Schema synchronization during resharding now restores `REPLICA IDENTITY` indexes in the post-data phase @rlittlefield
- Cross-shard aggregates with passthrough columns now work correctly @jkaczman
- `PREPARE` was leaking memory in the prepared statements cache @meskill
- Fix incorrect handling of errors in mixed simple/extended protocol exchange @jkaczman

#### Performance

- Lower the number of memory allocations in prepared statements cache @meskill
- Remove unnecessary statement allocation in the parser (50% allocation reduction in the hot path) @sgrif

#### Features

- Track selected, inserted and deleted rows, as reported by server connections @murex971
- Support prepared statements cache TTL (time to live), automatically re-planning statements periodically; this sidesteps the stale execution plan problem @meskill

#### Misc

- Clippy pass over new parser code @sgrif
- Bump Mac OS runner to `macos-26` (`macos-14` is about to be deprecated) @jkaczman
- Docker compose demo wasn't saving Postgres data correctly @andyatkinson
- Small refactor of pool health tracking @sgrif

#### New Contributors

- @bryanmehall made their first contribution in https://github.com/pgdogdev/pgdog/pull/1321
- @zacharywelch made their first contribution in https://github.com/pgdogdev/pgdog/pull/1333
- @jkaczman made their first contribution in https://github.com/pgdogdev/pgdog/pull/1336
- @andyatkinson made their first contribution in https://github.com/pgdogdev/pgdog/pull/1343

## Remaining releases

Please see [GitHub releases](https://github.com/pgdogdev/pgdog/releases) for changelogs for previous releases.

| Application | Docker image                     |
| ----------- | -------------------------------- |
| PgDog       | `ghcr.io/pgdogdev/pgdog:v0.1.54` |
