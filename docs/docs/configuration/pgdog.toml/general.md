
# General settings

General settings are relevant to the operations of the pooler itself, or apply to all database pools.

### `host`

The IP address of the local network interface pgDog will bind to listen for connections.

Default: **`0.0.0.0`** (all interfaces)

### `port`

The TCP port pgDog will bind to listen for connections.

Default: **`6432`**

### `workers`

Number of Tokio threads to spawn at pooler startup. In multicore systems, the recommended setting is two (2) per
virtual CPU. The value `0` means to spawn no threads and use the current thread runtime (single-threaded). The latter option is better on IO-bound systems where multi-threading is not necessary and could even hamper performance.

### `default_pool_size`

Default maximum number of server connections per database pool. The pooler will not open more than this many PostgreSQL database connections when serving clients.

Default: **`10`**

### `min_pool_size`

Default minimum number of connections per database pool to keep open at all times. Keeping some connections
open minimizes cold start time when clients connect to the pooler for the first time.

Default: **`1`**


### `pooler_mode`

Default pooler mode to use for database pools. See [Transaction mode](../../features/transaction-mode.md) and [session mode](../../features/session-mode.md) for more details on each mode.

Default:  **`transaction`**

## TLS

### `tls_certificate`

Path to the TLS certificate pgDog will use to setup TLS connections with clients. If none is provided, TLS will be disabled.

Default: **none**

### `tls_private_key`

Path to the TLS private key pgDog will use to setup TLS connections with clients. If none is provided, TLS will be disabled.

Default: **none**

## Healthchecks

### `healthcheck_interval`

Frequency of healthchecks performed by pgDog to ensure connections provided to clients from the pool are working.

Default: **`30s`**

### `idle_healthcheck_interval`

Frequency of healtchecks performed by pgDog on idle connections. This ensures the database is checked for health periodically when
pgDog receives little to no client requests.

Default: **`30s`**

#### Note on `min_pool_size`

Idle [healthchecks](../../features/healthchecks.md) try to use existing idle connections to validate the database is up and running. If there are no idle connections available, pgDog will create an ephemeral connection to perform the healthcheck. If you want to avoid creating healtcheck connections, make sure to have `min_pool_size` to be at least `1`.

### `idle_healthcheck_delay`

Delay running idle healthchecks at pgDog startup to give databases (and pools) time to spin up.

Default: **`5s`**

## Timeouts

These settings control how long pgDog waits for maintenance tasks to complete. These timeouts make sure pgDog can recover
from abnormal conditions like hardware failure.

### `rollback_timeout`

How long to allow for `ROLLBACK` queries to run on server connections with unfinished transactions. See [transaction mode](../../features/transaction-mode.md) for more details.

### `ban_timeout`

Pools blocked from serving traffic due to an error will be placed back into active rotation after this long. This ensures
that servers don't stay blocked forever due to healthcheck false positives.

Default: **`300s`** (5 minutes)

### `shutdown_timeout`

How long to wait for active clients to finish transactions when shutting down. This ensures that pgDog redeployments disrupt as few
queries as possible.

Default: **`60s`**

## Load balancer

### `load_balancing_strategy`

Which strategy to use for load balancing read queries. See [load balancer](../../features/load-balancer.md) for more details. Available options are:

* `random`
* `least_active_connections`
* `round_robin`

Default: **`random`**
