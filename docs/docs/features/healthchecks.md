# Healthchecks

Databases proxied by PgDog are regularly checked with healthchecks. A healthcheck is a simple query, e.g.
`SELECT 1`, which ensures the database is reachable and able to answer requests.

If a database fails a healthcheck, it's placed in a list of banned hosts. Banned databases are removed
from the load balancer and will not serve transactions. This allows PgDog to reduce errors clients see
when a database fails, for example due to hardware issues.

<center>
  <img src="/images/healtchecks.png" width="65%" alt="Healtchecks"/>
  <p><i>Replica failure</i></p>
</center>

## Configuration

Healthchecks are enabled by default and are used for all databases. Healthcheck interval is configurable
on a global and database levels.

The default healthcheck interval is **30 seconds**.

```toml
[global]
healthcheck_interval = 30_000 # ms

[[databases]]
name = "prod"
healthcheck_interval = 60_000 # ms
```

### Timeouts

By default, PgDog gives the database **5 seconds** to answer a healthcheck. If it doesn't receive a reply,
the database will be banned from serving traffic for a configurable amount of time. Both the healthcheck timeout
and the ban time are configurable.

```toml
[global]
healthcheck_timeout = 5_000 # 5 seconds
ban_timeout = 60_000 # 1 minute
```

### Ban expiration

By default, a ban has an expiration. Once the ban expires, the replica is unbanned and placed back into
rotation. This is done to maintain a healthy level of traffic across all databases and to allow for intermittent
issues, like network connectivity, to resolve themselves without manual intervention.

### Failsafe

If all databases in a cluster are banned due to a healthcheck failure, PgDog assumes that healthchecks
are returning incorrect information and unbans all databases in the cluster. This protects against false positives
and ensures the cluster continues to serve traffic.

## Learn more

- [Load balancer](load-balancer.md)
