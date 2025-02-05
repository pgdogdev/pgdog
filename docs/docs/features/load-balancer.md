# Load balancer

PgDog operates at the application layer (OSI Level 7) and is capable of load balancing queries across
multiple PostgreSQL databases.

<center>
  <img src="/images/replicas.png" width="65%" alt="Load balancer" />
</center>

## Strategies

The load balancer is configurable and can route queries
using one of several strategies:

* Random (default)
* Least active connections
* Round robin


### Random

Queries are sent to a database based using a random number generator modulus the number of replicas in the pool.
This strategy is the simplest and often effective at splitting traffic evenly across the cluster. It's unbiased
and assumes nothing about available resources or query performance.

This strategy is used by **default**.

### Least active connections

PgDog keeps track of how many active connections each database has and can route queries to databases
which are least busy executing requests. This allows to "bin pack" the cluster based on how seemingly active
(or inactive) the databases are.

This strategy is useful when all databases have identical resources and all queries have roughly the same
cost and runtime.

### Round robin

This strategy is often used in HTTP load balancers like nginx to route requests to hosts in the
same order they appear in the configuration. Each database receives exactly one query before the next
one is used.

This strategy makes the same assumptions as [least active connections](#least-active-connections), except it makes no attempt to bin pack
the cluster with workload and distributes queries evenly.

## Configuration

The load balancer is enabled automatically when a database cluster contains more than
one database. For example:

```toml
[[databases]]
name = "prod"
role = "replica"
host = "10.0.0.1"

[[databases]]
name = "prod"
role = "replica"
host = "10.0.0.2"
```

## Learn more

- [Healthchecks](healthchecks.md)
