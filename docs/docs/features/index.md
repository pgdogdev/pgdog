# pgDog features

pgDog contains multiple foundational and unique features which make it a great choice for modern PostgreSQL deployments.

## Load balancing

pgDog acts as an application level load balancer (OSI Level 7) for PostgreSQL. It routes transcations
from clients to different Postgres databases, allowing a cluster of replicas to share the load.

### Healthchecks

pgDog issues regular health checks to all databases and maintains a list of healthy databases. Transactions
are only routed to healthy hosts, while databases that experience errors are removed from the rotation automatically.

#### Automatic repair 
If a previously unhealthy host is repaired, pgDog will automatically detect this change and place the healthy
database back in rotation.

