# PgDog Production Readiness Test Suite

Validates PgDog for multi-tenant production use with 2000+ tenant databases, wildcard pool templates, and passthrough authentication.

## Prerequisites

- **Docker** and **Docker Compose** v2+
- **PostgreSQL client tools**: `psql`, `pgbench`
- **Python 3** (for config generation)
- **Rust toolchain** (for building PgDog and Rust integration tests)
- **curl** (for metrics validation)
- (Optional) **kubectl** for K8s tests

## Quick Start

```bash
# Full suite — builds PgDog, starts Postgres, creates 2000 databases, runs all tests
bash integration/production_readiness/run.sh

# Customized run
bash integration/production_readiness/run.sh \
    --tenant-count 500 \
    --duration 120 \
    --pool-size 15 \
    --max-wildcard-pools 1000
```

## Running Individual Tests

Start infrastructure first, then run tests independently:

```bash
# 1. Start Postgres
cd integration/production_readiness
docker compose up -d

# 2. Create tenant databases
bash setup/generate_tenants.sh --count 2000

# 3. Generate PgDog config
python3 setup/generate_config.py --pool-size 10 --output-dir config

# 4. Start PgDog (from the config directory)
cd config && /path/to/pgdog &

# 5. Run individual tests
bash load/scale_connect.sh --count 2000
bash load/multi_tenant_bench.sh --tenant-count 100 --clients 50 --duration 60
bash load/passthrough_auth.sh --user-count 100
bash load/pool_pressure.sh --pool-size 5 --connections 50
bash load/sustained_load.sh --duration 10 --clients 50

# New realistic tests
bash load/pool_lifecycle.sh --idle-timeout 10 --cycles 3 --batch-size 50
bash load/connection_storm.sh --storm-size 200 --parallel 100
bash load/idle_in_transaction.sh --target-db tenant_1
bash load/graceful_shutdown.sh --pgdog-bin /path/to/pgdog --config-dir config

# Fault injection tests (require toxiproxy running)
bash setup/configure_toxiproxy.sh
bash load/backend_failure.sh --target-db tenant_1
bash load/network_latency.sh --latency 5 --jitter 2

# 6. Validate observability
bash validate/check_metrics.sh
bash validate/check_pools.sh
bash validate/check_memory.sh <PGDOG_PID>
```

## Running Rust Integration Tests

These tests require PgDog running with wildcard config and tenant databases:

```bash
cd integration/rust
cargo nextest run --test-threads=1 pool_cap_saturation -- --ignored
cargo nextest run --test-threads=1 concurrent_pool_creation -- --ignored
```

## Running on Kubernetes

```bash
cd integration/production_readiness/k8s
bash run-k8s.sh 2000    # Creates namespace, deploys Postgres + PgDog, sets up 2000 tenants

# Then run load tests against localhost:6432 (port-forwarded)
bash ../load/multi_tenant_bench.sh --tenant-count 100 --clients 50

# Cleanup
kubectl delete namespace pgdog-test
```

## Test Descriptions

| Test | What it validates | Pass criteria |
|------|-------------------|---------------|
| **Scale connect** | Connect to 2000+ databases | 100% success, <5s total |
| **Multi-tenant load** | Throughput under concurrent tenant load | p99 <100ms, errors <0.5% |
| **Passthrough auth** | Auth delegation to Postgres | Correct creds succeed, wrong creds fail |
| **Pool pressure** | Behavior when pools are exhausted | Clear timeouts, recovery after pressure |
| **Sustained load** | Memory stability over time | RSS growth <20% |
| **Pool lifecycle** | Wildcard pool create→idle→evict→recreate | Pools evict, recreate works, stable memory |
| **Connection storm** | Thundering herd on cold pools | ≥90% success, recovery after burst |
| **Idle-in-transaction** | Pool blocking by held transactions | Queries queue/timeout, pool recovers |
| **Backend failure** | Reconnect after reset/partition/slow | Recovery after each fault type |
| **Network latency** | Behavior under injected latency | Timeouts work, throughput degrades gracefully |
| **Graceful shutdown** | SIGTERM drain under active load | Process exits within grace period, no panic |
| **Metrics check** | OpenMetrics endpoint accuracy | Valid Prometheus format, pool counts match |
| **Admin pools** | Admin interface consistency | Pool states match expected |
| **Pool cap** (Rust) | `max_wildcard_pools` enforcement | Clean rejection at limit, no corruption |
| **Concurrent creates** (Rust) | Race safety on pool instantiation | Exactly 1 pool per database |

## Configuration Reference

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `TENANT_COUNT` | 2000 | Number of tenant databases to create |
| `DURATION` | 60 | Load test duration (seconds) |
| `POOL_SIZE` | 10 | PgDog default_pool_size |
| `MAX_WILDCARD_POOLS` | 0 | Max wildcard pools (0 = unlimited) |
| `PGDOG_PORT` | 6432 | PgDog listen port |
| `PG_HOST` | 127.0.0.1 | Postgres host |
| `PG_PORT` | 15432 | Postgres port |

### CLI Arguments (run.sh)

```
--tenant-count N        Number of tenant databases
--duration N            Load test duration in seconds
--pool-size N           Default pool size
--max-wildcard-pools N  Max wildcard pools (0 = unlimited)
--skip-build            Skip cargo build (use existing binary)
--skip-infra            Skip Docker Compose startup and tenant creation
--skip-k8s              Skip K8s tests (default)
--with-k8s              Include K8s tests
```

## Interpreting Results

Results are written to `integration/production_readiness/results/`:

- `summary.txt` — Pass/fail per test
- `scale_connect.log` — Connection timing details
- `multi_tenant_bench.log` — TPS, latency percentiles
- `passthrough_auth.log` — Auth success/failure counts
- `pool_pressure.log` — Queue behavior, recovery timing
- `sustained_load.log` — Time-series telemetry
- `memory_trace.csv` — RSS measurements over time
- `metrics_snapshot.txt` — Raw OpenMetrics output
- `show_pools.txt` / `show_clients.txt` — Admin DB snapshots

### What "good" looks like

- **TPS**: Depends on hardware. Baseline with direct Postgres, then compare via PgDog. Overhead should be <15%.
- **p99 latency**: Under 100ms for transaction-mode pooling with moderate load.
- **Memory**: RSS should stabilize within the first minute. Growth >20% over 10 minutes suggests a leak.
- **Pool cap**: Rejection should be immediate (not timeout), with a clear error message.
- **Concurrent creates**: Zero duplicate pools. All connections succeed.

## Relation to ADR

This test suite addresses the "Edge cases to test before production use" from `2026-03-22-pgdog-as-db-bouncer.md`:

- ✅ **Wildcard pool cap reached** → `pool_cap_saturation` test
- ✅ **Concurrent first connections** → `concurrent_pool_creation` test
- ✅ **Cold tenant reconnect after eviction** → `pool_lifecycle` churn test
- ✅ **Backend failure / network partition** → `backend_failure` test (toxiproxy)
- ✅ **Network latency impact** → `network_latency` test (toxiproxy)
- ✅ **Graceful shutdown under load** → `graceful_shutdown` test
- ✅ **Idle-in-transaction pool starvation** → `idle_in_transaction` test
- ✅ **Thundering herd on cold pools** → `connection_storm` test
- ⬜ Password rotation for materialized wildcard pools (future: extend passthrough_auth test)
- ⬜ Reload after changing wildcard template (future: config reload test)

## Directory Structure

```
integration/production_readiness/
├── run.sh                    # Master orchestrator
├── docker-compose.yml        # Postgres test environment
├── README.md                 # This file
├── .gitignore
├── setup/
│   ├── init.sql              # Tenant database schema template
│   ├── generate_tenants.sh   # Create N tenant databases
│   ├── generate_config.py    # Generate PgDog wildcard config
│   └── configure_toxiproxy.sh # Create toxiproxy proxy for fault injection
├── config/                   # Generated PgDog config (gitignored)
├── load/
│   ├── tenant_workload.sql   # pgbench workload per tenant
│   ├── scale_connect.sh      # 2000+ database connection test
│   ├── multi_tenant_bench.sh # pgbench multi-tenant load
│   ├── passthrough_auth.sh   # Auth delegation test
│   ├── pool_pressure.sh      # Pool exhaustion test
│   ├── sustained_load.sh     # Long-running soak test
│   ├── pool_lifecycle.sh     # Wildcard pool churn (create/evict/recreate)
│   ├── connection_storm.sh   # Thundering herd on cold pools
│   ├── idle_in_transaction.sh # Transaction hold pool starvation
│   ├── backend_failure.sh    # Backend reset/partition/slow (toxiproxy)
│   ├── network_latency.sh    # Injected latency impact (toxiproxy)
│   └── graceful_shutdown.sh  # SIGTERM drain under load
├── validate/
│   ├── check_metrics.sh      # OpenMetrics validation
│   ├── check_pools.sh        # Admin DB pool checks
│   └── check_memory.sh       # RSS memory monitoring
├── k8s/
│   ├── namespace.yaml
│   ├── postgres-statefulset.yaml
│   ├── pgdog-deployment.yaml
│   ├── pgdog-configmap.yaml
│   ├── tenant-setup-job.yaml
│   └── run-k8s.sh
└── results/                  # Test output (gitignored)
```
