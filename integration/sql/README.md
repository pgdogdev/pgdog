# SQL Regression Suite

This directory hosts the pure-SQL regression harness used to ensure PgDog matches the behaviour of direct PostgreSQL connections.

## Layout

- `cases/` – numbered SQL scenarios using the `00x_slug_(setup|case|teardown).sql` convention.
- `global_setup.sql` / `global_teardown.sql` – optional hooks that run before/after every case.
- `lib.py` – harness logic plus in-code definitions for the six comparison targets.
- `run.sh` / `dev.sh` – integration entrypoints matching the other language suites.
- `test_sql_regression.py` – pytest runner that executes every case against all registered targets.

## Workflow

1. `run.sh` builds PgDog, starts the proxy, runs the suite, then shuts PgDog down.
2. For each scenario the harness:
   - runs `global_setup.sql` (if present) and the scenario’s `*_setup.sql` through every distinct DSN used by the targets (shared once for baseline/pgdog, once for the sharded pool),
   - executes the `*_case.sql` statements against all six variants (baseline Postgres, PgDog, PgDog sharded × text/binary),
   - asserts that statement status, column names, column types, row counts, and row payloads are identical for every target,
   - runs the scenario’s `*_teardown.sql` and then `global_teardown.sql` (if present) across the same DSNs to leave databases clean.

Add more scenarios by dropping files into `cases/`:

```
cases/
  002_select_edge_case_setup.sql    # optional
  002_select_edge_case_case.sql     # required
  002_select_edge_case_teardown.sql # optional
```

Metadata lives in SQL comments at the top of the `*_case.sql` file:

```
-- description: Exercise numeric encoding edge cases
-- tags: standard sharded
-- transactional: true
-- skip-targets: pgdog_sharded_binary
-- only-targets: postgres_standard_text pgdog_standard_text
```

Tags are informative only. Use `skip-targets` to drop one or more target names, or `only-targets` to pin a case to a specific subset (order-sensitive). Set `transactional: false` when the statements must commit.
