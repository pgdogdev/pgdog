# SQL Regression Suite

This directory hosts the pure-SQL regression harness used to ensure PgDog matches the behaviour of direct PostgreSQL connections.

## Layout

- `config.yaml` – connection targets and comparison pairs (baseline vs PgDog, text vs binary).
- `cases/` – numbered SQL scenarios using the `00x_slug_(setup|case|teardown).sql` convention.
- `global_setup.sql` / `global_teardown.sql` – optional hooks that run before/after every case.
- `run.sh` / `dev.sh` – integration entrypoints matching the other language suites.
- `test_sql_regression.py` – pytest runner that executes every case against each configured comparison pair.

## Workflow

1. `run.sh` builds PgDog, starts the proxy, runs the suite, then shuts PgDog down.
2. For each scenario the harness:
   - runs `global_setup.sql` (if present) and the scenario’s `*_setup.sql` on the baseline connection,
   - executes the `*_case.sql` statements once via the baseline connection and once via PgDog, toggling text/binary protocol as configured,
   - compares row counts, type metadata, and row payloads for equality,
   - runs the scenario’s `*_teardown.sql` and then `global_teardown.sql` (if present) to clean up fixtures.

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
```

Tags control which comparison pairs are exercised; use `standard` (default) or `sharded`. Set `transactional: false` when the statements must commit.
