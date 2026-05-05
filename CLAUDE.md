# About the project

PgDog is a connection pooler, load balancer and database sharder for PostgreSQL. It's written in async Rust. It implements the PostgreSQL network protocol: applications connect to PgDog, PgDog connects to Postgres, and neither know there is a proxy in between.

## Workflow

Load the `/rust` skill. Don't follow C/C++/Go norms.


## Running tests

Unit tests:

```sh
cargo nextest run --profile dev
```

If a test fails, run it directly by name. Integration tests require Postgres configured via `bash integration/setup.sh` first.

The integration harness is multi-language: each suite lives in `integration/<lang>/`.
`integration/run.sh` only dispatches the `python`, `ruby`, `java`, and `sql` suites.
Other suites (`go`, `rust`, `dry_run`, `pgbench`, `toxi`, `two_pc`, `plugins`,
`schema_sync`, `complex`, `mirror`, `load_balancer`, `copy_data`, ...) must be run
directly via their own `run.sh`.

```sh
bash integration/run.sh          # python + ruby + java + sql
bash integration/go/run.sh       # Go suite
```

Format before committing: `cargo fmt`. Run `cargo clippy` where practical.