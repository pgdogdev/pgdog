# Bash commands

- `cargo check` to test that the code compiles. It shouldn't contain warnings. This is quicker than `cargo build`.
- `cargo fmt` to reformat code according to Rust standards.
- `cargo nextest run --test-threads=1 <test name>` to run a specific test. Run `pgdog` tests from the `pgdog` directory (`cd pgdog` first).
- `cargo nextest run --test-threads=1 --no-fail-fast` to run all tests. Make sure to use `--test-threads=1` because some tests conflict with each other.

# Code style

- Use standard Rust code style.
- Use `cargo fmt` to reformat code automatically after every edit.
- Don't write functions with many arguments: create a struct and use that as input instead.

# Workflow

- Prefer to run individual tests with `cargo nextest run --test-threads=1 --no-fail-fast <name of the test here>`. This is much faster.
- A local PostgreSQL server is required for some tests to pass. Assume it's running, if not, stop and ask the user to start it.
- Coe

# About the project

PgDog is a connection pooler for Postgres that can shard databases. It implements the Postgres network protocol and uses pg_query to parse SQL queries. It aims to be 100% compatible with Postgres, without clients knowing they are talking to a proxy.
