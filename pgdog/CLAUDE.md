# Bash commands

- `cargo check` to test that the code compiles. It shouldn't contain warnings. This is quicker than `cargo build`.
- `cargo fmt` to reformat code according to Rust standards.
- `cargo nextest run <test name>` to run a specific test
- `cargo nextest run --test-threads=1` to run all tests. Make sure to use `--test-threads=1` because some tests conflict with each other.

# Code style

Use standard Rust code style. Use `cargo fmt` to reformat code automatically after every edit.

# Workflow

- Prefer to run individual tests with `cargo nextest run <name of the test here>`. This is much faster.
- A local PostgreSQL server is required for some tests to pass. Set it up and create a database called "pgdog". Create a user called "pgdog" with password "pgdog".

# About the project

PgDog is a connection pooler for Postgres that can shard databases. It implements the Postgres network protocol and uses pg_query to parse SQL queries. It aims to be 100% compatible with Postgres, without clients knowing they are talking to a proxy.
