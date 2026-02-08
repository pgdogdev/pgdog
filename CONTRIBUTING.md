# Contribution guidelines

Contributions are welcome. If you see a bug, feel free to submit a PR with a fix or an issue to discuss. For any features, please open an issue to discuss first.

## Necessary crates - cargo install <name>

(if you use mise, these can be installed with `mise install`)
- cargo-nextest
- cargo-watch

## Dev setup

1. Run cargo build in the project directory.
2. Install Postgres (all Pg versions supported).
3. Add user `pgdog` with password `pgdog`.
4. Some tests used prepared transactions. Enable them with `ALTER SYSTEM SET max_prepared_transactions TO 1000;` sql command and restart Postgres.
5. Run the setup script `bash integration/setup.sh`.
6. Run the tests `cargo nextest run --profile dev`. If a test fails, try running it directly.
7. Run the integration tests `bash integration/run.sh` or exact integration test with `bash integration/go/run.sh`.

## Coding

1. Please format your code with `cargo fmt`.
2. If you're feeling generous, `cargo clippy` as well.
3. Please write and include tests. This is production software used in one of the most important areas of the stack.
