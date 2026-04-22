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
4. Run the setup script `bash integration/setup.sh`. It configures required PostgreSQL
   settings and creates the test databases. If any settings were changed, the script
   will exit with a notice — restart PostgreSQL and re-run the script before continuing.
5. Run the tests `cargo nextest run --profile dev`. If a test fails, try running it directly.
6. Run the integration tests `bash integration/run.sh` or exact integration test with `bash integration/go/run.sh`.

## Coding

1. Please format your code with `cargo fmt`.
2. If you're feeling generous, `cargo clippy` as well.
3. Please write and include tests. This is production software used in one of the most important areas of the stack.
