# Contribution guidelines

If you see a bug or would like to request a feature, please create an [issue](https://github.com/pgdogdev/pgdog/issues). If you would like to submit a bug fix, please fork the repository and give us a link to your branch. Pull requests are currently restricted to project contributors.

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
5. Run the unit tests with `cargo nextest run`. If some test fails, try running it directly.
6. Run the integration tests `bash integration/run.sh` or exact integration test e.g. `bash integration/go/run.sh`.

## Coding

1. Please format your code with `cargo fmt`.
2. If you're feeling generous, `cargo clippy` as well.
3. Please write and include tests. This is production software used in one of the most important areas of the stack.
