# Contribution guidelines

Contributions are welcome. If you see a bug, feel free to submit a PR with a fix or an issue to discuss. For any features, please open an issue to discuss first.

## Necessary crates - cargo install <name>
- cargo-nextest
- cargo-watch

## Dev setup

1. Run cargo build in the project directory.
2. Install Postgres (v17 currently supported).
3. Run the setup script `bash integration/setup.sh`.
4. Launch pgdog configured for integration: `bash integration/dev-server.sh`.
5. Run the tests `cargo nextest run --test-threads=1`. If a test fails, try running it directly.

## Coding

1. Please format your code with `cargo fmt`.
2. If you're feeeling generous, `cargo clippy` as well.
3. Please write and include tests. This is production software used in one of the most important areas of the stack.
