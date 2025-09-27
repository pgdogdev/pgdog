# Repository Guidelines

## Project Structure & Module Organization
PgDog is a Rust workspace centred in `pgdog/` with the proxy under `pgdog/src` and end-to-end specs in `pgdog/tests`. Shared macros live in `pgdog-macros`, runtime extensions in `pgdog-plugin` plus `pgdog-plugin-build`, and loadable plugins under `plugins/`. Integration harnesses, Docker assets, and helper scripts sit in `integration/`, `docker/`, and `dev/`. Use `example.pgdog.toml` and `example.users.toml` as templates when adding sharded configs.

## Build, Test, and Development Commands
Run `cargo check` for a quick type pass, and `cargo build` to compile local binaries; switch to `cargo build --release` for benchmarking or packaging. Start the development stack with `bash integration/dev-server.sh`, which provisions dependencies and runs `cargo watch` for live reloads. CI parity tests run via `cargo nextest run --test-threads=1 --no-fail-fast`. **Never invoke** `cargo test` directly—always use `cargo nextest run --test-threads=1 ...` for unit or integration suites so concurrency stays deterministic.

## Coding Style & Naming Conventions
Follow Rust conventions: modules and functions in `snake_case`, types in `UpperCamelCase`, constants in `SCREAMING_SNAKE_CASE`. Keep modules under ~200 lines unless justified. Format with `cargo fmt` and lint using `cargo clippy --all-targets --all-features` before posting a PR.
Prefer keeping `#[cfg(test)]` blocks at the end of a file; only place `#[cfg(test)]` imports directly beneath normal imports when that keeps the module tidy.

## Testing Guidelines
Adhere to TDD—write the failing test first, implement minimally, then refactor. Co-locate unit tests with their crates, reserving heavier scenarios for `integration/` against the prepared-transaction Postgres stack. Invoke `cargo nextest run --test-threads=1 <test>` for focused iterations; gate Kerberos coverage behind `--features gssapi`. Do **not** run `cargo test`; Nextest with a single-thread budget is the required harness.

## Commit & Pull Request Guidelines
Use concise, imperative commit subjects (e.g., "fix gssapi credential lookup") and describe user impact plus risk areas in PRs. Link relevant issues, attach logs or test output, and include config diffs or screenshots for operational changes. Keep PR scope tight and ensure linting, formatting, and tests pass locally before requesting review.

## Security & Configuration Tips
Capture diagnostics before code changes, prefer configuration-driven fixes, and document new setup steps in `integration/README`. Never commit secrets; rely on the provided templates and environment variables when adjusting credentials or connection strings.
