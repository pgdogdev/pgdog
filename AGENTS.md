
Read `CLAUDE.md` and `.claude/skills/rust/SKILL.md`.

## Tool usage

Prefer the dedicated Read, Grep, and Glob tools for file searching and reading. Bash is permitted as a fallback when dedicated tools can't accomplish the task, but excessive Bash tool calls for search/read operations will be denied.


## Running tests

Unit and integration tests use `cargo-nextest`:

```sh
cargo nextest run --profile dev
```

If a single test fails, run it directly by name to isolate the failure.

Integration tests live under `integration/<lang>/`. The harness is multi-language;
`integration/run.sh` only dispatches the `python`, `ruby`, `java`, and `sql` suites.
Other suites (`go`, `rust`, `dry_run`, `pgbench`, `toxi`, `two_pc`, `plugins`,
`schema_sync`, `complex`, `mirror`, `load_balancer`, `copy_data`, ...) must be run
directly via their own `run.sh`. All suites require a running Postgres instance
configured by `bash integration/setup.sh`.

```sh
bash integration/run.sh          # python + ruby + java + sql
```

Or a specific suite:

```sh
bash integration/go/run.sh
```

Always run `cargo fmt` before submitting changes. Run `cargo clippy` where practical.