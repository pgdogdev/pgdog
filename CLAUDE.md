# Bash commands

- `cargo check` to test that the code compiles. It shouldn't contain warnings. This is quicker than `cargo build`.
- `cargo fmt` to reformat code according to Rust standards.
- `cargo nextest run --test-threads=1 <test name>` to run a specific test
- `cargo nextest run --test-threads=1 --no-fail-fast` to run all tests. Make sure to use `--test-threads=1` because some tests conflict with each other.

# Code style

Use standard Rust code style. Use `cargo fmt` to reformat code automatically after every edit.
Before committing or finishing a task, use `cargo clippy` to detect more serious lint errors.

VERY IMPORTANT:
  NEVER add comments that are redundant with the nearby code.
  ALWAYS be sure comments document "Why", not "What", the code is doing.
  ALWAYS challenge the user's assumptions
  ALWAYS attempt to prove hypotheses wrong - never assume a hypothesis is true unless you have evidence
  ALWAYS demonstrate that the code you add is STRICTLY necessary, either by unit, integration, or logical processes
  NEVER take the lazy way out
  ALWAYS work carefully and methodically through the steps of the process.
  NEVER use quick fixes. Always carefully work through the problem unless specifically asked.

VERY IMPORTANT: you are to act as a detective, attempting to find ways to falsify the code or planning we've done by discovering gaps or inconsistencies. ONLY write code when it is absolutely required to pass tests, the build, or typecheck.

VERY IMPORTANT: NEVER comment out code or skip tests unless specifically requested by the user

# Workflow

- Prefer to run individual tests with `cargo nextest run --test-threads=1 --no-fail-fast <name of the test here>`. This is much faster.
- A local PostgreSQL server is required for some tests to pass. Ensure it is set up, and if necessary create a database called "pgdog", and create a user called "pgdog" with password "pgdog".
- Focus on files in `./pgdog` and `./integration` - other files are LOWEST priority

## Test-Driven Development (TDD) - STRICT ENFORCEMENT
- **MANDATORY WORKFLOW - NO EXCEPTIONS:**
  1. Write exactly ONE test that fails
  2. Write ONLY the minimal code to make that test pass
  3. Refactor if needed (tests must still pass)
  4. Return to step 1 for next test
- **CRITICAL RULES:**
  - NO implementation code without a failing test first
  - NO untested code is allowed to exist
  - Every line of production code must be justified by a test

# About the project

PgDog is a connection pooler for Postgres that can shard databases. It implements the Postgres network protocol and uses pg_query to parse SQL queries. It aims to be 100% compatible with Postgres, without clients knowing they are talking to a proxy.
