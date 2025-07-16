# PgDog Integration Tests

This directory contains integration tests for PgDog across multiple programming languages and frameworks.

## Quick Start

```bash
# Run all tests with a single command
./test

# Run tests for a specific language
./test --language=python
./test --language=go

# Run tests and clean up afterwards
./test --clean
```

## Prerequisites

Before running tests, ensure you have:

1. **PostgreSQL v15+ installed and running** on localhost:5432
   - Superuser access with username `postgres` and password `postgres`
   - Or ability to create databases and users

2. **Rust toolchain** with cargo-nextest
   ```bash
   # Install Rust from https://rustup.rs or brew
   # curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
   #  OR
   # brew install rust
   
   # Install cargo-nextest
   cargo install cargo-nextest
   ```

3. **Available ports**: 5432 (PostgreSQL), 6432 (PgDog)

### Specific Language Tests
```bash
./test --language=python
./test --language=go
./test --language=ruby
./test --language=js
./test --language=java
./test --language=rust
```

### With Options
```bash
# Force re-run setup
./test --force-setup

# Clean up after tests
./test --clean

# Verbose output for debugging
./test --verbose

# Combine options
./test --language=python --verbose --clean
```

## Test Details

### Language-Specific Tests

Each language directory contains tests that verify PgDog compatibility with that language's PostgreSQL drivers:

- **Python**: Tests using `psycopg2` and `asyncpg`
- **Ruby**: Tests using `pg` gem
- **Go**: Tests using `pgx` and `database/sql`
- **Java**: JDBC driver compatibility tests
- **JavaScript**: Tests using `pg` and `node-postgres`
- **Rust**: Tests using `tokio-postgres`

### Special Test Suites

- **load_balancer**: Verifies load balancing across multiple PostgreSQL instances
- **pgbench**: Performance benchmarks comparing PgDog to direct PostgreSQL
- **toxi**: Network failure scenarios using Toxiproxy
- **complex**: Multi-language scenarios and edge cases

## Troubleshooting

### Common Issues

1. **"PostgreSQL not found" or connection errors**
   ```bash
   # Ensure PostgreSQL is running
   # sudo systemctl status postgresql  # Linux
   # brew services list                 # macOS + Homebrew
   
   # Check PostgreSQL is accessible
   psql -h localhost -U postgres -c "SELECT version();"
   ```

2. **"Port 6432 already in use"**
   ```bash
   # Check what's using the port
   lsof -i :6432
   
   # Kill any existing PgDog processes
   pkill -f pgdog
   ```

3. **"Permission denied" creating databases**
   - Ensure you're running as a PostgreSQL superuser
   - Check PostgreSQL authentication in `pg_hba.conf`

4. **Build failures**
   ```bash
   # Clean and rebuild
   cd ..
   cargo clean
   cargo build --release
   ```

### Debug Mode

Run tests with verbose output:
```bash
./test --verbose
# or
RUST_LOG=debug ./test
```

## Writing New Tests

To add tests for a new language:

1. Create a directory: `integration/new_language/`
2. Add a `run.sh` script that:
   - Sources `../common.sh`
   - Starts PgDog using `start_pgdog()`
   - Runs language-specific tests
   - Calls `stop_pgdog()` on completion
   - Check existing language `run.sh` for examples
