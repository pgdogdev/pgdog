# Debug Skill — pgdog Development Environment

## Rules

1. **Build command**: Always use `cargo build -p pgdog` (debug profile). Never use `--release` during debugging — it slows down builds by 4× and you rarely need to verify the final binary during development.

2. **Docker environment**: You have access to rootless Docker. Key services:
   - PostgreSQL runs on port **5433** (NOT 5432 — your real system postgres uses that)
   - Redis is available on the default port **6379**
   - If services aren't running, start them first:
     ```bash
     docker start $(docker ps -a -q --filter "name=pgdog" --latest)
     # Or inspect what containers exist:
     docker ps -a
     ```

3. **Running pgdog in background**: Use `systemd-run --user`, NOT `nohup` or `&`. Example:
   ```bash
   systemd-run --user --collect --unit=pgdog-debug \
     --setenv=RUST_LOG=debug \
     --working-directory=<Project root path> \
     <Project root path>/target/debug/pgdog --config pgdog.toml 2>&1
   ```
   To stop it later:
   ```bash
   systemctl --user list-units | grep pgdog
   systemctl --user stop <service-name>
   ```

## Useful Debugging Commands

### Check docker services

```bash
docker ps
```

### Check redis connectivity

```bash
redis-cli ping
```

### Clear redis cache (useful for testing)

```bash
redis-cli FLUSHALL
```

### Watch cache keys in real-time

```bash
redis-cli MONITOR | grep "pgdog:"
```

### Inspect cached response bytes

```bash
redis-cli --scan --pattern "pgdog:*" | head -1 | xargs redis-cli GET | xxd | head -20
```

## File Structure Reference

The cache implementation lives in:

```
pgdog/src/frontend/client/query_engine/cache/
├── mod.rs          # Module exports
├── client.rs       # Redis client wrapper (fred v9)
├── integration.rs  # cache_check(), send_cached_response(), cache_response()
├── policy.rs       # CachePolicyResolver (3-tier decision engine)
└── stats.rs        # QueryStatsTracker (hit/miss counters)
```

State documentation: `CacheState.md` in the project root.

## Common Pitfalls

- **Parser disabled by default**: `route.is_read()` returns false for `SELECT 1` when the query parser is off. The `is_likely_read()` heuristic in integration.rs covers this.
- **Policy defaults to NoCache**: `DatabaseCache.policy()` returns `CachePolicy::NoCache` by default. You must set `policy = "cache"` in the config.
- **Cache keys are hashed**: The key is a DefaultHasher hex digest of the raw query string, not the query itself.
- **Wire format is concatenated bytes**: Multiple PostgreSQL messages are concatenated into a single `Vec<u8>` with `[code: u8][length: u32be][payload: ...]` structure.
