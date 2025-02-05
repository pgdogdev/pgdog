# PgDog plugins

This directory contains (now and in the future) plugins that ship with PgDog and are built by original author(s)
or the community. You can use these as-is or modify them to your needs.

## Plugins

### `pgdog-routing`

The only plugin in here right now and the catch-all for routing traffic through PgDog. This plugin uses `pg_query.rs` (Rust bindings to `pg_query`)
to parse queries using the PostgreSQL parser, and splits traffic between primary and replicas. This allows users of this plugin to deploy
primaries and replicas in one PgDog configuration.
