# PgDog plugins

This directory contains plugins that ship with PgDog and are built by original author(s) or by the community. You can use these as-is or modify them to your needs.

## Plugins

### `pgdog-example-plugin`

Example plugin that can be used as reference by the community. It currently records
when a write was made to a table and, for the next 5 seconds after the write, redirects
all `SELECT` queries that touch table to the primary.

It's a simple workaround for Postgres replica lag, if you're using batch writes.
