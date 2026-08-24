# PgDog plugins

This directory contains plugins that ship with PgDog and are built by original author(s) or by the community. You can use these as-is or modify them to your needs.

## Plugins

### `pgdog-google-auth`

Authenticates PostgreSQL clients with Google OAuth 2.0 access tokens, including
tokens printed by `gcloud auth print-access-token`. The plugin validates tokens
with Google's `tokeninfo` endpoint and can restrict access by Google account,
domain, OAuth audience, and scope.

See the [`pgdog-google-auth` documentation](pgdog-google-auth/README.md).

### `pgdog-example-plugin`

Example plugin that can be used as reference by the community. It currently records
when a write was made to a table and, for the next 5 seconds after the write, redirects
all `SELECT` queries that touch table to the primary.

It's a simple workaround for Postgres replica lag, if you're using batch writes.
